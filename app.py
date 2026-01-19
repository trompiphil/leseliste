import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import requests
import time
import unicodedata
from datetime import datetime
from deep_translator import GoogleTranslator

# --- KONFIGURATION ---
st.set_page_config(page_title="Meine Bibliothek", page_icon="📚", layout="wide")

# --- CSS DESIGN (High Contrast Fix) ---
st.markdown("""
    <style>
    /* 1. Hintergrund */
    .stApp { 
        background-color: #f5f5dc !important; 
    }
    
    /* 2. Textfarben GLOBAL erzwingen */
    .stApp, .stMarkdown, h1, h2, h3, h4, h5, h6, p, div, span, label, th, td, li { 
        color: #2c3e50 !important; 
    }
    
    /* 3. Eingabefelder */
    .stTextInput input, .stTextArea textarea, .stSelectbox div[data-baseweb="select"] {
        background-color: #fffaf0 !important;
        border: 2px solid #d35400 !important;
        color: #000000 !important; /* Tiefschwarz */
    }
    
    /* 4. Buttons */
    .stButton button {
        background-color: #d35400 !important;
        color: white !important;
        font-weight: bold !important;
        font-size: 18px !important;
        border-radius: 8px;
        border: none;
    }

    /* 5. Kacheln Design */
    .book-card {
        background-color: #eaddcf;
        border: 2px solid #d35400;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
        height: 100%;
        box-shadow: 3px 3px 8px rgba(0,0,0,0.2);
    }
    .book-card img {
        max-width: 100px;
        border-radius: 5px;
        margin-bottom: 10px;
        box-shadow: 1px 1px 5px rgba(0,0,0,0.2);
    }
    .book-title {
        font-weight: 900 !important;
        font-size: 1.2em !important;
        margin-bottom: 5px;
        color: #000000 !important; /* Schwarz erzwingen */
    }
    .book-author {
        font-style: italic;
        margin-bottom: 10px;
        font-size: 1.0em;
        color: #4a3b2a !important;
    }

    /* 6. Navigation Tabs */
    div[role="radiogroup"] label {
        background-color: #eaddcf !important;
        border: 1px solid #d35400;
        color: #4a3b2a !important;
        font-weight: bold;
    }
    div[role="radiogroup"] label[data-checked="true"] {
        background-color: #d35400 !important;
        color: white !important;
    }
    </style>
""", unsafe_allow_html=True)

# --- FUNKTIONEN ---

def get_connection():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    if "gcp_service_account" in st.secrets:
        try:
            creds_dict = dict(st.secrets["gcp_service_account"])
            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            return gspread.authorize(creds)
        except Exception as e: return None
    return None

def setup_sheets(client):
    try: sh = client.open("Bücherliste") 
    except: st.error("Fehler: Tabelle 'Bücherliste' nicht gefunden."); st.stop()
    ws_books = sh.sheet1
    try: ws_authors = sh.worksheet("Autoren")
    except: ws_authors = sh.add_worksheet(title="Autoren", rows=1000, cols=1); ws_authors.update_cell(1, 1, "Name")
    return ws_books, ws_authors

def check_and_update_structure(ws):
    try:
        current_headers = ws.row_values(1)
        if not current_headers:
            ws.update_cell(1, 1, "Titel")
            current_headers = ["Titel"]
        needed = ["Titel", "Autor", "Genre", "Bewertung", "Cover", "Hinzugefügt", "Notiz", "Status"]
        next_col = len(current_headers) + 1
        for h in needed:
            if not any(ch.lower() == h.lower() for ch in current_headers):
                ws.update_cell(1, next_col, h)
                next_col += 1
                time.sleep(0.5)
    except: pass

def fetch_data_from_sheet(worksheet):
    # Definition aller Spalten, die wir erwarten
    expected_cols = ["Titel", "Autor", "Genre", "Bewertung", "Cover", "Hinzugefügt", "Notiz", "Status", "Name"]
    
    try:
        all_values = worksheet.get_all_values()
        if len(all_values) < 2: 
            return pd.DataFrame(columns=expected_cols)
        
        headers = [str(h).strip().lower() for h in all_values[0]]
        col_map = {}
        for idx, h in enumerate(headers):
            if "titel" in h: col_map["Titel"] = idx
            elif "autor" in h: col_map["Autor"] = idx
            elif h in ["cover", "bild"]: col_map["Cover"] = idx
            elif h in ["sterne", "bewertung"]: col_map["Bewertung"] = idx
            elif h in ["genre"]: col_map["Genre"] = idx
            elif "hinzugefügt" in h: col_map["Hinzugefügt"] = idx
            elif "notiz" in h: col_map["Notiz"] = idx
            elif "status" in h: col_map["Status"] = idx
            elif "name" in h: col_map["Name"] = idx

        rows = []
        for raw_row in all_values[1:]:
            # Standardwerte setzen (verhindert KeyError bei fehlenden Spalten)
            entry = {c: "" for c in expected_cols}
            entry["Status"] = "Gelesen" 
            
            for key, idx in col_map.items():
                if idx < len(raw_row):
                    entry[key] = raw_row[idx]
            
            if not entry["Status"]: entry["Status"] = "Gelesen"
            if entry["Titel"] or entry["Name"]:
                rows.append(entry)
        
        if not rows: return pd.DataFrame(columns=expected_cols)
        return pd.DataFrame(rows)
    except: return pd.DataFrame(columns=expected_cols)

def force_reload():
    st.session_state.clear()
    st.rerun()

def sync_authors(ws_books, ws_authors):
    if "sync_done" in st.session_state: return 0
    if "df_books" not in st.session_state: st.session_state.df_books = fetch_data_from_sheet(ws_books)
    if "df_authors" not in st.session_state: st.session_state.df_authors = fetch_data_from_sheet(ws_authors)
    
    df_b = st.session_state.df_books
    df_a = st.session_state.df_authors
    
    if df_b.empty or "Autor" not in df_b.columns: return 0
    
    existing = set()
    if "Name" in df_a.columns:
        existing = set([a.strip() for a in df_a["Name"].tolist() if str(a).strip()])
    
    book_authors = set([a.strip() for a in df_b["Autor"].tolist() if str(a).strip()])
    missing = list(book_authors - existing)
    missing.sort()
    
    if missing:
        ws_authors.append_rows([[name] for name in missing])
        st.session_state.sync_done = True
        del st.session_state.df_authors
        return len(missing)
    st.session_state.sync_done = True
    return 0

# --- HELPER ---
def process_genre(raw_genre):
    if not raw_genre: return "Roman"
    try:
        translator = GoogleTranslator(source='auto', target='de')
        t = translator.translate(raw_genre)
        return t if "römisch" not in t.lower() else "Roman"
    except: return "Roman"

def fetch_book_data_background(titel, autor):
    cover, genre = "", "Roman"
    try:
        q = f"{titel} {autor}"
        url = f"https://www.googleapis.com/books/v1/volumes?q={q}&langRestrict=de&maxResults=1"
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            d = r.json()
            if "items" in d:
                info = d["items"][0]["volumeInfo"]
                cover = info.get("imageLinks", {}).get("thumbnail", "")
                genre = process_genre(info.get("categories", ["Roman"])[0])
    except: pass
    
    if not cover:
        try:
            q = f"{titel} {autor}".replace(" ", "+")
            r = requests.get(f"https://openlibrary.org/search.json?q={q}&limit=1", headers={"User-Agent":"App/1.0"}, timeout=5)
            if r.status_code == 200:
                d = r.json()
                if d.get("numFound",0)>0 and d.get("docs"):
                    cid = d["docs"][0].get("cover_i")
                    if cid: cover = f"https://covers.openlibrary.org/b/id/{cid}-M.jpg"
        except: pass
    return cover, genre

def get_smart_author_name(short_name, all_authors):
    short = short_name.strip().lower()
    for full in sorted(all_authors, key=len, reverse=True):
        if short in str(full).lower(): return full
    return short_name

def cleanup_author_duplicates_batch(ws_books, ws_authors):
    def deep_clean(text): return unicodedata.normalize('NFKC', str(text)).replace('\u00A0', ' ').strip()
    books_vals = ws_books.get_all_values()
    if not books_vals: return 0
    headers = [str(h).lower() for h in books_vals[0]]
    idx_a = next((i for i, h in enumerate(headers) if "autor" in h), -1)
    if idx_a == -1: return 0
    raws = [deep_clean(row[idx_a]) for row in books_vals[1:] if len(row)>idx_a and row[idx_a]]
    clean_map = {}
    for r in raws: clean_map.setdefault(r.strip(), []).append(r)
    replacements = {}
    for clean, versions in clean_map.items():
        if len(set(versions))>1: 
            for v in versions: replacements[v] = clean
    keys = sorted(clean_map.keys(), key=len, reverse=True)
    for i, long in enumerate(keys):
        for short in keys[i+1:]:
            if short.lower() in long.lower() and short.lower() != long.lower():
                for v in clean_map.get(short, []): replacements[v] = clean_map[long][0]
    if not replacements: return 0
    new_data = [books_vals[0]]
    changed = False
    for row in books_vals[1:]:
        nr = list(row)
        if len(nr)>idx_a:
            orig = deep_clean(nr[idx_a])
            if orig in replacements:
                if nr[idx_a] != replacements[orig]: nr[idx_a] = replacements[orig]; changed = True
            elif nr[idx_a] != orig: nr[idx_a] = orig; changed = True
        new_data.append(nr)
    if changed: ws_books.update(new_data)
    final_authors = sorted(list({r[idx_a].strip() for r in new_data[1:] if len(r)>idx_a and r[idx_a].strip()}))
    ws_authors.clear(); ws_authors.update_cell(1,1,"Name")
    if final_authors: ws_authors.update(values=[["Name"]] + [[a] for a in final_authors])
    return 1

# --- HAUPTPROGRAMM ---
def main():
    st.title("📚 Meine Bibliothek")
    
    # --- NOTFALL KNOPF GANZ OBEN ---
    if st.sidebar.button("🚨 Cache leeren & Neustart"):
        st.session_state.clear()
        st.rerun()

    if "input_key" not in st.session_state: st.session_state.input_key = 0

    client = get_connection()
    if not client: st.error("Verbindung fehlt! (Secrets prüfen)"); st.stop()
    ws_books, ws_authors = setup_sheets(client)

    if "structure_checked" not in st.session_state:
        check_and_update_structure(ws_books)
        st.session_state.structure_checked = True

    if "df_books" not in st.session_state:
        with st.spinner("Lade Bücher..."): st.session_state.df_books = fetch_data_from_sheet(ws_books)
    if "df_authors" not in st.session_state: st.session_state.df_authors = fetch_data_from_sheet(ws_authors)

    # --- SICHERHEITSCHECK: DATAFRAME REPARIEREN ---
    # Falls das DataFrame aus dem Cache noch alt ist und keine "Notiz" hat
    for required_col in ["Notiz", "Status", "Hinzugefügt"]:
        if required_col not in st.session_state.df_books.columns:
            st.session_state.df_books[required_col] = ""

    sync_authors(ws_books, ws_authors)
    
    known_authors = []
    if "Name" in st.session_state.df_authors.columns:
        known_authors = [a for a in st.session_state.df_authors["Name"].tolist() if str(a).strip()]

    # NAVIGATION
    nav = st.radio("Menü", ["✍️ Neu (Gelesen)", "🔍 Sammlung", "🔮 Merkliste", "👥 Autoren"], horizontal=True, label_visibility="collapsed")
    
    # --- TAB 1 ---
    if nav == "✍️ Neu (Gelesen)":
        st.header("Buch gelesen & hinzufügen")
        with st.form("new_read"):
            inp = st.text_input("Titel, Autor", key=f"k_{st.session_state.input_key}")
            rating = st.slider("Bewertung", 1, 5, 5)
            note = st.text_area("Notiz (optional):")
            if st.form_submit_button("💾 Speichern"):
                if "," in inp:
                    tit, aut = [x.strip() for x in inp.split(",", 1)]
                    final_aut = get_smart_author_name(aut, known_authors)
                    with st.spinner("Speichere..."):
                        c, g = fetch_book_data_background(tit, final_aut)
                        date_str = datetime.now().strftime("%Y-%m-%d")
                        ws_books.append_row([tit, final_aut, g, rating, c or NO_COVER_MARKER, date_str, note, "Gelesen"])
                        cleanup_author_duplicates_batch(ws_books, ws_authors)
                        del st.session_state.df_books
                    st.success(f"Gelesen: {tit}"); st.balloons(); time.sleep(1); st.session_state.input_key += 1; st.rerun()
                else: st.error("Komma fehlt!")

    # --- TAB 2 ---
    elif nav == "🔍 Sammlung":
        c_head, c_view = st.columns([3, 1])
        with c_head: st.header("Meine gelesenen Bücher")
        with c_view: 
            view_mode = st.radio("Ansicht", ["Liste", "Kacheln"], horizontal=True, label_visibility="collapsed")

        df = st.session_state.df_books.copy()
        df = df[ (df["Status"] == "Gelesen") | (df["Status"] == "") ]
        
        if not df.empty:
            if "Hinzugefügt" in df.columns:
                df["Hinzugefügt"] = pd.to_datetime(df["Hinzugefügt"], errors='coerce')
                df = df.sort_values(by="Hinzugefügt", ascending=False)
            
            search = st.text_input("🔍 Suchen:", placeholder="Titel, Autor, Jahr...")
            if search:
                s = search.lower().strip()
                df = df[df["Titel"].str.lower().str.contains(s) | df["Autor"].str.lower().str.contains(s)]
            
            if view_mode == "Liste":
                st.data_editor(
                    df,
                    column_order=["Cover", "Titel", "Autor", "Bewertung", "Hinzugefügt", "Notiz"],
                    column_config={
                        "Cover": st.column_config.ImageColumn("Bild", width="small"),
                        "Hinzugefügt": st.column_config.DateColumn("Datum", format="DD.MM.YYYY"),
                    },
                    hide_index=True,
                    use_container_width=True
                )
            else: 
                cols = st.columns(3)
                for i, (idx, row) in enumerate(df.iterrows()):
                    with cols[i % 3]:
                        with st.container(border=True):
                            cov = row["Cover"] if row["Cover"] and row["Cover"] != "-" else "https://via.placeholder.com/100x150?text=Buch"
                            st.markdown(f"""
                            <div class="book-card">
                                <img src="{cov}" style="width:80px">
                                <div class="book-title">{row['Titel']}</div>
                                <div class="book-author">{row['Autor']}</div>
                                <div>{'⭐' * int(row['Bewertung'] if row['Bewertung'] else 0)}</div>
                            </div>
                            """, unsafe_allow_html=True)
                            with st.expander("📝 Notiz"):
                                st.write(row.get("Notiz", "-"))
                                if "Hinzugefügt" in row and pd.notnull(row['Hinzugefügt']):
                                    st.caption(f"📅 {row['Hinzugefügt'].strftime('%d.%m.%Y')}")
        else: st.info("Noch keine Bücher gelesen.")

    # --- TAB 3 ---
    elif nav == "🔮 Merkliste":
        st.header("🔮 Merkliste")
        with st.expander("➕ Neuer Wunschtitel"):
            with st.form("new_wish"):
                inp_w = st.text_input("Titel, Autor")
                note_w = st.text_area("Notiz / Warum lesen?")
                if st.form_submit_button("Auf die Liste setzen"):
                    if "," in inp_w:
                        tit, aut = [x.strip() for x in inp_w.split(",", 1)]
                        final_aut = get_smart_author_name(aut, known_authors)
                        c, g = fetch_book_data_background(tit, final_aut)
                        date_str = datetime.now().strftime("%Y-%m-%d")
                        ws_books.append_row([tit, final_aut, g, "", c or NO_COVER_MARKER, date_str, note_w, "Wunschliste"])
                        del st.session_state.df_books
                        st.success(f"Gemerkt: {tit}"); st.rerun()
                    else: st.error("Komma fehlt!")

        df = st.session_state.df_books.copy()
        if not df.empty and "Status" in df.columns:
            df_wish = df[df["Status"] == "Wunschliste"]
            if not df_wish.empty:
                for idx, row in df_wish.iterrows():
                    with st.container(border=True):
                        c1, c2, c3 = st.columns([1, 4, 2])
                        with c1: 
                            if row["Cover"] and row["Cover"] != "-": st.image(row["Cover"], width=60)
                            else: st.write("📚")
                        with c2:
                            st.subheader(row["Titel"])
                            st.write(f"*{row['Autor']}*")
                            if row.get("Notiz"): st.info(f"📝 {row['Notiz']}")
                        with c3:
                            if st.button("✅ Gelesen!", key=f"read_{idx}"):
                                cell = ws_books.find(row["Titel"])
                                headers = ws_books.row_values(1)
                                try:
                                    status_col_idx = [h.lower() for h in headers].index("status") + 1
                                    ws_books.update_cell(cell.row, status_col_idx, "Gelesen")
                                    del st.session_state.df_books
                                    st.success("Verschoben zu Gelesen!"); st.rerun()
                                except: st.error("Fehler beim Verschieben.")
            else: st.info("Merkliste ist leer.")

    # --- TAB 4 ---
    elif nav == "👥 Autoren":
        st.header("Autoren Statistik")
        df_b = st.session_state.df_books
        df_a = st.session_state.df_authors.copy()
        if not df_b.empty and "Autor" in df_b.columns:
            counts = df_b["Autor"].value_counts()
            if "Name" in df_a.columns:
                df_a["Bücher"] = df_a["Name"].map(counts).fillna(0).astype(int)
                st.dataframe(df_a.sort_values("Bücher", ascending=False), hide_index=True, use_container_width=True)

if __name__ == "__main__":
    main()
