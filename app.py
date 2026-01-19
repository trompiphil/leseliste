import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import requests
import time
import urllib.parse
from datetime import datetime
from deep_translator import GoogleTranslator
import json

# --- KONFIGURATION ---
st.set_page_config(page_title="Meine Bibliothek", page_icon="📚", layout="wide")

# --- CSS DESIGN ---
st.markdown("""
    <style>
    .stApp { background-color: #f5f5dc !important; }
    h1, h2, h3, h4, h5, h6, p, div, span, label, li, textarea, input, a { color: #2c3e50 !important; }
    
    .stTextInput input, .stTextArea textarea {
        background-color: #fffaf0 !important;
        border: 2px solid #d35400 !important;
        color: #000000 !important;
    }
    
    .stButton button {
        background-color: #d35400 !important;
        color: white !important;
        border-radius: 8px; 
        border: none;
        font-weight: bold;
    }
    .stButton button:hover {
        background-color: #e67e22 !important;
    }

    [data-testid="stVerticalBlockBorderWrapper"] > div {
        background-color: #eaddcf;
        border-radius: 12px;
        border: 1px solid #d35400;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
        padding: 10px;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        background-color: transparent;
        padding-bottom: 5px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background-color: #eaddcf;
        border: 1px solid #d35400;
        color: #4a3b2a;
        font-weight: bold;
        padding: 0 20px; 
        border-radius: 8px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #d35400 !important;
        color: white !important;
        border-color: #d35400 !important;
    }
    
    div[data-testid="stImage"] img {
        width: 80px !important;
        max-width: 80px !important;
        height: auto !important;
        margin-left: auto;
        margin-right: auto;
        display: block;
        border-radius: 5px;
        box-shadow: 1px 1px 4px rgba(0,0,0,0.2);
    }
    
    [data-testid="column"] { padding: 0px !important; }
    
    .stFeedback {
        padding-top: 0px !important;
        padding-bottom: 5px !important;
        justify-content: center;
    }
    
    a.external-link {
        text-decoration: none;
        font-weight: bold;
        color: #d35400 !important;
    }
    a.external-link:hover { text-decoration: underline; }
    
    .ai-box {
        background-color: #fff8e1;
        border-left: 4px solid #d35400;
        padding: 15px;
        border-radius: 5px;
        margin-bottom: 15px;
    }

    .book-tag {
        display: inline-block;
        background-color: #d35400;
        color: white !important;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.75em;
        margin-right: 5px;
        margin-bottom: 5px;
        font-weight: bold;
    }
    </style>
""", unsafe_allow_html=True)

# --- BACKEND ---

def get_connection():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    if "gcp_service_account" in st.secrets:
        try:
            creds_dict = dict(st.secrets["gcp_service_account"])
            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            return gspread.authorize(creds)
        except Exception: return None
    return None

def setup_sheets(client):
    try: sh = client.open("Bücherliste") 
    except: st.error("Fehler: Tabelle 'Bücherliste' nicht gefunden."); st.stop()
    ws_books = sh.sheet1
    try: ws_authors = sh.worksheet("Autoren")
    except: ws_authors = sh.add_worksheet(title="Autoren", rows=1000, cols=1); ws_authors.update_cell(1, 1, "Name")
    return ws_books, ws_authors

def check_structure(ws):
    try:
        head = ws.row_values(1)
        if not head: ws.update_cell(1,1,"Titel"); head=["Titel"]
        needed = ["Titel", "Autor", "Genre", "Bewertung", "Cover", "Hinzugefügt", "Notiz", "Status", "Tags", "Erschienen"]
        next_c = len(head)+1
        for n in needed:
            if not any(h.lower()==n.lower() for h in head):
                ws.update_cell(1, next_c, n); next_c+=1; time.sleep(0.5)
    except: pass

def get_data(ws):
    cols = ["Titel", "Autor", "Genre", "Bewertung", "Cover", "Hinzugefügt", "Notiz", "Status", "Tags", "Erschienen"]
    try:
        raw = ws.get_all_values()
        if len(raw) < 2: return pd.DataFrame(columns=cols)
        h_map = {str(h).strip().lower(): i for i, h in enumerate(raw[0])}
        data = []
        for r in raw[1:]:
            d = {}
            for c in cols:
                idx = h_map.get(c.lower())
                val = r[idx] if idx is not None and idx < len(r) else ""
                d[c] = val
            try:
                raw_val = d["Bewertung"]
                if isinstance(raw_val, str) and raw_val.strip().isdigit(): d["Bewertung"] = int(raw_val)
                elif isinstance(raw_val, (int, float)): d["Bewertung"] = int(raw_val)
                else: d["Bewertung"] = 0
            except: d["Bewertung"] = 0
            if not d["Status"]: d["Status"] = "Gelesen"
            if d["Titel"]: data.append(d)
        return pd.DataFrame(data)
    except: return pd.DataFrame(columns=cols)

def update_single_entry(ws, titel, field, value):
    try:
        cell = ws.find(titel)
        headers = [str(h).lower() for h in ws.row_values(1)]
        col = headers.index(field.lower()) + 1
        ws.update_cell(cell.row, col, value)
        return True
    except: return False

def delete_book(ws, titel):
    try:
        cell = ws.find(titel)
        ws.delete_rows(cell.row)
        return True
    except: return False

def update_full_dataframe(ws, new_df):
    current_data = ws.get_all_values()
    headers = [str(h).lower() for h in current_data[0]]
    col_idx = {k: headers.index(k) for k in ["titel","autor","bewertung","notiz","status"] if k in headers}
    if not col_idx: return False
    
    for index, row in new_df.iterrows():
        try:
            cell = ws.find(row["Titel"])
            if "Bewertung" in row: ws.update_cell(cell.row, col_idx["bewertung"]+1, row["Bewertung"])
            if "Notiz" in row: ws.update_cell(cell.row, col_idx["notiz"]+1, row["Notiz"])
            time.sleep(0.2)
        except: pass
    return True

# --- API & KI HELPER ---
def process_genre(raw):
    if not raw: return "Roman"
    try: 
        t = GoogleTranslator(source='auto', target='de').translate(raw)
        return "Roman" if "römisch" in t.lower() else t
    except: return "Roman"

def fetch_meta(titel, autor):
    c, g, y = "", "Roman", ""
    try:
        r = requests.get(f"https://www.googleapis.com/books/v1/volumes?q={titel} {autor}&maxResults=1").json()
        info = r["items"][0]["volumeInfo"]
        c = info.get("imageLinks", {}).get("thumbnail", "")
        g = process_genre(info.get("categories", ["Roman"])[0])
        pub_date = info.get("publishedDate", "")
        if pub_date: y = pub_date[:4]
    except: pass
    if not c:
        try:
            r = requests.get(f"https://openlibrary.org/search.json?q={titel} {autor}&limit=1").json()
            if r["docs"]: 
                doc = r["docs"][0]
                c = f"https://covers.openlibrary.org/b/id/{doc['cover_i']}-M.jpg"
                if not y and "first_publish_year" in doc: y = str(doc["first_publish_year"])
        except: pass
    return c, g, y

# --- REST API AUFRUF MIT "BRUTE FORCE" MODEL SEARCH ---
def call_gemini_direct(prompt):
    if "gemini_api_key" not in st.secrets: return None, "Kein API Key"
    api_key = st.secrets["gemini_api_key"]
    
    # LISTE ALLER MÖGLICHEN MODELLE (Wir probieren alle durch!)
    # Prio: Flash 1.5 -> Pro 1.5 -> Pro 1.0 -> Flash 1.0
    candidates = [
        "gemini-1.5-flash",
        "gemini-1.5-pro",
        "gemini-1.0-pro",
        "gemini-pro"
    ]
    
    last_error = ""
    
    for model in candidates:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
        headers = {'Content-Type': 'application/json'}
        data = {"contents": [{"parts": [{"text": prompt}]}]}
        
        try:
            response = requests.post(url, headers=headers, json=data)
            
            if response.status_code == 200:
                result = response.json()
                try:
                    text = result['candidates'][0]['content']['parts'][0]['text']
                    clean_text = text.replace("```json", "").replace("```", "").strip()
                    st.toast(f"Erfolg mit Modell: {model}") # Info für dich!
                    return clean_text, None
                except: continue # Format falsch, nächstes Modell
            elif response.status_code == 429:
                return None, "Rate Limit (Warte kurz)" # Bei 429 nicht weitersuchen, einfach warten
            elif response.status_code == 404:
                last_error = f"{model} nicht gefunden (404)"
                continue # Modell existiert nicht für diesen Key -> Nächstes!
            else:
                last_error = f"Fehler {response.status_code} bei {model}"
                continue
                
        except Exception as e: 
            last_error = str(e)
            continue

    return None, f"Alle Modelle gescheitert. Letzter: {last_error}"

@st.cache_data(show_spinner=False)
def get_ai_tags_and_year(titel, autor):
    prompt = f"""
    Buch: "{titel}" von {autor}.
    Aufgabe: 
    1. Gib mir 3-5 Tags (Themen/Stimmung, z.B. #Düster).
    2. Gib mir das Erscheinungsjahr (YYYY).
    Antworte NUR als JSON: {{ "tags": "#Tag1, #Tag2", "year": "YYYY" }}
    """
    raw_text, err = call_gemini_direct(prompt)
    if not raw_text: return {"tags": "", "year": ""}
    try: return json.loads(raw_text)
    except: return {"tags": "", "year": ""}

@st.cache_data(show_spinner=False)
def get_ai_book_info(titel, autor):
    prompt = f"""
    Du bist ein literarischer Assistent. Buch: "{titel}" von {autor}.
    Aufgabe 1: Schreibe einen spannenden Teaser (max 80 Wörter). Keine Spoiler!
    Aufgabe 2: Schreibe eine sehr kurze Biografie über den Autor (max 40 Wörter).
    Antworte im JSON Format: {{ "teaser": "...", "bio": "..." }}
    """
    raw_text, err = call_gemini_direct(prompt)
    if not raw_text: return {"teaser": f"KI Fehler: {err}", "bio": "-"}
    try: return json.loads(raw_text)
    except: return {"teaser": "Fehler beim Lesen", "bio": "-"}

def batch_enrich_books(ws, df):
    headers = [str(h).lower() for h in ws.row_values(1)]
    try: 
        col_tag_idx = headers.index("tags") + 1
        col_year_idx = headers.index("erschienen") + 1
    except: return 0
    
    df_missing = df[ (df["Tags"] == "") | (df["Erschienen"] == "") | (df["Tags"].isnull()) | (df["Erschienen"].isnull()) ]
    if df_missing.empty: return 0
    
    progress_bar = st.sidebar.progress(0)
    status_text = st.sidebar.empty()
    
    count = 0
    total = len(df_missing)
    
    for i, row in df_missing.iterrows():
        status_text.text(f"Analysiere ({count+1}/{total}): {row['Titel']}...")
        
        # KI Call
        ai_res = get_ai_tags_and_year(row["Titel"], row["Autor"])
        
        # Wenn leer (Fehler/Limit)
        if not ai_res.get("tags") and not ai_res.get("year"):
            time.sleep(2)
            continue

        try:
            cell = ws.find(row["Titel"])
            current_tags = row["Tags"]
            current_year = row["Erschienen"]
            if not current_tags and ai_res.get("tags"): ws.update_cell(cell.row, col_tag_idx, ai_res["tags"])
            if not current_year and ai_res.get("year"): ws.update_cell(cell.row, col_year_idx, ai_res["year"])
            count += 1
        except: pass
        
        progress_bar.progress((count + 1) / total)
        time.sleep(6.0) 
        
    status_text.success(f"Fertig! {count} Bücher aktualisiert.")
    time.sleep(2)
    status_text.empty()
    progress_bar.empty()
    return count

def smart_author(short, known):
    s = short.strip().lower()
    for k in sorted(known, key=len, reverse=True):
        if s in str(k).lower(): return k
    return short

def cleanup_author_duplicates_batch(ws_books, ws_authors):
    import unicodedata
    def deep_clean(text): return unicodedata.normalize('NFKC', str(text)).replace('\u00A0', ' ').strip()
    books_vals = ws_books.get_all_values()
    if not books_vals: return 0
    headers = [str(h).lower() for h in books_vals[0]]
    try: idx_a, idx_s = headers.index("autor"), headers.index("status")
    except: return 0
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
    if replacements:
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
        if changed: ws_books.update(new_data); books_vals = new_data 
    final_authors = set()
    for row in books_vals[1:]:
        if len(row) > idx_a and len(row) > idx_s:
            status = row[idx_s].strip()
            auth = row[idx_a].strip()
            if auth and status != "Wunschliste": final_authors.add(auth)
    ws_authors.clear(); ws_authors.update_cell(1,1,"Name")
    if final_authors: ws_authors.update(values=[["Name"]] + [[a] for a in sorted(list(final_authors))])
    return 1

# --- DIALOG (MIT EDITIER-FUNKTION) ---
@st.dialog("📖 Buch-Details")
def show_book_details(book, ws_books, ws_authors):
    d_tab1, d_tab2 = st.tabs(["ℹ️ Info", "✏️ Bearbeiten"])
    
    with d_tab1:
        st.markdown(f"### {book['Titel']}")
        
        # SICHERE ABFRAGE DES JAHRES
        year_val = book.get('Erschienen')
        year_str = f" ({year_val})" if year_val and str(year_val).strip() != "" else ""
        st.markdown(f"**von {book['Autor']}{year_str}**")
        
        col1, col2 = st.columns([1, 2])
        with col1:
            cov = book["Cover"] if book["Cover"] != "-" else "https://via.placeholder.com/200x300?text=No+Cover"
            st.markdown(f'<img src="{cov}" style="width:100%; border-radius:8px; box-shadow:0 2px 8px rgba(0,0,0,0.2);">', unsafe_allow_html=True)
            st.write("")
            if book.get('Bewertung'):
                st.info(f"Bewertung: {'★' * int(book['Bewertung'])}")
            
            if "Tags" in book and book["Tags"]:
                st.write("")
                tags_list = book["Tags"].split(",")
                tag_html = ""
                for t in tags_list:
                    tag_html += f'<span class="book-tag">{t.strip()}</span>'
                st.markdown(tag_html, unsafe_allow_html=True)
            
        with col2:
            ai_data = None
            if "gemini_api_key" in st.secrets:
                with st.spinner("✨ KI liest..."):
                    ai_data = get_ai_book_info(book["Titel"], book["Autor"])
            else: st.warning("Kein API Key.")

            if ai_data:
                st.markdown(f"""
                <div class="ai-box">
                    <b>📖 Worum geht's?</b><br>{ai_data.get('teaser', 'Ladefehler')}
                </div>
                <div class="ai-box" style="border-left-color: #2980b9; background-color: #eaf2f8; margin-top:10px;">
                    <b>👤 Autor</b><br>{ai_data.get('bio', '-')}
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("---")
            wiki_book = f"https://de.wikipedia.org/w/index.php?search={urllib.parse.quote(book['Titel'])}"
            google_search = f"https://www.google.com/search?q={urllib.parse.quote(book['Titel'] + ' ' + book['Autor'])}"
            st.markdown(f"[🔍 Google]({google_search}) | [📖 Wiki]({wiki_book})")

    with d_tab2:
        st.write("Daten bearbeiten.")
        with st.form("edit_book_form"):
            new_title = st.text_input("Titel", value=book["Titel"])
            new_author = st.text_input("Autor", value=book["Autor"])
            
            current_y = book.get("Erschienen", "")
            new_year = st.text_input("Erscheinungsjahr", value=current_y)
            
            current_tags = book.get("Tags", "")
            new_tags = st.text_input("Tags", value=current_tags)
            
            if st.form_submit_button("💾 Änderungen speichern"):
                try:
                    cell = ws_books.find(book["Titel"])
                    headers = [str(h).lower() for h in ws_books.row_values(1)]
                    col_t = headers.index("titel") + 1
                    col_a = headers.index("autor") + 1
                    
                    try: col_tags = headers.index("tags") + 1
                    except: col_tags = len(headers) + 1 
                    try: col_y = headers.index("erschienen") + 1
                    except: col_y = len(headers) + 2
                    
                    ws_books.update_cell(cell.row, col_t, new_title)
                    ws_books.update_cell(cell.row, col_a, new_author)
                    ws_books.update_cell(cell.row, col_tags, new_tags)
                    ws_books.update_cell(cell.row, col_y, new_year)
                    
                    cleanup_author_duplicates_batch(ws_books, ws_authors)
                    del st.session_state.df_books
                    st.success("Gespeichert!")
                    time.sleep(1); st.rerun()
                except: st.error("Fehler beim Speichern")

        st.markdown("---")
        st.markdown("**Gefahrenzone**")
        if st.button("🗑️ Buch unwiderruflich löschen", type="primary"):
            if delete_book(ws_books, book["Titel"]):
                del st.session_state.df_books
                st.success("Gelöscht!")
                time.sleep(1); st.rerun()
            else: st.error("Fehler beim Löschen.")

# --- MAIN ---
def main():
    st.title("📚 Meine Bibliothek")
    
    client = get_connection()
    if not client: st.error("Secrets fehlen!"); st.stop()
    ws_books, ws_authors = setup_sheets(client)
    
    if "checked" not in st.session_state: check_structure(ws_books); st.session_state.checked=True
    if "df_books" not in st.session_state: 
        with st.spinner("Lade Daten..."): st.session_state.df_books = get_data(ws_books)
    
    # --- HOTFIX: MIGRATION CHECK ---
    if "df_books" in st.session_state:
        if "Erschienen" not in st.session_state.df_books.columns:
            st.session_state.df_books = get_data(ws_books)
            st.rerun()

    df = st.session_state.df_books
    authors = list(set([a for i, row in df.iterrows() if row["Status"] != "Wunschliste" for a in [row["Autor"]] if a]))
    
    # --- SIDEBAR ---
    with st.sidebar:
        st.write("🔧 **Einstellungen**")
        if st.button("🔄 Cache leeren"): 
            st.session_state.clear(); st.rerun()
        
        st.markdown("---")
        st.write("🤖 **KI-Tools**")
        if st.button("✨ Daten anreichern (Tags & Jahr)"):
            count = batch_enrich_books(ws_books, df)
            if count > 0:
                st.success(f"{count} Bücher aktualisiert!")
                del st.session_state.df_books
                time.sleep(2); st.rerun()
            else: st.info("Alles aktuell.")

    tab_neu, tab_sammlung, tab_merkliste, tab_stats = st.tabs(["✍️ Neu", "🔍 Sammlung", "🔮 Merkliste", "👥 Statistik & Autoren"])
    
    # --- NEU ---
    with tab_neu:
        st.header("Buch hinzufügen")
        with st.form("add", clear_on_submit=True):
            c1, c2 = st.columns([2, 1])
            with c1: inp = st.text_input("Titel, Autor")
            with c2: 
                note = st.text_input("Notiz")
                rate = st.feedback("stars")
            if st.form_submit_button("Speichern"):
                if "," in inp:
                    val = (rate + 1) if rate is not None else 0
                    t, a = [x.strip() for x in inp.split(",", 1)]
                    fa = smart_author(a, authors)
                    with st.spinner("Lade Metadaten (inkl. Jahr & Tags)..."):
                        c, g, y = fetch_meta(t, fa)
                        # KI Tags (REST API)
                        ai_res = get_ai_tags_and_year(t, fa)
                        tags = ai_res["tags"] if ai_res else ""
                        if not y and ai_res: y = ai_res["year"]
                        
                        ws_books.append_row([t, fa, g, val, c or "-", datetime.now().strftime("%Y-%m-%d"), note, "Gelesen", tags, y])
                        cleanup_author_duplicates_batch(ws_books, ws_authors)
                        del st.session_state.df_books
                    st.success(f"Gespeichert: {t}"); st.balloons(); time.sleep(1.0); st.rerun()
                else: st.error("Format: Titel, Autor")

    # --- SAMMLUNG ---
    with tab_sammlung:
        view = st.radio("Ansicht", ["Kacheln", "Liste"], horizontal=True, label_visibility="collapsed")
        df_s = df[df["Status"] == "Gelesen"].copy()
        q = st.text_input("Suche...", label_visibility="collapsed")
        if q: df_s = df_s[df_s["Titel"].str.lower().str.contains(q.lower())]
        
        if view == "Liste":
            df_list = df_s[["Titel", "Autor", "Bewertung", "Notiz"]].copy()
            df_list.insert(0, "ℹ️", False)
            edited_df = st.data_editor(
                df_list,
                column_config={
                    "ℹ️": st.column_config.CheckboxColumn("Info", width="small"),
                    "Titel": st.column_config.TextColumn(disabled=True),
                    "Autor": st.column_config.TextColumn(disabled=True),
                    "Bewertung": st.column_config.NumberColumn("⭐", min_value=0, max_value=5, step=1),
                    "Notiz": st.column_config.TextColumn(width="large")
                }, hide_index=True, use_container_width=True, key="editor_list"
            )
            if edited_df["ℹ️"].any():
                sel_idx = edited_df[edited_df["ℹ️"]].index[0]
                show_book_details(df_s.loc[sel_idx], ws_books, ws_authors)
            if st.button("💾 Änderungen in Liste speichern"):
                if update_full_dataframe(ws_books, edited_df):
                    st.success("Aktualisiert!"); del st.session_state.df_books; time.sleep(1); st.rerun()
        else:
            cols = st.columns(3)
            for i, (idx, row) in enumerate(df_s.iterrows()):
                with cols[i % 3]:
                    with st.container(border=True):
                        c1, c2 = st.columns([1, 2])
                        with c1:
                            st.image(row["Cover"] if row["Cover"]!="-" else "https://via.placeholder.com/100", use_container_width=True)
                            if st.button("ℹ️ Info", key=f"k_{idx}"): show_book_details(row, ws_books, ws_authors)
                        with c2:
                            st.write(f"**{row['Titel']}**")
                            meta_line = row["Autor"]
                            if row.get("Erschienen"): meta_line += f" ({row['Erschienen']})"
                            if "Tags" in row and row["Tags"]: 
                                first_tag = row["Tags"].split(",")[0]
                                meta_line += f" • {first_tag}"
                            st.caption(meta_line)
                            
                            try: star_val = int(row['Bewertung'])
                            except: star_val = 0
                            new_stars = st.feedback("stars", key=f"fb_{idx}")
                            if f"fb_{idx}" in st.session_state and st.session_state[f"fb_{idx}"] is not None:
                                user_val = st.session_state[f"fb_{idx}"] + 1
                                if user_val != star_val:
                                    update_single_entry(ws_books, row["Titel"], "Bewertung", user_val)
                                    st.toast("Bewertung gespeichert!"); del st.session_state.df_books; time.sleep(0.2); st.rerun()
                            elif star_val > 0: st.markdown(f"<div style='color:#d35400'>{'★'*star_val}</div>", unsafe_allow_html=True)

                            old_n = row["Notiz"]
                            new_n = st.text_area("Notiz", old_n, key=f"n_{idx}", height=70, label_visibility="collapsed")
                            if new_n != old_n:
                                update_single_entry(ws_books, row["Titel"], "Notiz", new_n)
                                st.toast("Gespeichert!"); del st.session_state.df_books; time.sleep(0.5); st.rerun()

    # --- MERKLISTE ---
    with tab_merkliste:
        w_view = st.radio("Wunschliste Ansicht", ["Kacheln", "Liste"], horizontal=True, label_visibility="collapsed")
        with st.expander("➕ Neuer Wunsch"):
            with st.form("wish", clear_on_submit=True):
                iw = st.text_input("Titel, Autor")
                inote = st.text_input("Notiz")
                if st.form_submit_button("Hinzufügen"):
                    if "," in iw:
                        t, a = [x.strip() for x in iw.split(",", 1)]
                        c, g, y = fetch_meta(t, a)
                        ai_res = get_ai_tags_and_year(t, a)
                        tags = ai_res["tags"] if ai_res else ""
                        if not y and ai_res: y = ai_res["year"]
                        ws_books.append_row([t, a, g, "", c or "-", datetime.now().strftime("%Y-%m-%d"), inote, "Wunschliste", tags, y])
                        del st.session_state.df_books; st.success("Gemerkt!"); st.balloons(); time.sleep(1); st.rerun()
        
        df_w = df[df["Status"] == "Wunschliste"].copy()
        if not df_w.empty:
            if w_view == "Kacheln":
                cols = st.columns(3)
                for i, (idx, row) in enumerate(df_w.iterrows()):
                    with cols[i % 3]:
                        with st.container(border=True):
                            c1, c2 = st.columns([1, 2])
                            with c1:
                                st.image(row["Cover"] if row["Cover"]!="-" else "https://via.placeholder.com/100", use_container_width=True)
                                if st.button("ℹ️ Info", key=f"wk_{idx}"): show_book_details(row, ws_books, ws_authors)
                                if st.button("✅ Gelesen", key=f"wr_{idx}"):
                                    cell = ws_books.find(row["Titel"])
                                    ws_books.update_cell(cell.row, 8, "Gelesen")
                                    ws_books.update_cell(cell.row, 6, datetime.now().strftime("%Y-%m-%d"))
                                    cleanup_author_duplicates_batch(ws_books, ws_authors)
                                    del st.session_state.df_books; st.rerun()
                            with c2:
                                st.write(f"**{row['Titel']}**")
                                meta_line = row["Autor"]
                                if row.get("Erschienen"): meta_line += f" ({row['Erschienen']})"
                                st.caption(meta_line)
                                old_n = row["Notiz"]
                                new_n = st.text_area("Notiz", old_n, key=f"wn_{idx}", height=70, label_visibility="collapsed")
                                if new_n != old_n:
                                    update_single_entry(ws_books, row["Titel"], "Notiz", new_n)
                                    st.toast("Gespeichert!"); del st.session_state.df_books; time.sleep(0.5); st.rerun()
            else:
                df_w_list = df_w[["Titel", "Autor", "Notiz"]].copy()
                df_w_list.insert(0, "ℹ️", False)
                edited_w = st.data_editor(
                    df_w_list, 
                    column_config={"ℹ️": st.column_config.CheckboxColumn("Info", width="small")},
                    hide_index=True, use_container_width=True, key="editor_wish"
                )
                if edited_w["ℹ️"].any():
                    sel_idx = edited_w[edited_w["ℹ️"]].index[0]
                    show_book_details(df_w.loc[sel_idx], ws_books, ws_authors)
        else: st.info("Leer.")

    # --- STATISTIK & AUTOREN ---
    with tab_stats:
        st.header("📊 Statistik & Autoren")
        df_r = df[df["Status"] == "Gelesen"]
        c1, c2 = st.columns(2)
        c1.metric("Gelesen", len(df_r))
        if not df_r.empty:
            top = df_r["Autor"].mode()[0]
            c2.metric("Top Autor", top)
            
            st.markdown("---")
            st.subheader("Lieblings-Themen (Tags)")
            all_tags = []
            if "Tags" in df_r.columns:
                for t in df_r["Tags"].dropna():
                    all_tags.extend([x.strip() for x in t.split(",") if x.strip()])
                if all_tags:
                    tag_counts = pd.Series(all_tags).value_counts().reset_index()
                    tag_counts.columns = ["Thema", "Anzahl"]
                    st.dataframe(tag_counts, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            st.subheader("👥 Autoren Liste")
            auth_counts = df_r["Autor"].value_counts().reset_index()
            auth_counts.columns = ["Autor", "Anzahl Bücher"]
            st.dataframe(auth_counts, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
