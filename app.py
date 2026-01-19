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

# --- CSS DESIGN ---
st.markdown("""
    <style>
    .stApp { background-color: #f5f5dc !important; }
    h1, h2, h3, h4, h5, h6, p, div, span, label, li { color: #2c3e50 !important; }
    
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

    /* Kacheln Design */
    .book-card {
        background-color: #eaddcf;
        border: 1px solid #d35400;
        border-radius: 12px;
        padding: 15px;
        text-align: center;
        height: 100%;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .book-card img {
        max-width: 100px;
        border-radius: 6px;
        margin-bottom: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.2);
        margin-left: auto;
        margin-right: auto;
    }
    .book-title {
        font-weight: 800 !important;
        font-size: 1.1em !important;
        margin-bottom: 4px;
        color: #000000 !important;
        line-height: 1.2;
    }
    .book-author {
        font-size: 0.9em;
        color: #555 !important;
        margin-bottom: 8px;
        font-style: italic;
    }
    .book-note {
        background-color: #fffaf0;
        border: 1px dashed #d35400;
        border-radius: 6px;
        padding: 5px;
        font-size: 0.85em;
        color: #4a3b2a;
        margin-top: 8px;
        margin-bottom: 8px;
        text-align: left;
    }

    /* Navigation */
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
        needed = ["Titel", "Autor", "Genre", "Bewertung", "Cover", "Hinzugefügt", "Notiz", "Status"]
        next_c = len(head)+1
        for n in needed:
            if not any(h.lower()==n.lower() for h in head):
                ws.update_cell(1, next_c, n); next_c+=1; time.sleep(0.5)
    except: pass

def get_data(ws):
    cols = ["Titel", "Autor", "Genre", "Bewertung", "Cover", "Hinzugefügt", "Notiz", "Status"]
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
            
            # Typkonvertierung
            try: 
                if d["Bewertung"] and str(d["Bewertung"]).strip().isdigit():
                    d["Bewertung"] = int(d["Bewertung"])
                else:
                    d["Bewertung"] = 0
            except: d["Bewertung"] = 0
            
            if not d["Status"]: d["Status"] = "Gelesen"
            if d["Titel"]: data.append(d)
            
        return pd.DataFrame(data)
    except: return pd.DataFrame(columns=cols)

def update_full_dataframe(ws, new_df):
    current_data = ws.get_all_values()
    headers = [str(h).lower() for h in current_data[0]]
    
    col_idx = {
        "titel": headers.index("titel"),
        "autor": headers.index("autor"),
        "bewertung": headers.index("bewertung"),
        "notiz": headers.index("notiz"),
        "status": headers.index("status")
    } if "titel" in headers else {}
    
    if not col_idx: return False

    rows_to_delete = [] 
    
    for index, row in new_df.iterrows():
        titel = row["Titel"]
        
        if row.get("Löschen", False):
            try:
                cell = ws.find(titel)
                rows_to_delete.append(cell.row)
            except: pass
            continue

        try:
            cell = ws.find(titel)
            ws.update_cell(cell.row, col_idx["bewertung"]+1, row["Bewertung"])
            ws.update_cell(cell.row, col_idx["notiz"]+1, row["Notiz"])
            ws.update_cell(cell.row, col_idx["autor"]+1, row["Autor"])
            time.sleep(0.3) 
        except: pass

    rows_to_delete.sort(reverse=True)
    for r in rows_to_delete:
        ws.delete_rows(r)
        time.sleep(0.5)
        
    return True

def update_single_entry(ws, titel, field, value):
    try:
        cell = ws.find(titel)
        headers = [str(h).lower() for h in ws.row_values(1)]
        col = headers.index(field.lower()) + 1
        ws.update_cell(cell.row, col, value)
        return True
    except: return False

def process_genre(raw):
    if not raw: return "Roman"
    try: 
        t = GoogleTranslator(source='auto', target='de').translate(raw)
        return "Roman" if "römisch" in t.lower() else t
    except: return "Roman"

def fetch_meta(titel, autor):
    c, g = "", "Roman"
    try:
        r = requests.get(f"https://www.googleapis.com/books/v1/volumes?q={titel} {autor}&maxResults=1").json()
        info = r["items"][0]["volumeInfo"]
        c = info.get("imageLinks", {}).get("thumbnail", "")
        g = process_genre(info.get("categories", ["Roman"])[0])
    except: pass
    if not c:
        try:
            r = requests.get(f"https://openlibrary.org/search.json?q={titel} {autor}&limit=1").json()
            if r["docs"]: c = f"https://covers.openlibrary.org/b/id/{r['docs'][0]['cover_i']}-M.jpg"
        except: pass
    return c, g

def smart_author(short, known):
    s = short.strip().lower()
    for k in sorted(known, key=len, reverse=True):
        if s in str(k).lower(): return k
    return short

# --- MAIN ---
def main():
    st.title("📚 Meine Bibliothek")
    
    if st.sidebar.button("🚨 Cache Reset"): st.session_state.clear(); st.rerun()
    
    client = get_connection()
    if not client: st.error("Secrets fehlen!"); st.stop()
    ws_books, ws_authors = setup_sheets(client)
    
    if "checked" not in st.session_state: check_structure(ws_books); st.session_state.checked=True
    
    if "df_books" not in st.session_state: 
        with st.spinner("Lade Daten..."): st.session_state.df_books = get_data(ws_books)
    
    df = st.session_state.df_books
    authors = list(set([a for a in df["Autor"] if a]))
    
    nav = st.radio("Menü", ["✍️ Neu (Gelesen)", "🔍 Sammlung", "🔮 Merkliste", "👥 Autoren"], horizontal=True, label_visibility="collapsed")
    
    # ------------------------------------------------------------------
    # TAB: NEU
    # ------------------------------------------------------------------
    if nav == "✍️ Neu (Gelesen)":
        st.header("Buch hinzufügen")
        with st.container(border=True):
            c1, c2 = st.columns([2, 1])
            with c1:
                inp = st.text_input("Titel, Autor (mit Komma trennen!)")
                # HIER GEÄNDERT: st.text_input statt st.text_area für Enter-Submit
                note = st.text_input("Notiz (Enter zum Bestätigen im Feld)")
            with c2:
                st.write("Bewertung:")
                rating_idx = st.feedback("stars") 
                rating_val = (rating_idx + 1) if rating_idx is not None else 0
            
            if st.button("💾 In Bibliothek speichern"):
                if "," in inp:
                    tit, aut = [x.strip() for x in inp.split(",", 1)]
                    final_aut = smart_author(aut, authors)
                    with st.spinner("Suche Cover & Metadaten..."):
                        cov, gen = fetch_meta(tit, final_aut)
                        ws_books.append_row([tit, final_aut, gen, rating_val, cov or "-", datetime.now().strftime("%Y-%m-%d"), note, "Gelesen"])
                        del st.session_state.df_books
                    st.balloons(); time.sleep(1); st.rerun()
                else: st.error("Bitte 'Titel, Autor' mit Komma eingeben.")

    # ------------------------------------------------------------------
    # TAB: SAMMLUNG
    # ------------------------------------------------------------------
    elif nav == "🔍 Sammlung":
        col_h, col_v = st.columns([3, 1])
        with col_h: st.header("Gelesene Bücher")
        with col_v: view = st.radio("Ansicht", ["Liste", "Kacheln"], horizontal=True, label_visibility="collapsed")
        
        df_show = st.session_state.df_books.copy()
        df_show = df_show[ (df_show["Status"] == "Gelesen") ]
        
        q = st.text_input("🔍 Filter (Titel, Autor, Notiz...)", label_visibility="collapsed", placeholder="Suchen...")
        if q:
            q = q.lower()
            df_show = df_show[df_show["Titel"].str.lower().str.contains(q) | df_show["Autor"].str.lower().str.contains(q) | df_show["Notiz"].str.lower().str.contains(q)]

        try: df_show["Hinzugefügt"] = pd.to_datetime(df_show["Hinzugefügt"], errors='coerce')
        except: pass
        df_show = df_show.sort_values(by="Hinzugefügt", ascending=False)

        # --- LISTE ---
        if view == "Liste":
            df_editor = df_show[["Titel", "Autor", "Bewertung", "Cover", "Notiz", "Hinzugefügt"]].copy()
            df_editor["Löschen"] = False 
            
            edited_df = st.data_editor(
                df_editor,
                column_order=["Titel", "Autor", "Bewertung", "Cover", "Notiz", "Hinzugefügt", "Löschen"],
                column_config={
                    "Titel": st.column_config.TextColumn(disabled=True),
                    "Autor": st.column_config.TextColumn("Autor"),
                    "Bewertung": st.column_config.NumberColumn("⭐", min_value=1, max_value=5, step=1, help="1-5"),
                    "Cover": st.column_config.ImageColumn("Img", width="small"),
                    "Notiz": st.column_config.TextColumn("Notiz (Editierbar)", width="large"),
                    "Hinzugefügt": st.column_config.DateColumn("Datum", disabled=True, format="DD.MM.YYYY"),
                    "Löschen": st.column_config.CheckboxColumn("🗑️", help="Zum Löschen anhaken")
                },
                hide_index=True,
                use_container_width=True,
                num_rows="fixed"
            )
            
            if st.button("💾 Änderungen anwenden (Speichern/Löschen)"):
                with st.spinner("Synchronisiere mit Google Sheets..."):
                    success = update_full_dataframe(ws_books, edited_df)
                    if success:
                        del st.session_state.df_books
                        st.success("Erledigt!")
                        time.sleep(1)
                        st.rerun()
                    else: st.error("Fehler beim Speichern.")

        # --- KACHELN ---
        else:
            cols = st.columns(4) 
            for i, (idx, row) in enumerate(df_show.iterrows()):
                with cols[i % 4]:
                    with st.container(border=True):
                        cov = row["Cover"] if row["Cover"] != "-" else "https://via.placeholder.com/150x220?text=No+Cover"
                        
                        try: stars_val = int(row['Bewertung'])
                        except: stars_val = 0
                        
                        # Notiz vorbereiten (wenn vorhanden)
                        note_html = ""
                        if row["Notiz"] and row["Notiz"].strip():
                            note_html = f'<div class="book-note">📝 {row["Notiz"]}</div>'

                        st.markdown(f"""
                            <div class="book-card">
                                <div>
                                    <img src="{cov}">
                                    <div class="book-title">{row['Titel']}</div>
                                    <div class="book-author">{row['Autor']}</div>
                                    <div style="color:#d35400;">{'★' * stars_val}<span style="color:#ccc;">{'★' * (5-stars_val)}</span></div>
                                </div>
                                {note_html}
                            </div>
                        """, unsafe_allow_html=True)
                        
                        with st.popover("✏️ Bearbeiten", use_container_width=True):
                            st.write(f"**{row['Titel']}** bearbeiten")
                            # HIER GEÄNDERT: st.text_input statt st.text_area
                            new_note = st.text_input("Notiz", value=row["Notiz"], key=f"note_{idx}")
                            
                            new_stars_idx = st.feedback("stars", key=f"stars_{idx}")
                            final_rating = stars_val
                            if new_stars_idx is not None:
                                final_rating = new_stars_idx + 1

                            if st.button("Speichern", key=f"save_{idx}"):
                                update_single_entry(ws_books, row["Titel"], "Notiz", new_note)
                                if new_stars_idx is not None:
                                    update_single_entry(ws_books, row["Titel"], "Bewertung", final_rating)
                                
                                st.toast("Gespeichert!")
                                del st.session_state.df_books
                                time.sleep(1)
                                st.rerun()

    # ------------------------------------------------------------------
    # TAB: MERKLISTE
    # ------------------------------------------------------------------
    elif nav == "🔮 Merkliste":
        st.header("Wunschliste")
        with st.expander("➕ Neuen Wunsch hinzufügen", expanded=False):
            i_w = st.text_input("Titel, Autor")
            # HIER GEÄNDERT: st.text_input
            n_w = st.text_input("Notiz")
            if st.button("Auf Merkliste"):
                if "," in i_w:
                    t, a = [x.strip() for x in i_w.split(",",1)]
                    c, g = fetch_meta(t, a)
                    ws_books.append_row([t, a, g, "", c or "-", datetime.now().strftime("%Y-%m-%d"), n_w, "Wunschliste"])
                    del st.session_state.df_books; st.rerun()
        
        df_w = st.session_state.df_books[st.session_state.df_books["Status"]=="Wunschliste"].copy()
        if not df_w.empty:
            for i, r in df_w.iterrows():
                with st.container(border=True):
                    c1, c2, c3 = st.columns([1,4,1])
                    if r["Cover"]!="-": c1.image(r["Cover"], width=60)
                    else: c1.write("📚")
                    c2.subheader(r["Titel"])
                    c2.write(f"{r['Autor']} | 📝 {r['Notiz']}")
                    if c3.button("✅ Gelesen", key=f"w_{i}"):
                        cell = ws_books.find(r["Titel"])
                        ws_books.update_cell(cell.row, 8, "Gelesen")
                        ws_books.update_cell(cell.row, 6, datetime.now().strftime("%Y-%m-%d"))
                        del st.session_state.df_books; st.rerun()
        else: st.info("Merkliste leer.")

    # ------------------------------------------------------------------
    # TAB: AUTOREN
    # ------------------------------------------------------------------
    elif nav == "👥 Autoren":
        st.header("Autoren")
        df = st.session_state.df_books
        if not df.empty:
            auth_counts = df["Autor"].value_counts().reset_index()
            auth_counts.columns = ["Autor", "Anzahl Bücher"]
            st.dataframe(auth_counts, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
