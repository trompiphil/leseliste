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

# --- CSS DESIGN (Clean & High Contrast) ---
st.markdown("""
    <style>
    /* Globaler Reset für Farben */
    .stApp { background-color: #f5f5dc !important; }
    h1, h2, h3, h4, h5, h6, p, div, span, label, li { color: #2c3e50 !important; }
    
    /* Eingabefelder */
    .stTextInput input, .stTextArea textarea {
        background-color: #fffaf0 !important;
        border: 2px solid #d35400 !important;
        color: #000000 !important;
    }
    
    /* Buttons */
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

    /* Kacheln */
    .book-card {
        background-color: #eaddcf;
        border: 1px solid #d35400;
        border-radius: 12px;
        padding: 15px;
        text-align: center;
        height: 100%;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        transition: transform 0.2s;
    }
    .book-card:hover { transform: translateY(-3px); }
    .book-card img {
        max-width: 110px;
        border-radius: 6px;
        margin-bottom: 12px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }
    .book-title {
        font-weight: 800 !important;
        font-size: 1.1em !important;
        margin-bottom: 4px;
        color: #000000 !important;
        line-height: 1.3;
    }
    .book-author {
        font-size: 0.95em;
        color: #555 !important;
        margin-bottom: 10px;
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

# --- BACKEND FUNKTIONEN ---

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
            
            # Typkonvertierung für sauberes Editieren
            # Bewertung zu Zahl (oder 0)
            try: d["Bewertung"] = int(d["Bewertung"]) if d["Bewertung"] else 0
            except: d["Bewertung"] = 0
            
            if not d["Status"]: d["Status"] = "Gelesen"
            if d["Titel"]: data.append(d)
            
        return pd.DataFrame(data)
    except: return pd.DataFrame(columns=cols)

def update_full_dataframe(ws, new_df):
    """
    Überschreibt die Tabelle basierend auf dem editierten DataFrame.
    Sucht Zeilen anhand des Titels (oder Position, hier vereinfacht via Titel-Match für Updates).
    Für Bulk-Delete und Bulk-Edit.
    """
    # Wir lesen erst die aktuelle Sheet-Daten, um die Zeilennummern zu haben
    current_data = ws.get_all_values()
    headers = [str(h).lower() for h in current_data[0]]
    
    # Mapping Spaltenname -> Index
    col_idx = {
        "titel": headers.index("titel"),
        "autor": headers.index("autor"),
        "bewertung": headers.index("bewertung"),
        "notiz": headers.index("notiz"),
        "status": headers.index("status")
    } if "titel" in headers else {}
    
    if not col_idx: return False

    # Updates sammeln
    updates = []
    rows_to_delete = [] # Wir sammeln Titel zum Löschen
    
    # new_df hat eine Spalte "Löschen" (bool)
    
    for index, row in new_df.iterrows():
        # Titel dient als ID (Achtung: Titeländerung hier schwierig, wir nehmen an Titel bleibt ID)
        titel = row["Titel"]
        
        if row.get("Löschen", False):
            # Zum Löschen vormerken
            try:
                cell = ws.find(titel)
                rows_to_delete.append(cell.row)
            except: pass
            continue

        # Daten Update
        try:
            cell = ws.find(titel)
            # Wir updaten Zelle für Zelle ist zu langsam. Batch Update Row ist besser.
            # Hier vereinfacht: Wir prüfen Änderungen.
            
            # Bewertung update
            ws.update_cell(cell.row, col_idx["bewertung"]+1, row["Bewertung"])
            # Notiz update
            ws.update_cell(cell.row, col_idx["notiz"]+1, row["Notiz"])
            # Autor update (falls korrigiert)
            ws.update_cell(cell.row, col_idx["autor"]+1, row["Autor"])
            
            time.sleep(0.3) # Rate limit protection
        except: pass

    # Löschen (Rückwärts, damit Indizes stimmen)
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

# --- QUELLEN ---
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
    
    # Setup
    client = get_connection()
    if not client: st.error("Secrets fehlen!"); st.stop()
    ws_books, ws_authors = setup_sheets(client)
    
    if "checked" not in st.session_state: check_structure(ws_books); st.session_state.checked=True
    
    # Load Data
    if "df_books" not in st.session_state: 
        with st.spinner("Lade Daten..."): st.session_state.df_books = get_data(ws_books)
    
    # Autoren Sync (simpel)
    df = st.session_state.df_books
    authors = list(set([a for a in df["Autor"] if a]))
    
    # Navigation
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
                note = st.text_area("Notiz", height=100)
            with c2:
                st.write("Bewertung:")
                # NEU: Klickbare Sterne
                rating_idx = st.feedback("stars") # Gibt 0-4 zurück
                rating_val = (rating_idx + 1) if rating_idx is not None else 0
            
            if st.button("💾 In Bibliothek speichern"):
                if "," in inp:
                    tit, aut = [x.strip() for x in inp.split(",", 1)]
                    final_aut = smart_author(aut, authors)
                    with st.spinner("Suche Cover & Metadaten..."):
                        cov, gen = fetch_meta(tit, final_aut)
                        ws_books.append_row([tit, final_aut, gen, rating_val, cov or "-", datetime.now().strftime("%Y-%m-%d"), note, "Gelesen"])
                        # Autoren Update Logic hier weggelassen für Speed, passiert beim Reload
                        del st.session_state.df_books
                    st.balloons(); time.sleep(1); st.rerun()
                else: st.error("Bitte 'Titel, Autor' mit Komma eingeben.")

    # ------------------------------------------------------------------
    # TAB: SAMMLUNG (LISTE & KACHELN)
    # ------------------------------------------------------------------
    elif nav == "🔍 Sammlung":
        col_h, col_v = st.columns([3, 1])
        with col_h: st.header("Gelesene Bücher")
        with col_v: view = st.radio("Ansicht", ["Liste", "Kacheln"], horizontal=True, label_visibility="collapsed")
        
        # Daten filtern
        df_show = st.session_state.df_books.copy()
        df_show = df_show[ (df_show["Status"] == "Gelesen") ]
        
        # Suche
        q = st.text_input("🔍 Filter (Titel, Autor, Notiz...)", label_visibility="collapsed", placeholder="Suchen...")
        if q:
            q = q.lower()
            df_show = df_show[df_show["Titel"].str.lower().str.contains(q) | df_show["Autor"].str.lower().str.contains(q) | df_show["Notiz"].str.lower().str.contains(q)]

        # Sortierung nach Datum (neu oben)
        try: df_show["Hinzugefügt"] = pd.to_datetime(df_show["Hinzugefügt"], errors='coerce')
        except: pass
        df_show = df_show.sort_values(by="Hinzugefügt", ascending=False)

        # --- LISTEN ANSICHT (BEARBEITBAR) ---
        if view == "Liste":
            # Spalten vorbereiten für Editor
            # Reihenfolge: Titel, Autor, Bewertung, Cover, Notiz, Datum, LÖSCHEN
            df_editor = df_show[["Titel", "Autor", "Bewertung", "Cover", "Notiz", "Hinzugefügt"]].copy()
            df_editor["Löschen"] = False # Checkbox Spalte
            
            edited_df = st.data_editor(
                df_editor,
                column_order=["Titel", "Autor", "Bewertung", "Cover", "Notiz", "Hinzugefügt", "Löschen"],
                column_config={
                    "Titel": st.column_config.TextColumn(disabled=True), # Titel als ID lieber nicht ändern
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
            
            # Speicher Button
            if st.button("💾 Änderungen anwenden (Speichern/Löschen)"):
                # Unterschiede finden oder einfach alles relevante updaten
                with st.spinner("Synchronisiere mit Google Sheets..."):
                    # Wir übergeben das editierte DF an die Update Funktion
                    success = update_full_dataframe(ws_books, edited_df)
                    if success:
                        del st.session_state.df_books
                        st.success("Erledigt!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Fehler beim Speichern.")

        # --- KACHEL ANSICHT (BEARBEITBAR via Popover) ---
        else:
            cols = st.columns(4) # 4 Spalten für Clean Look auf Wide
            for i, (idx, row) in enumerate(df_show.iterrows()):
                with cols[i % 4]:
                    with st.container(border=True):
                        # Cover Anzeige
                        cov = row["Cover"] if row["Cover"] != "-" else "https://via.placeholder.com/150x220?text=No+Cover"
                        st.markdown(f"""
                            <div style="text-align:center;">
                                <img src="{cov}" style="max-height:140px; border-radius:5px; margin-bottom:10px;">
                                <div style="font-weight:bold; height:50px; overflow:hidden; display:flex; align-items:center; justify-content:center;">{row['Titel']}</div>
                                <div style="color:gray; font-size:0.9em; margin-bottom:5px;">{row['Autor']}</div>
                                <div style="color:#d35400;">{'★' * int(row['Bewertung'])}<span style="color:#ccc;">{'★' * (5-int(row['Bewertung']))}</span></div>
                            </div>
                        """, unsafe_allow_html=True)
                        
                        # Bearbeiten Popover
                        with st.popover("✏️ Bearbeiten", use_container_width=True):
                            st.write(f"**{row['Titel']}** bearbeiten")
                            
                            # Formular im Popover
                            new_note = st.text_area("Notiz", value=row["Notiz"], key=f"note_{idx}")
                            
                            # Sterne im Popover
                            # Feedback widget returns 0-4, wir müssen mappen
                            curr_stars = int(row["Bewertung"]) - 1 if row["Bewertung"] > 0 else 0
                            new_stars_idx = st.feedback("stars", key=f"stars_{idx}")
                            
                            # Wenn Feedback noch nicht berührt wurde, ist es None -> Default nehmen
                            if new_stars_idx is None:
                                final_stars = row["Bewertung"] # Alter Wert
                            else:
                                final_stars = new_stars_idx + 1 # Neuer Wert

                            if st.button("Speichern", key=f"save_{idx}"):
                                update_single_entry(ws_books, row["Titel"], "Notiz", new_note)
                                # Nur updaten wenn Sterne sich geändert haben via Widget (das ist tricky in Streamlit)
                                # Vereinfachung: Wir schreiben den Wert aus dem Feedback Widget, wenn vorhanden
                                if new_stars_idx is not None:
                                    update_single_entry(ws_books, row["Titel"], "Bewertung", final_stars)
                                
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
            n_w = st.text_area("Notiz")
            if st.button("Auf Merkliste"):
                if "," in i_w:
                    t, a = [x.strip() for x in i_w.split(",",1)]
                    c, g = fetch_meta(t, a)
                    ws_books.append_row([t, a, g, "", c or "-", datetime.now().strftime("%Y-%m-%d"), n_w, "Wunschliste"])
                    del st.session_state.df_books; st.rerun()
        
        # Merkliste als Tabelle (einfacher)
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
                        # Spalte 8 ist Status (A=1... H=8)
                        ws_books.update_cell(cell.row, 8, "Gelesen")
                        # Datum auf heute setzen? Spalte 6 (F)
                        ws_books.update_cell(cell.row, 6, datetime.now().strftime("%Y-%m-%d"))
                        del st.session_state.df_books; st.rerun()
        else: st.info("Merkliste leer.")

    # ------------------------------------------------------------------
    # TAB: AUTOREN
    # ------------------------------------------------------------------
    elif nav == "👥 Autoren":
        st.header("Autoren")
        # Einfache Liste
        df = st.session_state.df_books
        if not df.empty:
            auth_counts = df["Autor"].value_counts().reset_index()
            auth_counts.columns = ["Autor", "Anzahl Bücher"]
            st.dataframe(auth_counts, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
