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
import re

# --- KONFIGURATION ---
st.set_page_config(page_title="Meine Bibliothek", page_icon="📚", layout="wide")

# --- STATE INIT ---
NAV_OPTIONS = ["✍️ Neu", "🔍 Sammlung", "🔮 Merkliste", "👥 Statistik"]
if "active_tab" not in st.session_state: st.session_state.active_tab = NAV_OPTIONS[1]
if st.session_state.active_tab not in NAV_OPTIONS: st.session_state.active_tab = NAV_OPTIONS[1]

# --- CSS DESIGN ---
st.markdown("""
    <style>
    .stApp { background-color: #f5f5dc !important; }
    h1, h2, h3, h4, h5, h6, p, div, span, label, li, textarea, input, a { color: #2c3e50 !important; }
    .stTextInput input, .stTextArea textarea { background-color: #fffaf0 !important; border: 2px solid #d35400 !important; color: #000000 !important; }
    .stButton button { background-color: #d35400 !important; color: white !important; border-radius: 8px; border: none; font-weight: bold; }
    .stButton button:hover { background-color: #e67e22 !important; }
    [data-testid="stVerticalBlockBorderWrapper"] > div { background-color: #eaddcf; border-radius: 12px; border: 1px solid #d35400; box-shadow: 2px 2px 5px rgba(0,0,0,0.1); padding: 10px; }
    .ai-box { background-color: #fff8e1; border-left: 4px solid #d35400; padding: 15px; border-radius: 5px; margin-bottom: 15px; }
    div[role="radiogroup"] { display: flex; flex-direction: row; justify-content: center; gap: 10px; width: 100%; }
    div[role="radiogroup"] label { background-color: #eaddcf; padding: 10px 20px; border-radius: 8px; border: 1px solid #d35400; cursor: pointer; font-weight: bold; color: #4a3b2a !important; }
    div[role="radiogroup"] label[data-checked="true"] { background-color: #d35400 !important; color: white !important; }
    .tile-teaser { font-size: 0.9em; color: #555; margin-top: 5px; font-style: italic; }
    .problem-book { font-size: 0.8em; color: #c0392b; margin-top: -10px; margin-bottom: 10px; }
    </style>
""", unsafe_allow_html=True)

# --- BACKEND ---
@st.cache_resource
def get_connection():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    if "gcp_service_account" in st.secrets:
        try:
            creds_dict = dict(st.secrets["gcp_service_account"])
            if "private_key" in creds_dict: creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            return gspread.authorize(creds)
        except Exception: return None
    return None

def setup_sheets(client):
    try: sh = client.open("Bücherliste") 
    except: st.error("Fehler: Tabelle 'Bücherliste' nicht gefunden."); st.stop()
    ws_books = sh.sheet1
    try: ws_logs = sh.worksheet("Logs")
    except: ws_logs = sh.add_worksheet(title="Logs", rows=1000, cols=3); ws_logs.append_row(["Zeitstempel", "Typ", "Nachricht"])
    try: ws_authors = sh.worksheet("Autoren")
    except: ws_authors = sh.add_worksheet(title="Autoren", rows=1000, cols=1); ws_authors.update_cell(1, 1, "Name")
    return sh, ws_books, ws_logs, ws_authors

def log_to_sheet(ws_logs, message, msg_type="INFO"):
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ws_logs.insert_row([ts, msg_type, str(message)], index=2)
    except Exception as e: st.error(f"LOGGING FEHLER: {e}")

def check_structure(ws):
    if "structure_checked" in st.session_state: return
    try:
        head = ws.row_values(1)
        if not head: ws.update_cell(1,1,"Titel"); head=["Titel"]
        needed = ["Titel", "Autor", "Genre", "Bewertung", "Cover", "Hinzugefügt", "Notiz", "Status", "Tags", "Erschienen", "Teaser", "Bio"]
        next_c = len(head)+1
        for n in needed:
            if not any(h.lower()==n.lower() for h in head):
                ws.update_cell(1, next_c, n); next_c+=1; time.sleep(0.5)
        st.session_state.structure_checked = True
    except: pass

# --- DATA ---
def get_data_fresh(ws):
    cols = ["Titel", "Autor", "Genre", "Bewertung", "Cover", "Hinzugefügt", "Notiz", "Status", "Tags", "Erschienen", "Teaser", "Bio"]
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
                d["Bewertung"] = int(raw_val) if str(raw_val).isdigit() else 0
            except: d["Bewertung"] = 0
            if not d["Status"]: d["Status"] = "Gelesen"
            if d["Titel"]: data.append(d)
        return pd.DataFrame(data)
    except: return pd.DataFrame(columns=cols)

def get_data(ws):
    if "df_books" not in st.session_state:
        with st.spinner("Lade Daten..."): st.session_state.df_books = get_data_fresh(ws)
    return st.session_state.df_books

def force_reload():
    if "df_books" in st.session_state: del st.session_state.df_books

# --- AUTOMATIC CLEANUP (THE MAGIC) ---
def auto_cleanup_authors(ws_books):
    """
    Läuft automatisch im Hintergrund.
    Sucht nach 'kurzen' Autorennamen und ersetzt sie durch 'lange' Versionen,
    wenn diese existieren (z.B. Novik -> Naomi Novik).
    """
    try:
        all_vals = ws_books.get_all_values()
        if len(all_vals) < 2: return
        
        headers = [str(h).lower() for h in all_vals[0]]
        idx_a = headers.index("autor")
        
        # 1. Mapping erstellen
        import unicodedata
        def clean(t): return unicodedata.normalize('NFKC', str(t)).strip()
        
        raw_authors = [clean(row[idx_a]) for row in all_vals[1:] if len(row) > idx_a and row[idx_a]]
        unique_authors = sorted(list(set(raw_authors)), key=len, reverse=True) # Längste zuerst
        
        replacements = {}
        for long in unique_authors:
            for short in unique_authors:
                if long == short: continue
                # Wenn kurz in lang enthalten ist UND lang deutlich länger ist (vermeidet falsche Matches)
                if short in long and len(long) > len(short) + 2:
                    if short not in replacements:
                        replacements[short] = long
        
        if not replacements: return # Nichts zu tun
        
        # 2. Anwenden
        updates = []
        for i, row in enumerate(all_vals):
            if i == 0: continue
            if len(row) > idx_a:
                current = clean(row[idx_a])
                if current in replacements:
                    # Batch Update vorbereiten oder direkt schreiben
                    ws_books.update_cell(i+1, idx_a+1, replacements[current])
                    time.sleep(0.2) # Sanft zur API
    except: pass # Silent fail im Background

def update_single_entry(ws, titel, field, value):
    try:
        cell = ws.find(titel)
        headers = [str(h).lower() for h in ws.row_values(1)]
        col = headers.index(field.lower()) + 1
        ws.update_cell(cell.row, col, value)
        
        # AUTO CLEANUP TRIGGER
        if field.lower() == "autor": auto_cleanup_authors(ws)
        
        force_reload()
        return True
    except: return False

def delete_book(ws, titel):
    try:
        cell = ws.find(titel)
        ws.delete_rows(cell.row)
        force_reload()
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
    
    # AUTO CLEANUP TRIGGER
    auto_cleanup_authors(ws)
    
    force_reload()
    return True

def filter_and_sort_books(df, query, sort_by):
    if query:
        q = query.lower()
        mask = (
            df['Titel'].str.lower().str.contains(q, na=False) |
            df['Autor'].str.lower().str.contains(q, na=False) |
            df['Tags'].str.lower().str.contains(q, na=False)
        )
        df = df[mask]
    if sort_by == "Autor (A-Z)":
        df['Lastname'] = df['Autor'].apply(lambda x: x.split(' ')[-1] if x and ' ' in x else x)
        df = df.sort_values(by='Lastname', key=lambda col: col.str.lower())
    elif sort_by == "Titel (A-Z)":
        df = df.sort_values(by='Titel', key=lambda col: col.str.lower())
    return df

# --- API HELPERS ---
def process_genre(raw):
    if not raw: return "Roman"
    try: return "Roman" if "römisch" in GoogleTranslator(source='auto', target='de').translate(raw).lower() else raw
    except: return "Roman"

def fetch_cover_candidates_loose(titel, autor, ws_logs=None):
    candidates = [] 
    try:
        query = f"{titel} {autor}"
        if ws_logs: log_to_sheet(ws_logs, f"Suche Cover: {query}", "DEBUG")
        url = f"https://www.googleapis.com/books/v1/volumes?q={urllib.parse.quote(query)}&maxResults=6&printType=books"
        r = requests.get(url).json()
        items = r.get("items", [])
        for item in items:
            info = item.get("volumeInfo", {})
            imgs = info.get("imageLinks", {})
            img_url = ""
            if "extraLarge" in imgs: img_url = imgs["extraLarge"]
            elif "large" in imgs: img_url = imgs["large"]
            elif "medium" in imgs: img_url = imgs["medium"]
            elif "thumbnail" in imgs: img_url = imgs["thumbnail"]
            if img_url:
                if img_url.startswith("http://"): img_url = img_url.replace("http://", "https://")
                if img_url not in candidates: candidates.append(img_url)
    except: pass
    try:
        r = requests.get(f"https://openlibrary.org/search.json?q={titel} {autor}&limit=3").json()
        if r["docs"]: 
            for doc in r["docs"]:
                if "cover_i" in doc:
                    url = f"https://covers.openlibrary.org/b/id/{doc['cover_i']}-L.jpg"
                    if url not in candidates: candidates.append(url)
    except: pass
    return candidates

def fetch_meta_single(titel, autor):
    cands = fetch_cover_candidates_loose(titel, autor)
    c = cands[0] if cands else "-"
    return c, "Roman", datetime.now().strftime("%Y") 

# --- AI CORE ---
@st.cache_data(show_spinner=False)
def get_available_models(api_key):
    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
    try:
        r = requests.get(url)
        if r.status_code == 200:
            data = r.json()
            models = [m['name'].replace("models/", "") for m in data.get('models', []) if 'generateContent' in m.get('supportedGenerationMethods', [])]
            models.sort(key=lambda x: "gemma" not in x)
            return models
        return []
    except: return []

def call_ai_manual(prompt, model_name):
    api_key = st.secrets["gemini_api_key"]
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
    headers = {'Content-Type': 'application/json'}
    data = {"contents": [{"parts": [{"text": prompt}]}]}
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            try:
                res = response.json()
                txt = res['candidates'][0]['content']['parts'][0]['text']
                match = re.search(r'\{[\s\S]*\}', txt)
                if match: return match.group(0), None
                return txt, None
            except: return None, "Parse Fehler"
        elif response.status_code == 429: return None, "RATE_LIMIT"
        else: return None, f"Fehler {response.status_code}"
    except Exception as e: return None, str(e)

def fetch_all_ai_data_manual(titel, autor, model_name):
    prompt = f"""
    Antworte NUR mit validem JSON.
    Buch: "{titel}" von {autor}.
    JSON Format:
    {{
      "tags": "3-5 kurze Tags auf Deutsch",
      "year": "Erscheinungsjahr (Zahl)",
      "teaser": "Spannender Teaser auf Deutsch (max 60 Worte)",
      "bio": "Kurze Autor Bio auf Deutsch (max 30 Worte)"
    }}
    """
    txt, err = call_ai_manual(prompt, model_name)
    
    # Fallback-Objekt, damit "Fehler" gespeichert wird und Buch nicht als "offen" bleibt
    fallback = {
        "tags": "-", "year": "", 
        "teaser": f"Keine automatischen Infos verfügbar. ({err})" if err else "Keine automatischen Infos verfügbar.", 
        "bio": "-"
    }
    
    if err: return fallback, err
    try: return json.loads(txt), None
    except: 
        # Wenn JSON kaputt ist, nehmen wir trotzdem Fallback, damit der Loop weitergeht
        return {
            "tags": "-", "year": "", 
            "teaser": "Keine automatischen Infos verfügbar (JSON Fehler).", 
            "bio": "-"
        }, "JSON Error"

def smart_author(short, known):
    s = short.strip().lower()
    for k in sorted(known, key=len, reverse=True):
        if s in str(k).lower(): return k
    return short

# --- UI DIALOGS ---
@st.dialog("🖼️ Cover auswählen")
def open_cover_gallery(book, ws_books, ws_logs):
    st.write(f"Suche Cover für **{book['Titel']}**...")
    if "gallery_images" not in st.session_state:
        with st.spinner("Suche..."):
            log_to_sheet(ws_logs, f"Manuelle Suche für: {book['Titel']}", "SEARCH")
            cands = fetch_cover_candidates_loose(book["Titel"], book["Autor"], ws_logs)
            st.session_state.gallery_images = cands
    if st.session_state.gallery_images:
        cols = st.columns(3)
        for i, img_url in enumerate(st.session_state.gallery_images):
            with cols[i % 3]:
                st.image(img_url, use_container_width=True)
                if st.button("Übernehmen", key=f"gal_btn_{i}"):
                    try:
                        cell = ws_books.find(book["Titel"])
                        headers = [str(h).lower() for h in ws_books.row_values(1)]
                        try: c_col = headers.index("cover") + 1
                        except: c_col = 5
                        ws_books.update_cell(cell.row, c_col, img_url)
                        log_to_sheet(ws_logs, f"Neues Cover gesetzt: {book['Titel']}", "UPDATE")
                        # AUTO CLEANUP
                        auto_cleanup_authors(ws_books)
                        force_reload()
                        del st.session_state.gallery_images
                        st.rerun()
                    except Exception as e: st.error(f"Fehler: {e}")
    else:
        st.warning("Nichts gefunden.")
        if st.button("Abbrechen"): st.rerun()

@st.dialog("📖 Buch-Details")
def show_book_details(book, ws_books, ws_authors, ws_logs):
    t1, t2 = st.tabs(["ℹ️ Info", "✏️ Bearbeiten"])
    with t1:
        st.markdown(f"### {book['Titel']}")
        st.markdown(f"**von {book['Autor']}**")
        c1, c2 = st.columns([1, 2])
        with c1:
            cov = book["Cover"] if book["Cover"] != "-" else "https://via.placeholder.com/200x300?text=No+Cover"
            st.markdown(f'<img src="{cov}" style="width:100%; border-radius:8px; box-shadow:0 2px 8px rgba(0,0,0,0.2);">', unsafe_allow_html=True)
            if book.get('Bewertung'): st.info(f"Bewertung: {'★' * int(book['Bewertung'])}")
            if "Tags" in book and book["Tags"]:
                st.write("")
                for t in book["Tags"].split(","): st.markdown(f'<span class="book-tag">{t.strip()}</span>', unsafe_allow_html=True)
        with c2:
            st.markdown(f"""<div class="ai-box"><b>📖 Teaser</b><br>{book.get('Teaser', '...')}</div>
            <div class="ai-box" style="border-left-color: #2980b9; background-color: #eaf2f8; margin-top:10px;">
                <b>👤 Autor</b><br>{book.get('Bio', '-')}</div>""", unsafe_allow_html=True)
    with t2:
        new_title = st.text_input("Titel", value=book["Titel"])
        new_author = st.text_input("Autor", value=book["Autor"])
        new_year = st.text_input("Jahr", value=book.get("Erschienen", ""))
        new_tags = st.text_input("Tags", value=book.get("Tags", ""))
        st.markdown("---")
        current_cover = book.get("Cover", "")
        new_cover_url = st.text_input("Cover URL", value=current_cover)
        if st.button("🔍 Cover online suchen (Galerie)"):
            with st.spinner("Suche..."):
                cands = fetch_cover_candidates_loose(book["Titel"], book["Autor"], ws_logs)
                if cands: st.session_state.inline_candidates = cands
                else: st.warning("Nichts gefunden.")
        if "inline_candidates" in st.session_state:
            cols = st.columns(3)
            for i, img_url in enumerate(st.session_state.inline_candidates):
                with cols[i % 3]:
                    st.image(img_url, use_container_width=True)
                    if st.button("Wählen", key=f"inl_{i}"):
                        st.session_state.selected_inline_cover = img_url
                        del st.session_state.inline_candidates
                        st.rerun()
        if "selected_inline_cover" in st.session_state:
            new_cover_url = st.session_state.selected_inline_cover
            st.success("Bild übernommen!")
        st.markdown("---")
        new_teaser = st.text_area("Teaser", value=book.get("Teaser", ""))
        new_bio = st.text_area("Bio", value=book.get("Bio", ""))
        if st.button("💾 Speichern", type="primary"):
            try:
                cell = ws_books.find(book["Titel"])
                headers = [str(h).lower() for h in ws_books.row_values(1)]
                final_cover = st.session_state.get("selected_inline_cover", new_cover_url)
                col_t = headers.index("titel") + 1
                col_a = headers.index("autor") + 1
                try: col_c = headers.index("cover") + 1
                except: col_c = 5
                try: col_tags = headers.index("tags") + 1
                except: col_tags = len(headers) + 1 
                try: col_y = headers.index("erschienen") + 1
                except: col_y = len(headers) + 2
                try: col_teaser = headers.index("teaser") + 1
                except: col_teaser = len(headers) + 3
                try: col_bio = headers.index("bio") + 1
                except: col_bio = len(headers) + 4
                
                ws_books.update_cell(cell.row, col_t, new_title)
                ws_books.update_cell(cell.row, col_a, new_author)
                ws_books.update_cell(cell.row, col_c, final_cover)
                ws_books.update_cell(cell.row, col_tags, new_tags)
                ws_books.update_cell(cell.row, col_y, new_year)
                ws_books.update_cell(cell.row, col_teaser, new_teaser)
                ws_books.update_cell(cell.row, col_bio, new_bio)
                
                # AUTO CLEANUP
                auto_cleanup_authors(ws_books)
                force_reload()
                
                if "selected_inline_cover" in st.session_state: del st.session_state.selected_inline_cover
                log_to_sheet(ws_logs, f"Update: {new_title}", "SAVE")
                st.success("Gespeichert!"); st.balloons(); time.sleep(1); st.rerun()
            except Exception as e: st.error(f"Fehler: {e}")
        st.markdown("---")
        if st.button("🗑️ Löschen"):
            if delete_book(ws_books, book["Titel"]):
                st.success("Gelöscht!"); time.sleep(1); st.rerun()

# --- MAIN ---
def main():
    st.title("📚 Meine Bibliothek")
    if "gallery_images" in st.session_state: del st.session_state.gallery_images
    
    client = get_connection()
    if not client: st.error("Secrets fehlen!"); st.stop()
    sh, ws_books, ws_logs, ws_authors = setup_sheets(client)
    check_structure(ws_books)
    df = get_data(ws_books)
    authors = list(set([a for i, row in df.iterrows() if row["Status"] != "Wunschliste" for a in [row["Autor"]] if a]))
    
    with st.sidebar:
        st.write("🔧 **Einstellungen**")
        st.markdown(f"🔗 [**📂 Tabelle öffnen**](https://docs.google.com/spreadsheets/d/{sh.id})")
        if st.button("🔄 Cache leeren"): force_reload(); st.rerun()
        if st.button("🛠️ Schreibtest"):
            try: ws_logs.update_cell(1, 3, "TEST_OK"); log_to_sheet(ws_logs, "Test", "DEBUG"); st.success("Erfolg!")
            except Exception as e: st.error(f"Fehler: {e}")
        
        st.markdown("---")
        if "available_models_list" not in st.session_state:
            with st.spinner("Lade Modelle..."):
                if "gemini_api_key" in st.secrets: st.session_state.available_models_list = get_available_models(st.secrets["gemini_api_key"])
                else: st.session_state.available_models_list = []
        models = st.session_state.available_models_list
        default_idx = 0
        for i, m in enumerate(models):
            if "gemma" in m: default_idx = i; break
        selected_model = st.selectbox("🧠 KI-Modell", models, index=default_idx if models else None)
        pause_time = 1.0 if (selected_model and "gemma" in selected_model) else 8.0
        
        st.markdown("---")
        st.write("🤖 **KI-Update**")
        missing_count = 0
        missing_indices = []
        if not df.empty:
            for i, r in df.iterrows():
                # Definiere "Offen" neu: Zu kurz ODER enthält Fehlermeldung
                teaser = str(r.get("Teaser", ""))
                is_error = "Fehler" in teaser or "Keine automatischen" in teaser or "Formatierungsfehler" in teaser
                if len(teaser) < 5 or is_error:
                    missing_count += 1
                    missing_indices.append(i)
        
        if missing_count > 0:
            st.info(f"{missing_count} Bücher offen.")
            if missing_count < 10:
                for idx in missing_indices: st.markdown(f"<div class='problem-book'>• {df.loc[idx]['Titel']}</div>", unsafe_allow_html=True)
            
            if st.button("✨ Infos laden"):
                if not selected_model: st.error("Kein Modell!"); st.stop()
                with st.status(f"Starte mit {selected_model}...", expanded=True) as status:
                    prog_bar = status.progress(0)
                    headers = [str(h).lower() for h in ws_books.row_values(1)]
                    try:
                        c_tag, c_year = headers.index("tags") + 1, headers.index("erschienen") + 1
                        c_teaser, c_bio = headers.index("teaser") + 1, headers.index("bio") + 1
                    except: st.error("Spaltenfehler"); st.stop()
                    
                    done = 0
                    for idx in missing_indices:
                        row = df.loc[idx]
                        status.write(f"Bearbeite: **{row['Titel']}**...")
                        log_to_sheet(ws_logs, f"Auto-Update: {row['Titel']}", "AI_JOB")
                        ai_data, err = fetch_all_ai_data_manual(row["Titel"], row["Autor"], selected_model)
                        
                        if err == "RATE_LIMIT":
                            status.write("⏳ Limit! Warte 60s...")
                            time.sleep(60)
                            ai_data, err = fetch_all_ai_data_manual(row["Titel"], row["Autor"], selected_model)
                        
                        if ai_data:
                            try:
                                cell = ws_books.find(row["Titel"])
                                if ai_data.get("tags") and ai_data["tags"] != "-": ws_books.update_cell(cell.row, c_tag, ai_data["tags"])
                                if ai_data.get("year"): ws_books.update_cell(cell.row, c_year, ai_data["year"])
                                # Teaser/Bio IMMER updaten, auch wenn es der Error-Text ist, damit Zähler sinkt
                                ws_books.update_cell(cell.row, c_teaser, ai_data.get("teaser", "-"))
                                if ai_data.get("bio"): ws_books.update_cell(cell.row, c_bio, ai_data.get("bio", "-"))
                                log_to_sheet(ws_logs, f"Gespeichert: {row['Titel']}", "SUCCESS")
                            except Exception as e: log_to_sheet(ws_logs, f"Error: {e}", "ERROR")
                        done += 1
                        prog_bar.progress(done / missing_count)
                        time.sleep(pause_time)
                    
                    # AUTO CLEANUP AM ENDE DES BATCHES
                    auto_cleanup_authors(ws_books)
                    force_reload()
                    status.update(label="Fertig!", state="complete", expanded=False)
                    time.sleep(1); st.rerun()
        else: st.success("Alles aktuell.")
            
        with st.expander("📜 System-Log", expanded=False):
            try:
                logs = ws_logs.get_all_values()
                if len(logs) > 1:
                    last_logs = logs[:10]
                    txt = ""
                    for l in last_logs: txt += f"{l[0]} | {l[2]}\n"
                    st.code(txt)
            except: st.write("Keine Logs")

    st.write("")
    nav = st.radio("Navigation", NAV_OPTIONS, 
                   horizontal=True, 
                   index=NAV_OPTIONS.index(st.session_state.active_tab),
                   label_visibility="collapsed",
                   key="nav_radio")
    
    if nav != st.session_state.active_tab:
        st.session_state.active_tab = nav
        st.rerun()

    if st.session_state.active_tab == "✍️ Neu":
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
                    with st.spinner("Speichere..."):
                        c, g, y = fetch_meta_single(t, fa)
                        ws_books.append_row([t, fa, g, val, c or "-", datetime.now().strftime("%Y-%m-%d"), note, "Gelesen", "", y or "", "", ""])
                        log_to_sheet(ws_logs, f"Neu: {t}", "NEW")
                        
                        # AUTO CLEANUP
                        auto_cleanup_authors(ws_books)
                        force_reload()
                    st.success(f"Gespeichert: {t}"); st.balloons(); time.sleep(1.0); st.rerun()
                else: st.error("Format: Titel, Autor")

    # --- SAMMLUNG / MERKLISTE HELPER ---
    def render_library_view(dataset, is_wishlist=False):
        c1, c2 = st.columns([2, 1])
        with c1:
            q = st.text_input("Suche (Titel, Autor, Tags)", placeholder="Suchen...", label_visibility="collapsed")
        with c2:
            sort_by = st.selectbox("Sortieren", ["Autor (A-Z)", "Titel (A-Z)"], label_visibility="collapsed")
        
        view_mode = st.radio("Ansicht", ["Kacheln", "Liste"], horizontal=True, label_visibility="collapsed", key=f"v_{is_wishlist}")
        
        df_filtered = filter_and_sort_books(dataset, q, sort_by)
        
        if df_filtered.empty:
            st.info("Keine Bücher gefunden.")
            return

        if view_mode == "Liste":
            cols_show = ["Titel", "Autor", "Notiz"]
            if not is_wishlist: cols_show.insert(2, "Bewertung")
            df_display = df_filtered[cols_show].copy()
            df_display.insert(0, "Info", False)
            edited = st.data_editor(df_display, column_config={"Info": st.column_config.CheckboxColumn("Info", width="small"), "Titel": st.column_config.TextColumn(disabled=True), "Autor": st.column_config.TextColumn(disabled=True), "Bewertung": st.column_config.NumberColumn("⭐", min_value=0, max_value=5)}, hide_index=True, use_container_width=True, key=f"ed_{is_wishlist}")
            if edited["Info"].any():
                sel_idx = edited[edited["Info"]].index[0]
                orig_title = df_display.iloc[sel_idx]["Titel"]
                orig_row = df[df["Titel"] == orig_title].iloc[0]
                show_book_details(orig_row, ws_books, ws_authors, ws_logs)

        else: # Kacheln
            cols = st.columns(3)
            for i, (idx, row) in enumerate(df_filtered.iterrows()):
                with cols[i % 3]:
                    with st.container(border=True):
                        c_img, c_content = st.columns([1, 2])
                        with c_img:
                            st.image(row["Cover"] if row["Cover"]!="-" else "https://via.placeholder.com/100", use_container_width=True)
                            b1, b2 = st.columns([4, 1])
                            if b1.button("ℹ️ Info", key=f"inf_{idx}_{is_wishlist}", use_container_width=True): 
                                show_book_details(row, ws_books, ws_authors, ws_logs)
                            if b2.button("🔄", key=f"upd_{idx}_{is_wishlist}", help="Cover"):
                                open_cover_gallery(row, ws_books, ws_logs)
                        with c_content:
                            st.write(f"**{row['Titel']}**")
                            st.caption(f"{row['Autor']}")
                            if not is_wishlist:
                                try: s_val = int(row['Bewertung'])
                                except: s_val = 0
                                if s_val > 0: st.markdown(f"<span style='color:#d35400'>{'★'*s_val}</span>", unsafe_allow_html=True)
                            
                            teaser_text = row.get("Teaser", "")
                            if teaser_text and len(str(teaser_text)) > 5:
                                short_teaser = str(teaser_text)[:200] + "..." if len(str(teaser_text)) > 200 else str(teaser_text)
                                st.markdown(f"<div class='tile-teaser'>{short_teaser}</div>", unsafe_allow_html=True)
                            else: st.caption("Noch kein Teaser.")

                            if is_wishlist:
                                if st.button("✅ Gelesen", key=f"read_{idx}"):
                                    cell = ws_books.find(row["Titel"])
                                    ws_books.update_cell(cell.row, 8, "Gelesen")
                                    ws_books.update_cell(cell.row, 6, datetime.now().strftime("%Y-%m-%d"))
                                    force_reload()
                                    st.rerun()

    if st.session_state.active_tab == "🔍 Sammlung":
        df_s = df[df["Status"] == "Gelesen"].copy()
        render_library_view(df_s, is_wishlist=False)

    elif st.session_state.active_tab == "🔮 Merkliste":
        with st.expander("➕ Neuer Wunsch"):
            with st.form("wish", clear_on_submit=True):
                iw = st.text_input("Titel, Autor")
                inote = st.text_input("Notiz")
                if st.form_submit_button("Hinzufügen"):
                    if "," in iw:
                        t, a = [x.strip() for x in iw.split(",", 1)]
                        fa = smart_author(a, authors)
                        c, g, y = fetch_meta_single(t, fa)
                        ws_books.append_row([t, fa, g, "", c or "-", datetime.now().strftime("%Y-%m-%d"), inote, "Wunschliste", "", y or "", "", ""])
                        
                        # AUTO CLEANUP
                        auto_cleanup_authors(ws_books)
                        force_reload()
                        log_to_sheet(ws_logs, f"Wunsch: {t}", "WISH"); st.success("Gemerkt!"); st.balloons(); time.sleep(1); st.rerun()
        df_w = df[df["Status"] == "Wunschliste"].copy()
        if not df_w.empty:
            render_library_view(df_w, is_wishlist=True)
        else: st.info("Leer.")

    elif st.session_state.active_tab == "👥 Statistik":
        st.header("📊 Statistik")
        df_r = df[df["Status"] == "Gelesen"]
        if not df_r.empty:
            c1, c2 = st.columns(2)
            c1.metric("Gelesen", len(df_r))
            c2.metric("Top Autor", df_r["Autor"].mode()[0] if not df_r.empty else "-")
            st.markdown("---")
            all_tags = []
            if "Tags" in df_r.columns:
                for t in df_r["Tags"].dropna():
                    tags = [x.strip() for x in str(t).split(",") if x.strip()]
                    all_tags.extend(tags)
            if all_tags:
                st.subheader("🏆 Top 3 Themen")
                tag_counts = pd.Series(all_tags).value_counts().head(3)
                c_top = st.columns(3)
                for i, (tag, count) in enumerate(tag_counts.items()):
                    c_top[i].metric(label=f"Platz {i+1}", value=tag, delta=f"{count} Bücher")
            st.markdown("---")
            with st.expander("👥 Alle Autoren (Datenbank)"):
                st.write(", ".join(sorted(authors)))

if __name__ == "__main__":
    main()
