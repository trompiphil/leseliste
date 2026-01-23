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
import threading
import wikipedia

# --- KONFIGURATION ---
st.set_page_config(page_title="Meine Leseliste", page_icon="📚", layout="wide")

# --- SPRACHE EINSTELLEN ---
try: wikipedia.set_lang("de")
except: pass

# --- STATE INIT ---
NAV_OPTIONS = ["✍️ Neu", "🔍 Sammlung", "🔮 Merkliste", "👥 Statistik"]
if "active_tab" not in st.session_state: st.session_state.active_tab = NAV_OPTIONS[1]
if st.session_state.active_tab not in NAV_OPTIONS: st.session_state.active_tab = NAV_OPTIONS[1]
if "background_status" not in st.session_state: st.session_state.background_status = "idle"
if "bg_message" not in st.session_state: st.session_state.bg_message = None

# --- CSS DESIGN ---
st.markdown("""
    <style>
    .stApp { background-color: #f5f5dc !important; }
    h1, h2, h3, h4, h5, h6, p, div, span, label, li, textarea, input, a { color: #2c3e50 !important; }
    .stTextInput input, .stTextArea textarea { background-color: #fffaf0 !important; border: 2px solid #d35400 !important; color: #000000 !important; }
    
    /* --- BUTTONS (MINI) --- */
    .stButton button {
        border-radius: 6px !important;
        border: 1px solid #d35400 !important;
        font-size: 0.8rem !important; 
        padding: 2px 8px !important;   
        min-height: 0px !important;
        height: auto !important;       
        line-height: 1.2 !important;
        margin-top: 5px !important;
        width: 100% !important;
    }
    
    .stButton button[kind="primary"] { background-color: #d35400 !important; color: white !important; font-weight: bold; }
    .stButton button[kind="secondary"] { background-color: transparent !important; color: #d35400 !important; opacity: 0.7; }

    /* SIDEBAR BUTTONS */
    [data-testid="stSidebar"] .stButton button {
        padding: 0.5rem 1rem !important;
        min-height: 2.5rem !important;
        margin-top: 0px !important;
    }

    /* --- KACHEL CONTAINER --- */
    [data-testid="stVerticalBlockBorderWrapper"] > div { 
        background-color: #eaddcf; 
        border-radius: 8px; 
        border: 1px solid #d35400; 
        box-shadow: 1px 1px 3px rgba(0,0,0,0.1); 
        padding: 8px;
    }
    
    /* Navigation */
    div[role="radiogroup"] { display: flex; flex-direction: row; justify-content: center; gap: 5px; width: 100%; flex-wrap: wrap; }
    div[role="radiogroup"] label { background-color: #eaddcf; padding: 5px 15px; border-radius: 8px; border: 1px solid #d35400; cursor: pointer; font-weight: bold; color: #4a3b2a !important; font-size: 0.9em; }
    div[role="radiogroup"] label[data-checked="true"] { background-color: #d35400 !important; color: white !important; }
    
    /* Text Styles */
    .tile-title { font-weight: bold; font-size: 1.0em; line-height: 1.2; margin-bottom: 2px; display: block; }
    .tile-meta { font-size: 0.85em; color: #555; margin-bottom: 4px; display: block; }
    .tile-teaser { 
        font-size: 0.8em; 
        color: #444; 
        margin-top: 4px; 
        margin-bottom: 6px;
        font-style: italic; 
        line-height: 1.3; 
        display: -webkit-box; 
        -webkit-line-clamp: 7; 
        -webkit-box-orient: vertical; 
        overflow: hidden; 
    }
    .year-badge { background-color: #fff8e1; padding: 1px 4px; border-radius: 3px; border: 1px solid #d35400; font-size: 0.75em; color: #d35400; font-weight: bold; margin-left: 5px; }
    .read-year-badge { background-color: #dcedc8; padding: 1px 4px; border-radius: 3px; border: 1px solid #7cb342; font-size: 0.75em; color: #558b2f; font-weight: bold; margin-left: 5px; }

    /* Dialog Boxen */
    .box-teaser { background-color: #fff8e1; border-left: 4px solid #d35400; padding: 10px; border-radius: 4px; margin-bottom: 10px; color: #2c3e50; }
    .box-author { background-color: #eaf2f8; border-left: 4px solid #2980b9; padding: 10px; border-radius: 4px; margin-top: 10px; color: #2c3e50; }

    /* --- MOBILE FORCE ROW (Layout Enforcer) --- */
    div[data-testid="stImage"] img {
        width: 80px !important;
        max-width: 80px !important;
        height: auto !important;
        object-fit: contain; 
    }

    [data-testid="stVerticalBlockBorderWrapper"] > div > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] {
        display: flex !important;
        flex-direction: row !important;
        flex-wrap: nowrap !important;
        align-items: start !important;
    }

    [data-testid="stVerticalBlockBorderWrapper"] > div > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(1) {
        flex: 0 0 80px !important;
        min-width: 80px !important;
        width: 80px !important;
        margin-right: 12px !important;
    }
    
    [data-testid="stVerticalBlockBorderWrapper"] > div > [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] > [data-testid="column"]:nth-child(2) {
        flex: 1 1 auto !important;
        min-width: 0 !important;
    }

    .status-running { color: #d35400; font-weight: bold; animation: pulse 2s infinite; }
    @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.5; } 100% { opacity: 1; } }
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
    if not client: return None, None, None, None
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
    except Exception: pass

def check_structure(ws):
    if "structure_checked" in st.session_state: return
    try:
        head = ws.row_values(1)
        if not head: ws.update_cell(1,1,"Titel"); head=["Titel"]
        # Update needed columns list to include 'Lesejahr'
        needed = ["Titel", "Autor", "Genre", "Bewertung", "Cover", "Hinzugefügt", "Notiz", "Status", "Tags", "Erschienen", "Teaser", "Bio", "Lesejahr"]
        
        # Check current columns and append missing ones
        current_cols_lower = [h.lower() for h in head]
        next_c = len(head) + 1
        
        for n in needed:
            if n.lower() not in current_cols_lower:
                ws.update_cell(1, next_c, n)
                next_c += 1
                time.sleep(0.5)
                
        st.session_state.structure_checked = True
    except: pass

# --- DATA ---
def get_data_fresh(ws):
    # Added "Lesejahr" to cols
    cols = ["Titel", "Autor", "Genre", "Bewertung", "Cover", "Hinzugefügt", "Notiz", "Status", "Tags", "Erschienen", "Teaser", "Bio", "Lesejahr"]
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

# --- AUTOMATIC CLEANUP & SYNC ---
def auto_cleanup_authors(ws_books, ws_authors):
    try:
        all_vals = ws_books.get_all_values()
        if len(all_vals) < 2: return
        headers = [str(h).lower() for h in all_vals[0]]
        idx_a = headers.index("autor")
        import unicodedata
        def clean(t): return unicodedata.normalize('NFKC', str(t)).strip()
        raw_authors = [clean(row[idx_a]) for row in all_vals[1:] if len(row) > idx_a and row[idx_a]]
        unique_authors_raw = sorted(list(set(raw_authors)), key=len, reverse=True)
        replacements = {}
        for long in unique_authors_raw:
            for short in unique_authors_raw:
                if long == short: continue
                if short in long and len(long) > len(short) + 2:
                    if short not in replacements: replacements[short] = long
        if replacements:
            for i, row in enumerate(all_vals):
                if i == 0: continue
                if len(row) > idx_a:
                    current = clean(row[idx_a])
                    if current in replacements:
                        ws_books.update_cell(i+1, idx_a+1, replacements[current])
                        time.sleep(0.2)
        updated_vals = ws_books.get_all_values()
        final_authors = sorted(list(set([clean(row[idx_a]) for row in updated_vals[1:] if len(row) > idx_a and row[idx_a]])))
        if ws_authors:
            try:
                ws_authors.clear()
                data_to_write = [["Name"]] + [[a] for a in final_authors]
                ws_authors.update(range_name="A1", values=data_to_write)
            except: pass
    except: pass

def delete_book(ws, titel, ws_authors):
    try:
        cell = ws.find(titel)
        ws.delete_rows(cell.row)
        auto_cleanup_authors(ws, ws_authors)
        force_reload()
        return True
    except: return False

def filter_and_sort_books(df_in, query, sort_by):
    df = df_in.copy()
    if query:
        q = query.lower()
        mask = (
            df['Titel'].str.lower().str.contains(q, na=False) |
            df['Autor'].str.lower().str.contains(q, na=False) |
            df['Tags'].str.lower().str.contains(q, na=False)
        )
        df = df[mask]
    
    if sort_by == "Autor (A-Z)":
        df['sort_key'] = df['Autor'].apply(lambda x: str(x).strip().split(' ')[-1] if x and str(x).strip() else "zzz")
        df = df.sort_values(by=['sort_key', 'Titel'], key=lambda col: col.str.lower())
    elif sort_by == "Titel (A-Z)":
        df = df.sort_values(by='Titel', key=lambda col: col.str.lower())
    elif sort_by == "Lesejahr (Neu -> Alt)":
        # Konvertiere Lesejahr zu Numerisch für Sortierung, leere Werte nach unten
        df['year_sort'] = pd.to_numeric(df['Lesejahr'], errors='coerce').fillna(0)
        df = df.sort_values(by=['year_sort', 'Titel'], ascending=[False, True])
    
    return df

# --- API HELPERS (TRIPLE ENGINE) ---
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

def get_wiki_info(titel, autor):
    try:
        search_query = f"{titel} {autor} buch roman"
        results = wikipedia.search(search_query)
        if not results: return ""
        page = wikipedia.page(results[0])
        return page.content[:3000] 
    except: return ""

def get_google_books_description(titel, autor):
    try:
        query = f"{titel} {autor}"
        url = f"https://www.googleapis.com/books/v1/volumes?q={urllib.parse.quote(query)}&maxResults=1"
        r = requests.get(url).json()
        if "items" in r:
            return r["items"][0]["volumeInfo"].get("description", "")
    except: return ""
    return ""

# --- AI CORE (Engine 3: Processing & Retry) ---
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
    
    max_retries = 3
    for attempt in range(max_retries):
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
            elif response.status_code == 503:
                time.sleep(3)
                continue
            elif response.status_code == 429:
                return None, "RATE_LIMIT"
            else:
                return None, f"Fehler {response.status_code}"
        except Exception as e: return None, str(e)
    
    return None, "Server Timeout (503)"

def fetch_all_ai_data_manual(titel, autor, model_name):
    wiki_text = get_wiki_info(titel, autor)
    google_text = get_google_books_description(titel, autor)
    
    context_str = ""
    if wiki_text: context_str += f"WIKIPEDIA TEXT:\n{wiki_text}\n\n"
    if google_text: context_str += f"GOOGLE BOOKS TEXT:\n{google_text}\n\n"
    
    prompt = f"""
    Antworte NUR mit validem JSON.
    Buch: "{titel}" von {autor}.
    
    Hintergrundwissen (nutze dies prioritär, falls vorhanden):
    {context_str}
    
    Aufgabe:
    1. Schreibe einen spannenden Teaser (max 60 Wörter). Nutze Wikipedia für Fakten, Google für Details.
    2. Schreibe eine Bio (max 40 Wörter).
    3. Ermittle das Jahr und Tags.
    
    JSON Format:
    {{
      "tags": "3-5 Tags (Deutsch)",
      "year": "Jahr (Zahl)",
      "teaser": "Teaser Text (Deutsch)",
      "bio": "Bio Text (Deutsch)"
    }}
    """
    
    txt, err = call_ai_manual(prompt, model_name)
    fallback = {"tags": "-", "year": "", "teaser": f"Keine Infos ({err})" if err else "Keine Infos.", "bio": "-"}
    if err: return fallback, err
    try: return json.loads(txt), None
    except: return {"tags": "-", "year": "", "teaser": "JSON Fehler.", "bio": "-"}, "JSON Error"

def smart_author(short, known):
    s = short.strip().lower()
    for k in sorted(known, key=len, reverse=True):
        if s in str(k).lower(): return k
    return short

# --- BACKGROUND WORKER ---
def background_update_task(missing_indices, df_copy, model_name, ws_books, ws_logs, ws_authors):
    log_to_sheet(ws_logs, "🚀 Hintergrund-Update gestartet", "START")
    headers = [str(h).lower() for h in ws_books.row_values(1)]
    try:
        c_tag = headers.index("tags") + 1
        c_year = headers.index("erschienen") + 1
        c_teaser = headers.index("teaser") + 1
        c_bio = headers.index("bio") + 1
    except: 
        log_to_sheet(ws_logs, "Spaltenfehler im Background Worker", "ERROR")
        return

    for idx in missing_indices:
        try:
            row = df_copy.loc[idx]
            ai_data, err = fetch_all_ai_data_manual(row["Titel"], row["Autor"], model_name)
            if err == "RATE_LIMIT":
                time.sleep(60)
                ai_data, err = fetch_all_ai_data_manual(row["Titel"], row["Autor"], model_name)
            if ai_data:
                cell = ws_books.find(row["Titel"])
                if ai_data.get("tags") and ai_data["tags"] != "-": ws_books.update_cell(cell.row, c_tag, ai_data["tags"])
                if ai_data.get("year"): ws_books.update_cell(cell.row, c_year, ai_data["year"])
                ws_books.update_cell(cell.row, c_teaser, ai_data.get("teaser", "-"))
                if ai_data.get("bio") and ai_data["bio"] != "-": ws_books.update_cell(cell.row, c_bio, ai_data.get("bio", "-"))
                log_to_sheet(ws_logs, f"Background: {row['Titel']} fertig", "SUCCESS")
            time.sleep(1.0)
        except Exception as e: log_to_sheet(ws_logs, f"Error bei {row['Titel']}: {e}", "ERROR")
    auto_cleanup_authors(ws_books, ws_authors)
    log_to_sheet(ws_logs, "✅ Hintergrund-Update beendet", "DONE")

# --- UI DIALOGS ---
@st.dialog("🖼️ Cover auswählen")
def open_cover_gallery(book, ws_books, ws_logs, ws_authors):
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
                        auto_cleanup_authors(ws_books, ws_authors)
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
            
            # --- LESEJAHR DISPLAY ---
            if book.get("Lesejahr"):
                st.markdown(f"📅 **Gelesen:** {book['Lesejahr']}")
                
            if "Tags" in book and book["Tags"]:
                st.write("")
                for t in book["Tags"].split(","): st.markdown(f'<span class="book-tag">{t.strip()}</span>', unsafe_allow_html=True)
        with c2:
            st.markdown(f"""
            <div class="box-teaser">
                <b>📖 Teaser</b><br>{book.get('Teaser', '...')}
            </div>
            <div class="box-author">
                <b>👤 Autor</b><br>{book.get('Bio', '-')}
            </div>
            """, unsafe_allow_html=True)
            
    with t2:
        st.write("📝 **Daten bearbeiten**")
        new_title = st.text_input("Titel", value=book["Titel"])
        new_author = st.text_input("Autor", value=book["Autor"])
        
        c_meta1, c_meta2 = st.columns(2)
        with c_meta1: new_year = st.text_input("Erscheinungsjahr", value=book.get("Erschienen", ""))
        with c_meta2: new_read_year = st.text_input("Gelesen im Jahr", value=book.get("Lesejahr", ""))
        
        new_tags = st.text_input("Tags", value=book.get("Tags", ""))
        
        st.markdown("---")
        st.write("🖼️ **Cover ändern**")
        
        # INLINE COVER SEARCH
        if "gallery_images" not in st.session_state:
            if st.button("🔍 Galerie laden"):
                with st.spinner("Suche..."):
                    cands = fetch_cover_candidates_loose(book["Titel"], book["Autor"], ws_logs)
                    st.session_state.gallery_images = cands
        
        selected_new_cover = None
        if "gallery_images" in st.session_state and st.session_state.gallery_images:
            cols = st.columns(3)
            for i, img_url in enumerate(st.session_state.gallery_images):
                with cols[i % 3]:
                    st.image(img_url, use_container_width=True)
                    if st.button("Wählen", key=f"gal_{i}"):
                        st.session_state.temp_cover = img_url
                        st.success("Ausgewählt!")
        
        # Manuelle URL
        current_cover = st.session_state.get("temp_cover", book.get("Cover", ""))
        new_cover_url = st.text_input("Cover URL", value=current_cover)

        st.markdown("---")
        st.write("✨ **KI-Aktionen**")
        if st.button("🪄 Infos neu generieren (Triple Engine)", type="primary"):
            with st.spinner("Recherchiere (Wiki + Google + KI)..."):
                mod_name = st.session_state.get("selected_model_name", "gemma-3-27b-it")
                ai_data, err = fetch_all_ai_data_manual(new_title, new_author, mod_name)
                if ai_data:
                    st.session_state.temp_ai_data = ai_data
                    st.success("Generiert! Bitte unten speichern.")
                else: st.error(f"Fehler: {err}")

        # SPEICHERN
        st.markdown("---")
        if st.button("💾 Alle Änderungen speichern", type="primary"):
            try:
                cell = ws_books.find(book["Titel"])
                headers = [str(h).lower() for h in ws_books.row_values(1)]
                
                # Mapping
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
                try: col_read_year = headers.index("lesejahr") + 1
                except: col_read_year = len(headers) + 5
                
                # Check for AI updates
                final_teaser = book.get("Teaser", "")
                final_bio = book.get("Bio", "")
                if "temp_ai_data" in st.session_state:
                    ai = st.session_state.temp_ai_data
                    final_teaser = ai.get("teaser", final_teaser)
                    final_bio = ai.get("bio", final_bio)
                    if ai.get("year"): new_year = ai["year"]
                    if ai.get("tags"): new_tags = ai["tags"]
                
                ws_books.update_cell(cell.row, col_t, new_title)
                ws_books.update_cell(cell.row, col_a, new_author)
                ws_books.update_cell(cell.row, col_c, new_cover_url)
                ws_books.update_cell(cell.row, col_tags, new_tags)
                ws_books.update_cell(cell.row, col_y, new_year)
                ws_books.update_cell(cell.row, col_teaser, final_teaser)
                ws_books.update_cell(cell.row, col_bio, final_bio)
                ws_books.update_cell(cell.row, col_read_year, new_read_year)
                
                auto_cleanup_authors(ws_books, ws_authors)
                force_reload()
                
                # Cleanup Session
                if "gallery_images" in st.session_state: del st.session_state.gallery_images
                if "temp_cover" in st.session_state: del st.session_state.temp_cover
                if "temp_ai_data" in st.session_state: del st.session_state.temp_ai_data
                
                st.success("Gespeichert!")
                time.sleep(1)
                st.rerun()
            except Exception as e: st.error(f"Fehler: {e}")
            
        if st.button("🗑️ Buch löschen"):
            if delete_book(ws_books, book["Titel"], ws_authors):
                st.success("Gelöscht!"); time.sleep(1); st.rerun()


# --- MAIN ---
def main():
    st.title("Meine Leseliste")
    
    # Cleanup old session states on load
    if "gallery_images" in st.session_state: del st.session_state.gallery_images
    
    client = get_connection()
    if not client: st.error("Secrets fehlen!"); st.stop()
    
    # SETUP SHEETS WITH ROBUST UNPACKING
    sheets_res = setup_sheets(client)
    if not sheets_res or not sheets_res[0]:
        st.error("Fehler bei der Verbindung zu Google Sheets.")
        st.stop()
        
    sh, ws_books, ws_logs, ws_authors = sheets_res
    check_structure(ws_books)
    df = get_data(ws_books)
    authors = list(set([a for i, row in df.iterrows() if row["Status"] != "Wunschliste" for a in [row["Autor"]] if a]))
    
    with st.sidebar:
        st.write("🔧 **Einstellungen**")
        
        # 1. KI UPDATE (GANZ OBEN)
        missing_count = 0
        missing_indices = []
        if not df.empty:
            for i, r in df.iterrows():
                teaser = str(r.get("Teaser", ""))
                is_error = "Fehler" in teaser or "Keine automatischen" in teaser or "Formatierungsfehler" in teaser
                if len(teaser) < 5 or is_error:
                    missing_count += 1
                    missing_indices.append(i)
                    
        st.write("🤖 **KI-Update**")
        if missing_count > 0:
            st.warning(f"{missing_count} Bücher offen.")
            if st.button("✨ Infos laden", type="primary", use_container_width=True):
                # Background Worker
                if not 'selected_model_name' in st.session_state: st.session_state.selected_model_name = "gemma-3-27b-it" # Fallback
                t = threading.Thread(target=background_update_task, args=(missing_indices, df.copy(), st.session_state.selected_model_name, ws_books, ws_logs, ws_authors), name="BackgroundUpdater")
                t.start()
                st.session_state.background_status = "running"
                st.toast("Hintergrund-Update gestartet!")
                time.sleep(0.5)
                st.rerun()
        else:
            st.success("Alles aktuell.")

        # STATUS CHECK FOR BACKGROUND WORKER
        if st.session_state.background_status == "running":
            st.markdown("<div class='status-running'>🔄 Hintergrund-Update läuft...</div>", unsafe_allow_html=True)
            is_running = any(t.name == "BackgroundUpdater" for t in threading.enumerate())
            if not is_running:
                st.session_state.background_status = "idle"
                st.session_state.bg_message = "✅ Laden abgeschlossen!"
                force_reload()
                st.rerun()
        
        if st.session_state.bg_message:
            st.toast(st.session_state.bg_message)
            st.session_state.bg_message = None

        st.markdown("---")
        
        # 2. VERWALTUNG (OPTISCH ANGEGLICHEN)
        st.write("⚙️ **Verwaltung**")
        st.link_button("📂 Tabelle öffnen", f"https://docs.google.com/spreadsheets/d/{sh.id}", use_container_width=True)
        st.button("🔄 Cache leeren", use_container_width=True, on_click=lambda: (force_reload(), st.rerun()))
        if st.button("🛠️ Schreibtest", use_container_width=True):
            try: ws_logs.update_cell(1, 3, "TEST_OK"); log_to_sheet(ws_logs, "Test", "DEBUG"); st.success("Erfolg!")
            except Exception as e: st.error(f"Fehler: {e}")
            
        st.markdown("---")
        
        # 3. MODELL (GANZ UNTEN)
        if "available_models_list" not in st.session_state:
            with st.spinner("Lade Modelle..."):
                if "gemini_api_key" in st.secrets: st.session_state.available_models_list = get_available_models(st.secrets["gemini_api_key"])
                else: st.session_state.available_models_list = []
        models = st.session_state.available_models_list
        default_idx = 0
        search_prio = ["gemma-3-27b", "gemma-3"] 
        found = False
        for prio in search_prio:
            for i, m in enumerate(models):
                if prio in m: default_idx = i; found = True; break
            if found: break
        selected_model = st.selectbox("🧠 KI-Modell", models, index=default_idx if models else None)
        st.session_state.selected_model_name = selected_model
        
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
                read_year = st.text_input("Gelesen im Jahr (optional)")
                rate = st.feedback("stars")
            if st.form_submit_button("Speichern"):
                if "," in inp:
                    val = (rate + 1) if rate is not None else 0
                    t, a = [x.strip() for x in inp.split(",", 1)]
                    fa = smart_author(a, authors)
                    
                    # Lesejahr Logic: Wenn leer -> Aktuelles Jahr
                    final_read_year = read_year.strip() if read_year else str(datetime.now().year)
                    
                    with st.spinner("Speichere..."):
                        c, g, y = fetch_meta_single(t, fa)
                        # Append Row needs exact column count based on check_structure logic
                        # Structure: Titel, Autor, Genre, Bewertung, Cover, Hinzugefügt, Notiz, Status, Tags, Erschienen, Teaser, Bio, Lesejahr
                        ws_books.append_row([t, fa, g, val, c or "-", datetime.now().strftime("%Y-%m-%d"), note, "Gelesen", "", y or "", "", "", final_read_year])
                        log_to_sheet(ws_logs, f"Neu: {t}", "NEW")
                        auto_cleanup_authors(ws_books, ws_authors)
                        force_reload()
                    st.success(f"Gespeichert: {t}"); st.balloons(); time.sleep(1.0); st.rerun()
                else: st.error("Format: Titel, Autor")

    # --- RENDER FUNKTION ---
    def render_library_view(dataset, is_wishlist=False):
        c1, c2 = st.columns([2, 1])
        with c1: q = st.text_input("Suche (Titel, Autor, Tags)", placeholder="Suchen...", label_visibility="collapsed")
        with c2: sort_by = st.selectbox("Sortieren", ["Autor (A-Z)", "Titel (A-Z)", "Lesejahr (Neu -> Alt)"], label_visibility="collapsed")
        view_mode = st.radio("Ansicht", ["Kacheln", "Liste"], horizontal=True, label_visibility="collapsed", key=f"v_{is_wishlist}")
        
        df_filtered = filter_and_sort_books(dataset, q, sort_by)
        if df_filtered.empty:
            st.info("Keine Bücher gefunden.")
            return

        if view_mode == "Liste":
            # Liste Anzeige anpassen
            cols_show = ["Titel", "Autor", "Notiz", "Lesejahr"]
            if not is_wishlist: cols_show.insert(2, "Bewertung")
            
            # Ensure columns exist before selecting
            cols_show = [c for c in cols_show if c in df_filtered.columns]
            
            df_display = df_filtered[cols_show].copy()
            df_display.insert(0, "Info", False)
            edited = st.data_editor(df_display, column_config={
                "Info": st.column_config.CheckboxColumn("Info", width="small"),
                "Titel": st.column_config.TextColumn(disabled=True),
                "Autor": st.column_config.TextColumn(disabled=True),
                "Bewertung": st.column_config.NumberColumn("⭐", min_value=0, max_value=5),
                "Lesejahr": st.column_config.TextColumn("Jahr")
            }, hide_index=True, use_container_width=True, key=f"ed_{is_wishlist}")
            
            if edited["Info"].any():
                sel_idx = edited[edited["Info"]].index[0]
                orig_title = df_display.iloc[sel_idx]["Titel"]
                orig_row = df[df["Titel"] == orig_title].iloc[0]
                show_book_details(orig_row, ws_books, ws_authors, ws_logs)
        else:
            # --- ROW CHUNKING FIX (Sortierung auch Mobil korrekt) ---
            for i in range(0, len(df_filtered), 3):
                batch = df_filtered.iloc[i:i+3]
                cols = st.columns(3)
                for j, (idx, row) in enumerate(batch.iterrows()):
                    with cols[j]:
                        with st.container(border=True):
                            c_img, c_content = st.columns([1, 2])
                            with c_img:
                                st.image(row["Cover"] if row["Cover"]!="-" else "https://via.placeholder.com/100", use_container_width=True)
                            with c_content:
                                st.markdown(f"<span class='tile-title'>{row['Titel']}</span>", unsafe_allow_html=True)
                                year_disp = f"<span class='year-badge'>{row.get('Erschienen')}</span>" if row.get("Erschienen") else ""
                                st.markdown(f"<span class='tile-meta'>{row['Autor']}{year_disp}</span>", unsafe_allow_html=True)
                                
                                if not is_wishlist:
                                    try: s_val = int(row['Bewertung'])
                                    except: s_val = 0
                                    
                                    stars_html = f"<span style='color:#d35400'>{'★'*s_val}</span>" if s_val > 0 else ""
                                    # Lesejahr anzeigen wenn vorhanden
                                    read_year_html = ""
                                    if row.get("Lesejahr"):
                                        read_year_html = f"<span class='read-year-badge'>'{str(row['Lesejahr'])[-2:]}</span>"
                                    
                                    st.markdown(f"{stars_html}{read_year_html}", unsafe_allow_html=True)
                                
                                teaser_text = row.get("Teaser", "")
                                if teaser_text and len(str(teaser_text)) > 5:
                                    st.markdown(f"<div class='tile-teaser'>{teaser_text}</div>", unsafe_allow_html=True)
                                else: st.caption("Noch kein Teaser.")
                                
                                if st.button("ℹ️ Details", key=f"inf_{idx}_{is_wishlist}", type="primary"): 
                                    show_book_details(row, ws_books, ws_authors, ws_logs)
                                
                                if is_wishlist:
                                    if st.button("✅ Gelesen", key=f"read_{idx}", use_container_width=True):
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
                        # Append Row needs correct length - Lesejahr is empty for wishlist usually or default? Let's leave empty
                        ws_books.append_row([t, fa, g, "", c or "-", datetime.now().strftime("%Y-%m-%d"), inote, "Wunschliste", "", y or "", "", "", ""])
                        auto_cleanup_authors(ws_books, ws_authors)
                        force_reload()
                        log_to_sheet(ws_logs, f"Wunsch: {t}", "WISH"); st.success("Gemerkt!"); st.balloons(); time.sleep(1); st.rerun()
        df_w = df[df["Status"] == "Wunschliste"].copy()
        if not df_w.empty:
            render_library_view(df_w, is_wishlist=True)
        else: st.info("Leer.")

    elif st.session_state.active_tab == "👥 Statistik":
        st.header("📊 Statistik")
        df_r = df[df["Status"] == "Gelesen"]
        c1, c2 = st.columns(2)
        c1.metric("Gelesen", len(df_r))
        top_author_name = "-"
        top_author_count = 0
        if not df_r.empty:
            top_author_name = df_r["Autor"].mode()[0]
            top_author_count = len(df_r[df_r["Autor"] == top_author_name])
        c2.metric("Top Autor", top_author_name, f"{top_author_count} Bücher" if top_author_count > 0 else None)
        st.markdown("---")
        
        # Lesejahre Statistik (NEU)
        if "Lesejahr" in df_r.columns:
            st.subheader("📅 Bücher pro Jahr")
            try:
                # Filtere leere Jahre raus und zähle
                year_counts = df_r["Lesejahr"].value_counts().reset_index()
                year_counts.columns = ["Jahr", "Anzahl"]
                year_counts = year_counts[year_counts["Jahr"] != ""]
                year_counts = year_counts.sort_values("Jahr", ascending=False)
                st.dataframe(year_counts, use_container_width=True, hide_index=True)
            except: st.write("Noch keine Daten.")
            
        st.markdown("---")
        all_tags = []
        if not df_r.empty and "Tags" in df_r.columns:
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
        st.subheader("📚 Alle Autoren (Gelesen)")
        if not df_r.empty:
            auth_stats = df_r["Autor"].value_counts().reset_index()
            auth_stats.columns = ["Autor", "Anzahl"]
            auth_stats = auth_stats.sort_values(by=["Anzahl", "Autor"], ascending=[False, True])
            st.dataframe(auth_stats, use_container_width=True, hide_index=True, column_config={"Autor": st.column_config.TextColumn("Autor"), "Anzahl": st.column_config.ProgressColumn("Gelesen", format="%d", min_value=0, max_value=int(auth_stats["Anzahl"].max()))})

if __name__ == "__main__":
    main()
