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
if "active_tab" not in st.session_state:
    st.session_state.active_tab = "Sammlung" 

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
    .book-tag { display: inline-block; background-color: #d35400; color: white !important; padding: 2px 8px; border-radius: 12px; font-size: 0.75em; margin-right: 5px; margin-bottom: 5px; font-weight: bold; }
    .log-box { font-family: monospace; font-size: 0.8em; background-color: #333; color: #0f0; padding: 10px; border-radius: 5px; max-height: 200px; overflow-y: scroll; }
    
    div[role="radiogroup"] { display: flex; flex-direction: row; justify-content: center; gap: 10px; width: 100%; }
    div[role="radiogroup"] label { background-color: #eaddcf; padding: 10px 20px; border-radius: 8px; border: 1px solid #d35400; cursor: pointer; font-weight: bold; color: #4a3b2a !important; }
    div[role="radiogroup"] label[data-checked="true"] { background-color: #d35400 !important; color: white !important; }
    div[role="radiogroup"] label p { font-size: 1.1em; }
    </style>
""", unsafe_allow_html=True)

# --- BACKEND & LOGGING ---
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
    
    # Log Sheet erzwingen
    try: ws_logs = sh.worksheet("Logs")
    except: 
        ws_logs = sh.add_worksheet(title="Logs", rows=1000, cols=3)
        ws_logs.append_row(["Zeitstempel", "Typ", "Nachricht"])
        
    try: ws_authors = sh.worksheet("Autoren")
    except: ws_authors = sh.add_worksheet(title="Autoren", rows=1000, cols=1); ws_authors.update_cell(1, 1, "Name")
    return ws_books, ws_logs, ws_authors

def log_event(ws_logs, message, msg_type="INFO"):
    """Schreibt Logs und zeigt Fehler an, falls es nicht klappt"""
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ws_logs.append_row([ts, msg_type, str(message)])
    except Exception as e:
        # Falls Logging fehlschlägt, geben wir es in der App aus (damit wir es merken)
        print(f"LOGGING FAILED: {e}")

def check_structure(ws):
    try:
        head = ws.row_values(1)
        if not head: ws.update_cell(1,1,"Titel"); head=["Titel"]
        needed = ["Titel", "Autor", "Genre", "Bewertung", "Cover", "Hinzugefügt", "Notiz", "Status", "Tags", "Erschienen", "Teaser", "Bio"]
        next_c = len(head)+1
        for n in needed:
            if not any(h.lower()==n.lower() for h in head):
                ws.update_cell(1, next_c, n); next_c+=1; time.sleep(0.5)
    except: pass

def get_data(ws):
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

# --- API HELPERS (COVER SEARCH 2.0) ---
def process_genre(raw):
    if not raw: return "Roman"
    try: t = GoogleTranslator(source='auto', target='de').translate(raw); return "Roman" if "römisch" in t.lower() else t
    except: return "Roman"

def fetch_cover_candidates(titel, autor, ws_logs=None):
    """
    Sucht nach Covern. Strategie:
    1. Strenge Suche (intitle:x inauthor:y)
    2. Wenn leer: Lockere Suche (x y) und filtern
    """
    candidates = [] 
    
    # 1. Google Books API
    try:
        # STRATEGIE A: Streng
        query = f"intitle:{titel} inauthor:{autor}"
        url = f"https://www.googleapis.com/books/v1/volumes?q={urllib.parse.quote(query)}&maxResults=5&printType=books"
        r = requests.get(url).json()
        
        items = r.get("items", [])
        
        # STRATEGIE B: Locker (Fallback, wenn A leer)
        if not items:
            if ws_logs: log_event(ws_logs, f"Strenge Suche leer für {titel}, versuche locker...", "INFO")
            query_loose = f"{titel} {autor}"
            url_loose = f"https://www.googleapis.com/books/v1/volumes?q={urllib.parse.quote(query_loose)}&maxResults=5&printType=books"
            r_loose = requests.get(url_loose).json()
            items = r_loose.get("items", [])

        for item in items:
            info = item.get("volumeInfo", {})
            imgs = info.get("imageLinks", {})
            
            # Bild URL holen (größtes zuerst)
            img_url = ""
            if "extraLarge" in imgs: img_url = imgs["extraLarge"]
            elif "large" in imgs: img_url = imgs["large"]
            elif "medium" in imgs: img_url = imgs["medium"]
            elif "thumbnail" in imgs: img_url = imgs["thumbnail"]
            
            if img_url:
                if img_url.startswith("http://"): img_url = img_url.replace("http://", "https://")
                if img_url not in candidates: candidates.append(img_url)
                    
    except Exception as e:
        if ws_logs: log_event(ws_logs, f"Google Cover Error: {e}", "WARN")

    # 2. OpenLibrary Fallback
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
    cands = fetch_cover_candidates(titel, autor)
    c = cands[0] if cands else "-"
    return c, "Roman", datetime.now().strftime("%Y") 

# --- AI CORE ---
def clean_json_string(text):
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match: return match.group(0)
        return text
    except: return text

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
                return clean_json_string(txt), None
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
    if err: return None, err
    try: return json.loads(txt), None
    except: return None, "JSON Fehler"

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

def smart_author(short, known):
    s = short.strip().lower()
    for k in sorted(known, key=len, reverse=True):
        if s in str(k).lower(): return k
    return short

# --- DIALOG & UI ---
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
            if book.get('Bewertung'):
                st.info(f"Bewertung: {'★' * int(book['Bewertung'])}")
            if "Tags" in book and book["Tags"]:
                st.write("")
                tags_list = book["Tags"].split(",")
                tag_html = ""
                for t in tags_list: tag_html += f'<span class="book-tag">{t.strip()}</span>'
                st.markdown(tag_html, unsafe_allow_html=True)
        with c2:
            st.markdown(f"""
            <div class="ai-box"><b>📖 Worum geht's?</b><br>{book.get('Teaser', '...')}</div>
            <div class="ai-box" style="border-left-color: #2980b9; background-color: #eaf2f8; margin-top:10px;">
                <b>👤 Autor</b><br>{book.get('Bio', '-')}</div>
            """, unsafe_allow_html=True)
            st.markdown(f"[🔍 Google](https://www.google.com/search?q={urllib.parse.quote(book['Titel'] + ' ' + book['Autor'])})")

    with t2:
        st.write("Daten bearbeiten:")
        new_title = st.text_input("Titel", value=book["Titel"])
        new_author = st.text_input("Autor", value=book["Autor"])
        new_year = st.text_input("Erscheinungsjahr", value=book.get("Erschienen", ""))
        new_tags = st.text_input("Tags", value=book.get("Tags", ""))
        
        st.markdown("---")
        st.write("**Cover ändern**")
        current_cover = book.get("Cover", "")
        new_cover_url = st.text_input("Cover URL (manuell)", value=current_cover)
        
        if st.button("🔍 Cover online suchen (Galerie öffnen)"):
            with st.spinner("Suche passende Bilder..."):
                log_event(ws_logs, f"Suche Cover für: {book['Titel']}", "SEARCH")
                cands = fetch_cover_candidates(book["Titel"], book["Autor"], ws_logs)
                if cands:
                    st.session_state.cover_candidates = cands
                    log_event(ws_logs, f"Gefunden: {len(cands)} Bilder", "SUCCESS")
                else:
                    st.warning("Keine Bilder gefunden.")
                    log_event(ws_logs, "Keine Bilder gefunden", "WARN")
        
        if "cover_candidates" in st.session_state and st.session_state.cover_candidates:
            st.write("Wähle ein Bild:")
            cols = st.columns(3)
            for i, img_url in enumerate(st.session_state.cover_candidates):
                with cols[i % 3]:
                    st.image(img_url, use_container_width=True)
                    if st.button("Übernehmen", key=f"sel_cov_{i}"):
                        new_cover_url = img_url
                        st.session_state.selected_new_cover = img_url
                        del st.session_state.cover_candidates
                        st.success("Ausgewählt!")
                        
        st.markdown("---")
        new_teaser = st.text_area("Teaser", value=book.get("Teaser", ""))
        new_bio = st.text_area("Bio", value=book.get("Bio", ""))
        
        if st.button("💾 Speichern & Schließen", type="primary"):
            try:
                cell = ws_books.find(book["Titel"])
                headers = [str(h).lower() for h in ws_books.row_values(1)]
                final_cover = st.session_state.get("selected_new_cover", new_cover_url)
                
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
                
                cleanup_author_duplicates_batch(ws_books, ws_authors)
                del st.session_state.df_books
                if "cover_candidates" in st.session_state: del st.session_state.cover_candidates
                if "selected_new_cover" in st.session_state: del st.session_state.selected_new_cover
                
                log_event(ws_logs, f"Buch gespeichert: {new_title}", "SAVE")
                st.success("Gespeichert!"); st.balloons(); time.sleep(1); st.rerun()
            except Exception as e:
                st.error(f"Fehler: {e}")
                log_event(ws_logs, f"Save Error: {e}", "ERROR")

        st.markdown("---")
        if st.button("🗑️ Löschen"):
            if delete_book(ws_books, book["Titel"]):
                del st.session_state.df_books
                st.success("Gelöscht!"); time.sleep(1); st.rerun()

# --- MAIN ---
def main():
    st.title("📚 Meine Bibliothek")
    
    client = get_connection()
    if not client: st.error("Secrets fehlen!"); st.stop()
    ws_books, ws_logs, ws_authors = setup_sheets(client)
    
    if "checked" not in st.session_state: check_structure(ws_books); st.session_state.checked=True
    if "df_books" not in st.session_state: 
        with st.spinner("Lade Daten..."): st.session_state.df_books = get_data(ws_books)
    
    if "df_books" in st.session_state:
        cols = st.session_state.df_books.columns
        if "Teaser" not in cols or "Bio" not in cols:
            st.session_state.df_books = get_data(ws_books)
            st.rerun()

    df = st.session_state.df_books
    authors = list(set([a for i, row in df.iterrows() if row["Status"] != "Wunschliste" for a in [row["Autor"]] if a]))
    
    with st.sidebar:
        st.write("🔧 **Einstellungen**")
        if st.button("🔄 Cache leeren"): 
            st.session_state.clear(); st.rerun()
        
        st.markdown("---")
        if "available_models_list" not in st.session_state:
            with st.spinner("Lade Modelle..."):
                if "gemini_api_key" in st.secrets:
                    st.session_state.available_models_list = get_available_models(st.secrets["gemini_api_key"])
                else: st.session_state.available_models_list = []
        
        models = st.session_state.available_models_list
        default_idx = 0
        for i, m in enumerate(models):
            if "gemma" in m: default_idx = i; break
        
        selected_model = st.selectbox("🧠 KI-Modell wählen", models, index=default_idx if models else None)
        if selected_model and "gemma" in selected_model:
            st.success("🚀 Highspeed-Modus (14k Limits)")
            pause_time = 1.0 
        else:
            st.warning("🐢 Standard-Modus")
            pause_time = 8.0 
        
        st.markdown("---")
        st.write("🤖 **KI-Update (Manuell)**")
        missing_count = 0
        missing_indices = []
        if not df.empty:
            for i, r in df.iterrows():
                if len(str(r.get("Teaser", ""))) < 5:
                    missing_count += 1
                    missing_indices.append(i)
        
        if missing_count > 0:
            st.info(f"{missing_count} Bücher offen.")
            if st.button("✨ Fehlende Infos laden"):
                if not selected_model: st.error("Kein Modell!"); st.stop()
                with st.status(f"Starte mit {selected_model}...", expanded=True) as status:
                    prog_bar = status.progress(0)
                    headers = [str(h).lower() for h in ws_books.row_values(1)]
                    try:
                        c_tag = headers.index("tags") + 1
                        c_year = headers.index("erschienen") + 1
                        c_teaser = headers.index("teaser") + 1
                        c_bio = headers.index("bio") + 1
                    except: st.error("Spaltenfehler"); st.stop()
                    
                    done = 0
                    for idx in missing_indices:
                        row = df.loc[idx]
                        status.write(f"Bearbeite: **{row['Titel']}**...")
                        log_event(ws_logs, f"Auto-Update Start: {row['Titel']}", "AI_JOB")
                        
                        ai_data, err = fetch_all_ai_data_manual(row["Titel"], row["Autor"], selected_model)
                        if err == "RATE_LIMIT":
                            status.write("⏳ Rate Limit! Warte 60s...")
                            time.sleep(60)
                            ai_data, err = fetch_all_ai_data_manual(row["Titel"], row["Autor"], selected_model)
                        
                        if ai_data:
                            try:
                                cell = ws_books.find(row["Titel"])
                                if ai_data.get("tags"): ws_books.update_cell(cell.row, c_tag, ai_data["tags"])
                                if ai_data.get("year"): ws_books.update_cell(cell.row, c_year, ai_data["year"])
                                if ai_data.get("teaser"): ws_books.update_cell(cell.row, c_teaser, ai_data["teaser"])
                                if ai_data.get("bio"): ws_books.update_cell(cell.row, c_bio, ai_data["bio"])
                                log_event(ws_logs, f"Auto-Update Erfolg: {row['Titel']}", "SUCCESS")
                            except Exception as e: log_event(ws_logs, f"Sheet Error: {e}", "ERROR")
                        
                        done += 1
                        prog_bar.progress(done / missing_count)
                        time.sleep(pause_time)
                    
                    status.update(label="Fertig!", state="complete", expanded=False)
                    del st.session_state.df_books
                    time.sleep(1); st.rerun()
        else: st.success("Alles aktuell.")
            
        with st.expander("📜 System-Log"):
            try:
                logs = ws_logs.get_all_values()
                if len(logs) > 1:
                    last_logs = logs[-10:]
                    txt = ""
                    for l in reversed(last_logs): txt += f"{l[0][11:]} {l[2]}\n"
                    st.code(txt)
            except: st.write("Keine Logs")

    st.write("")
    nav = st.radio("Navigation", ["✍️ Neu", "🔍 Sammlung", "🔮 Merkliste", "👥 Statistik"], 
                   horizontal=True, 
                   index=["✍️ Neu", "🔍 Sammlung", "🔮 Merkliste", "👥 Statistik"].index(st.session_state.active_tab),
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
                        cleanup_author_duplicates_batch(ws_books, ws_authors)
                        del st.session_state.df_books
                        log_event(ws_logs, f"Neu: {t}", "NEW")
                    st.success(f"Gespeichert: {t}"); st.balloons(); time.sleep(1.0); st.rerun()
                else: st.error("Format: Titel, Autor")

    elif st.session_state.active_tab == "🔍 Sammlung":
        df_s = df[df["Status"] == "Gelesen"].copy()
        q = st.text_input("Suche...", label_visibility="collapsed")
        if q: df_s = df_s[df_s["Titel"].str.lower().str.contains(q.lower())]
        cols = st.columns(3)
        for i, (idx, row) in enumerate(df_s.iterrows()):
            with cols[i % 3]:
                with st.container(border=True):
                    c1, c2 = st.columns([1, 2])
                    with c1:
                        st.image(row["Cover"] if row["Cover"]!="-" else "https://via.placeholder.com/100", use_container_width=True)
                        if st.button("ℹ️ Info", key=f"k_{idx}", use_container_width=True): 
                            show_book_details(row, ws_books, ws_authors, ws_logs)
                    with c2:
                        st.write(f"**{row['Titel']}**")
                        st.caption(f"{row['Autor']}")
                        try: star_val = int(row['Bewertung'])
                        except: star_val = 0
                        new_stars = st.feedback("stars", key=f"fb_{idx}")
                        if f"fb_{idx}" in st.session_state and st.session_state[f"fb_{idx}"] is not None:
                            user_val = st.session_state[f"fb_{idx}"] + 1
                            if user_val != star_val:
                                update_single_entry(ws_books, row["Titel"], "Bewertung", user_val)
                                st.toast("Gespeichert!"); del st.session_state.df_books; time.sleep(0.2); st.rerun()
                        elif star_val > 0: st.markdown(f"<div style='color:#d35400'>{'★'*star_val}</div>", unsafe_allow_html=True)
                        old_n = row["Notiz"]
                        new_n = st.text_area("Notiz", old_n, key=f"n_{idx}", height=70, label_visibility="collapsed")
                        if new_n != old_n:
                            update_single_entry(ws_books, row["Titel"], "Notiz", new_n)
                            st.toast("Gespeichert!"); del st.session_state.df_books; time.sleep(0.5); st.rerun()

    elif st.session_state.active_tab == "🔮 Merkliste":
        with st.expander("➕ Neuer Wunsch"):
            with st.form("wish", clear_on_submit=True):
                iw = st.text_input("Titel, Autor")
                inote = st.text_input("Notiz")
                if st.form_submit_button("Hinzufügen"):
                    if "," in iw:
                        t, a = [x.strip() for x in iw.split(",", 1)]
                        c, g, y = fetch_meta_single(t, a)
                        ws_books.append_row([t, a, g, "", c or "-", datetime.now().strftime("%Y-%m-%d"), inote, "Wunschliste", "", y or "", "", ""])
                        del st.session_state.df_books; log_event(ws_logs, f"Wunsch: {t}", "WISH"); st.success("Gemerkt!"); st.balloons(); time.sleep(1); st.rerun()
        df_w = df[df["Status"] == "Wunschliste"].copy()
        if not df_w.empty:
            cols = st.columns(3)
            for i, (idx, row) in enumerate(df_w.iterrows()):
                with cols[i % 3]:
                    with st.container(border=True):
                        c1, c2 = st.columns([1, 2])
                        with c1:
                            st.image(row["Cover"] if row["Cover"]!="-" else "https://via.placeholder.com/100", use_container_width=True)
                            if st.button("ℹ️ Info", key=f"wk_{idx}"): show_book_details(row, ws_books, ws_authors, ws_logs)
                            if st.button("✅ Gelesen", key=f"wr_{idx}"):
                                cell = ws_books.find(row["Titel"])
                                ws_books.update_cell(cell.row, 8, "Gelesen")
                                ws_books.update_cell(cell.row, 6, datetime.now().strftime("%Y-%m-%d"))
                                del st.session_state.df_books; st.rerun()
                        with c2:
                            st.write(f"**{row['Titel']}**")
                            st.caption(row['Autor'])
                            old_n = row["Notiz"]
                            new_n = st.text_area("Notiz", old_n, key=f"wn_{idx}", height=70, label_visibility="collapsed")
                            if new_n != old_n:
                                update_single_entry(ws_books, row["Titel"], "Notiz", new_n)
                                st.toast("Gespeichert!"); del st.session_state.df_books; time.sleep(0.5); st.rerun()
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
                    all_tags.extend([x.strip() for x in str(t).split(",") if x.strip()])
                if all_tags:
                    tag_counts = pd.Series(all_tags).value_counts().reset_index()
                    tag_counts.columns = ["Thema", "Anzahl"]
                    st.dataframe(tag_counts, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
