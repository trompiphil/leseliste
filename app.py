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
import threading
import re

# --- KONFIGURATION ---
st.set_page_config(page_title="Meine Bibliothek", page_icon="📚", layout="wide")

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
    .log-box { font-family: monospace; font-size: 0.8em; background-color: #333; color: #0f0; padding: 10px; border-radius: 5px; max-height: 300px; overflow-y: scroll; }
    </style>
""", unsafe_allow_html=True)

# --- BACKEND ---
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
    return ws_books, ws_logs, ws_authors

def log_event(ws_logs, message, msg_type="INFO"):
    try:
        ts = datetime.now().strftime("%H:%M:%S")
        ws_logs.append_row([ts, msg_type, str(message)])
    except: pass

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

# --- API HELPERS ---
def process_genre(raw):
    if not raw: return "Roman"
    try: t = GoogleTranslator(source='auto', target='de').translate(raw); return "Roman" if "römisch" in t.lower() else t
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

# --- ROBUSTER KI CORE ---
def clean_json_string(text):
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match: return match.group(0)
        return text
    except: return text

# Findet das beste Modell (OHNE Streamlit Cache, für den Thread)
def find_working_model_pure(api_key):
    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
    try:
        r = requests.get(url)
        if r.status_code == 200:
            data = r.json()
            valid_models = []
            for m in data.get('models', []):
                if 'generateContent' in m.get('supportedGenerationMethods', []):
                    valid_models.append(m['name'].split('/')[-1])
            
            # WICHTIG: 2.0-flash-exp und 1.5-flash-8b bevorzugen, 1.5-flash als fallback
            priority = ["gemini-2.0-flash-exp", "gemini-1.5-flash-8b", "gemini-1.5-flash", "gemini-1.5-pro"]
            
            for p in priority:
                if p in valid_models: return p
            
            if valid_models: return valid_models[0]
            
        return None
    except: return None

def call_gemini(prompt, model_name):
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
            except: return None, f"Parse Fehler. Raw: {response.text}"
        elif response.status_code == 429: return None, "RATELIMIT" # Spezielles Keyword
        elif response.status_code == 404: return None, f"Modell nicht gefunden (404)"
        else: return None, f"HTTP Fehler {response.status_code}: {response.text}"
    except Exception as e: return None, str(e)

def fetch_all_ai_data_debug(titel, autor, model_name, ws_logs):
    prompt = f"""
    Buch: "{titel}" von {autor}.
    Erstelle ein JSON mit genau diesen Keys: "tags", "year", "teaser" (max 60 Worte), "bio" (max 30 Worte).
    Antworte NUR mit dem JSON String. Keine Markdown Formatierung.
    """
    txt, err = call_gemini(prompt, model_name)
    
    if err:
        if "RATELIMIT" in err: return "RATELIMIT" # Fehler hochreichen
        log_event(ws_logs, f"KI Fehler bei '{titel}': {err}", "ERROR")
        return {}
    
    try: 
        data = json.loads(txt)
        return data
    except Exception as e: 
        log_event(ws_logs, f"JSON Parse Error bei '{titel}': {str(e)}", "ERROR")
        return {}

# --- BACKGROUND WORKER (SINGLETON) ---
def background_worker_process(missing_books_data, creds_dict):
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        if "private_key" in creds_dict: creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        sh = client.open("Bücherliste")
        ws = sh.sheet1
        try: ws_logs = sh.worksheet("Logs")
        except: ws_logs = sh.add_worksheet("Logs", 1000, 3)
        
        log_event(ws_logs, "Autopilot gestartet (Singleton).", "START")
        
        api_key = st.secrets["gemini_api_key"]
        model_name = find_working_model_pure(api_key)
        
        if not model_name:
            log_event(ws_logs, "Kein Modell im Hintergrund gefunden!", "ERROR")
            return
            
        log_event(ws_logs, f"Nutze Modell: {model_name}", "INIT")
        
        headers = [str(h).lower() for h in ws.row_values(1)]
        c_tag = headers.index("tags") + 1
        c_year = headers.index("erschienen") + 1
        c_teaser = headers.index("teaser") + 1
        c_bio = headers.index("bio") + 1

        for book in missing_books_data:
            # Schneller Start für den ersten, dann Pausen
            time.sleep(5) 
            
            titel = book['Titel']
            log_event(ws_logs, f"Bearbeite: {titel}", "INFO")
            
            ai_data = fetch_all_ai_data_debug(titel, book['Autor'], model_name, ws_logs)
            
            if ai_data == "RATELIMIT":
                log_event(ws_logs, f"Rate Limit! Warte 60s...", "WARN")
                time.sleep(60) # Strafbank
                # Versuch es für das gleiche Buch nochmal beim nächsten Loop oder überspring
                continue 
            
            if isinstance(ai_data, dict) and ai_data:
                try:
                    cell = ws.find(titel)
                    if ai_data.get("tags"): ws.update_cell(cell.row, c_tag, ai_data["tags"])
                    if ai_data.get("year"): ws.update_cell(cell.row, c_year, ai_data["year"])
                    if ai_data.get("teaser"): ws.update_cell(cell.row, c_teaser, ai_data["teaser"])
                    if ai_data.get("bio"): ws.update_cell(cell.row, c_bio, ai_data["bio"])
                    log_event(ws_logs, f"Erfolg: {titel}", "SUCCESS")
                except Exception as e:
                    log_event(ws_logs, f"Sheet Fehler: {str(e)}", "ERROR")
            else:
                log_event(ws_logs, f"Keine Daten erhalten für {titel}", "WARN")
                
        log_event(ws_logs, "Alle Aufträge erledigt.", "END")
                
    except Exception as e:
        print(f"Background Crash: {e}")

# --- HELPERS ---
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

def delete_book(ws, titel):
    try:
        cell = ws.find(titel)
        ws.delete_rows(cell.row)
        return True
    except: return False

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
    
    # --- BACKGROUND CHECK (SINGLETON) ---
    
    # Check if thread is already running
    is_running = any(t.name == "BackgroundWorker" for t in threading.enumerate())
    
    if not is_running:
        missing_books = []
        if not df.empty:
            for i, r in df.iterrows():
                # Nur fehlende bearbeiten
                if len(str(r.get("Teaser", ""))) < 5 or len(str(r.get("Tags", ""))) < 2:
                    missing_books.append(r.to_dict())
        
        if missing_books:
            creds = dict(st.secrets["gcp_service_account"])
            # Thread mit festem Namen starten
            t = threading.Thread(target=background_worker_process, args=(missing_books, creds), name="BackgroundWorker")
            t.start()
            st.toast(f"Hintergrund-Dienst gestartet ({len(missing_books)} Bücher)")

    with st.sidebar:
        st.write("🔧 **Einstellungen**")
        if st.button("🔄 Cache leeren"): 
            st.session_state.clear(); st.rerun()
        
        st.markdown("---")
        st.write("📜 **System-Log (Live)**")
        try:
            logs = ws_logs.get_all_values()
            if len(logs) > 1:
                last_logs = logs[-6:]
                log_text = ""
                for l in reversed(last_logs):
                    log_text += f"[{l[0].split(' ')[0]}] {l[2]}\n"
                st.markdown(f"<div class='log-box'>{log_text}</div>", unsafe_allow_html=True)
            else: st.info("Keine Logs.")
        except: st.warning("Log-Fehler")

    tab_neu, tab_sammlung, tab_merkliste, tab_stats = st.tabs(["✍️ Neu", "🔍 Sammlung", "🔮 Merkliste", "👥 Statistik"])
    
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
                    with st.spinner("Speichere..."):
                        c, g, y = fetch_meta(t, fa)
                        ws_books.append_row([t, fa, g, val, c or "-", datetime.now().strftime("%Y-%m-%d"), note, "Gelesen", "", y or "", "", ""])
                        cleanup_author_duplicates_batch(ws_books, ws_authors)
                        del st.session_state.df_books
                    st.success(f"Gespeichert: {t} (KI läuft im Hintergrund)"); time.sleep(1.0); st.rerun()
                else: st.error("Format: Titel, Autor")

    with tab_sammlung:
        view = st.radio("Ansicht", ["Kacheln", "Liste"], horizontal=True, label_visibility="collapsed")
        df_s = df[df["Status"] == "Gelesen"].copy()
        q = st.text_input("Suche...", label_visibility="collapsed")
        if q: df_s = df_s[df_s["Titel"].str.lower().str.contains(q.lower())]
        
        if view == "Liste":
            df_list = df_s[["Titel", "Autor", "Bewertung", "Notiz"]].copy()
            df_list.insert(0, "ℹ️", False)
            edited_df = st.data_editor(df_list, column_config={"ℹ️": st.column_config.CheckboxColumn("Info", width="small"), "Titel": st.column_config.TextColumn(disabled=True), "Autor": st.column_config.TextColumn(disabled=True), "Bewertung": st.column_config.NumberColumn("⭐", min_value=0, max_value=5, step=1), "Notiz": st.column_config.TextColumn(width="large")}, hide_index=True, use_container_width=True, key="editor_list")
            if edited_df["ℹ️"].any():
                sel_idx = edited_df[edited_df["ℹ️"]].index[0]
                show_book_details(df_s.loc[sel_idx], ws_books, ws_authors)
            if st.button("💾 Änderungen speichern"):
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
                        ws_books.append_row([t, a, g, "", c or "-", datetime.now().strftime("%Y-%m-%d"), inote, "Wunschliste", "", y or "", "", ""])
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
                                    del st.session_state.df_books; st.rerun()
                            with c2:
                                st.write(f"**{row['Titel']}**")
                                st.caption(row['Autor'])
                                old_n = row["Notiz"]
                                new_n = st.text_area("Notiz", old_n, key=f"wn_{idx}", height=70, label_visibility="collapsed")
                                if new_n != old_n:
                                    update_single_entry(ws_books, row["Titel"], "Notiz", new_n)
                                    st.toast("Gespeichert!"); del st.session_state.df_books; time.sleep(0.5); st.rerun()
            else:
                df_w_list = df_w[["Titel", "Autor", "Notiz"]].copy()
                df_w_list.insert(0, "ℹ️", False)
                edited_w = st.data_editor(df_w_list, column_config={"ℹ️": st.column_config.CheckboxColumn("Info", width="small")}, hide_index=True, use_container_width=True, key="editor_wish")
                if edited_w["ℹ️"].any():
                    sel_idx = edited_w[edited_w["ℹ️"]].index[0]
                    show_book_details(df_w.loc[sel_idx], ws_books, ws_authors)
        else: st.info("Leer.")

    with tab_stats:
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
