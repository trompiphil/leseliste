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
    
    # Sicherstellen, dass Logs existiert
    try: 
        ws_logs = sh.worksheet("Logs")
    except: 
        ws_logs = sh.add_worksheet(title="Logs", rows=1000, cols=3)
        ws_logs.append_row(["Zeitstempel", "Typ", "Nachricht"])
        
    try: ws_authors = sh.worksheet("Autoren")
    except: ws_authors = sh.add_worksheet(title="Autoren", rows=1000, cols=1); ws_authors.update_cell(1, 1, "Name")
    return ws_books, ws_logs, ws_authors

def log_event(ws_logs, message, msg_type="INFO"):
    """Schreibt Logs zuverlässig in Sheet"""
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

# --- SMARTER COVER SEARCH ---
def process_genre(raw):
    if not raw: return "Roman"
    try: t = GoogleTranslator(source='auto', target='de').translate(raw); return "Roman" if "römisch" in t.lower() else t
    except: return "Roman"

def fetch_smart_meta(titel, autor, ws_logs=None):
    """
    Intelligente Suche: Prüft Autor-Übereinstimmung und sucht bestes Bild.
    """
    c, g, y = "", "Roman", ""
    
    # 1. Google Books API (Mit Autor-Check)
    try:
        # Suche gezielt nach Titel und Autor
        query = f"intitle:{titel} inauthor:{autor}"
        url = f"https://www.googleapis.com/books/v1/volumes?q={urllib.parse.quote(query)}&maxResults=5&langRestrict=de"
        r = requests.get(url).json()
        
        found_item = None
        
        if "items" in r:
            # Wir gehen die Ergebnisse durch und suchen den besten Match
            for item in r["items"]:
                info = item.get("volumeInfo", {})
                authors = info.get("authors", [])
                
                # Check: Ist der Autor im Ergebnis enthalten? (Fuzzy Check)
                author_match = False
                for a in authors:
                    if autor.lower() in a.lower() or a.lower() in autor.lower():
                        author_match = True
                        break
                
                if author_match:
                    found_item = info
                    break # Bestes Ergebnis gefunden
            
            # Fallback: Wenn kein Autor passt, nimm das erste (besser als nix)
            if not found_item and r["items"]:
                found_item = r["items"][0]["volumeInfo"]
        
        if found_item:
            # Genre & Jahr
            g = process_genre(found_item.get("categories", ["Roman"])[0])
            pub = found_item.get("publishedDate", "")
            if pub: y = pub[:4]
            
            # Bestes Bild suchen (Extra Large -> Large -> Medium -> Small -> Thumbnail)
            imgs = found_item.get("imageLinks", {})
            if "extraLarge" in imgs: c = imgs["extraLarge"]
            elif "large" in imgs: c = imgs["large"]
            elif "medium" in imgs: c = imgs["medium"]
            elif "thumbnail" in imgs: c = imgs["thumbnail"]
            elif "smallThumbnail" in imgs: c = imgs["smallThumbnail"]
            
            # HTTPS erzwingen
            if c.startswith("http://"): c = c.replace("http://", "https://")
            
    except Exception as e:
        if ws_logs: log_event(ws_logs, f"Google Books Error: {e}", "WARN")

    # 2. OpenLibrary Fallback (Wenn Google nichts liefert)
    if not c:
        try:
            r = requests.get(f"https://openlibrary.org/search.json?q={titel} {autor}&limit=1").json()
            if r["docs"]: 
                doc = r["docs"][0]
                c = f"https://covers.openlibrary.org/b/id/{doc['cover_i']}-L.jpg" # L für Large
                if not y and "first_publish_year" in doc: y = str(doc["first_publish_year"])
        except: pass
        
    return c, g, y

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
    Antworte NUR mit validem JSON. Keine Einleitung.
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

@st.dialog("📖 Buch-Details")
def show_book_details(book, ws_books, ws_authors):
    d_tab1, d_tab2 = st.tabs(["ℹ️ Info", "✏️ Bearbeiten"])
    with d_tab1:
        st.markdown(f"### {book['Titel']}")
        year_str = f" ({book.get('Erschienen')})" if book.get('Erschienen') else ""
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
            teaser = book.get("Teaser", "")
            bio = book.get("Bio", "")
            if not teaser or len(str(teaser)) < 5: teaser_disp = "<i>(Keine Info. Bitte 'Fehlende Infos laden' nutzen)</i>"
            else: teaser_disp = teaser
            if not bio or len(str(bio)) < 5: bio_disp = "-"
            else: bio_disp = bio

            st.markdown(f"""
            <div class="ai-box">
                <b>📖 Worum geht's?</b><br>{teaser_disp}
            </div>
            <div class="ai-box" style="border-left-color: #2980b9; background-color: #eaf2f8; margin-top:10px;">
                <b>👤 Autor</b><br>{bio_disp}
            </div>
            """, unsafe_allow_html=True)
            st.markdown("---")
            wiki_book = f"https://de.wikipedia.org/w/index.php?search={urllib.parse.quote(book['Titel'])}"
            google = f"https://www.google.com/search?q={urllib.parse.quote(book['Titel'] + ' ' + book['Autor'])}"
            st.markdown(f"[🔍 Google]({google}) | [📖 Wiki]({wiki_book})")

    with d_tab2:
        with st.form("edit_book_form"):
            new_title = st.text_input("Titel", value=book["Titel"])
            new_author = st.text_input("Autor", value=book["Autor"])
            new_year = st.text_input("Erscheinungsjahr", value=book.get("Erschienen", ""))
            
            current_cover = book.get("Cover", "")
            new_cover_url = st.text_input("Cover URL (manuell)", value=current_cover)
            
            new_tags = st.text_input("Tags", value=book.get("Tags", ""))
            new_teaser = st.text_area("Teaser", value=book.get("Teaser", ""))
            new_bio = st.text_area("Bio", value=book.get("Bio", ""))
            
            if st.form_submit_button("💾 Speichern"):
                try:
                    cell = ws_books.find(book["Titel"])
                    headers = [str(h).lower() for h in ws_books.row_values(1)]
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
                    ws_books.update_cell(cell.row, col_c, new_cover_url)
                    ws_books.update_cell(cell.row, col_tags, new_tags)
                    ws_books.update_cell(cell.row, col_y, new_year)
                    ws_books.update_cell(cell.row, col_teaser, new_teaser)
                    ws_books.update_cell(cell.row, col_bio, new_bio)
                    
                    cleanup_author_duplicates_batch(ws_books, ws_authors)
                    del st.session_state.df_books
                    st.success("Gespeichert!"); time.sleep(1); st.rerun()
                except Exception as e: st.error(f"Fehler: {e}")
        st.markdown("---")
        if st.button("🗑️ Löschen", type="primary"):
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
                        
                        log_event(ws_logs, f"Start KI Update: {row['Titel']}", "MANUAL_AI")
                        ai_data, err = fetch_all_ai_data_manual(row["Titel"], row["Autor"], selected_model)
                        
                        if err == "RATE_LIMIT":
                            log_event(ws_logs, "Rate Limit erreicht.", "WARN")
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
                                log_event(ws_logs, f"KI Daten gespeichert: {row['Titel']}", "SUCCESS")
                            except Exception as e:
                                log_event(ws_logs, f"Fehler beim Speichern: {e}", "ERROR")
                        else:
                            log_event(ws_logs, f"KI Fehler: {err}", "ERROR")
                        
                        done += 1
                        prog_bar.progress(done / missing_count)
                        time.sleep(pause_time) 
                    
                    status.update(label="Fertig!", state="complete", expanded=False)
                    del st.session_state.df_books
                    time.sleep(1)
                    st.rerun()
        else:
            st.success("Alles aktuell.")
            
        with st.expander("📜 System-Log"):
            try:
                logs = ws_logs.get_all_values()
                if len(logs) > 1:
                    last_logs = logs[-10:]
                    txt = ""
                    for l in reversed(last_logs): txt += f"{l[0][11:]} {l[2]}\n"
                    st.code(txt)
            except: st.write("Keine Logs")

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
                        c, g, y = fetch_smart_meta(t, fa, ws_logs)
                        ws_books.append_row([t, fa, g, val, c or "-", datetime.now().strftime("%Y-%m-%d"), note, "Gelesen", "", y or "", "", ""])
                        cleanup_author_duplicates_batch(ws_books, ws_authors)
                        del st.session_state.df_books
                        log_event(ws_logs, f"Buch hinzugefügt: {t}", "NEW_BOOK")
                    st.success(f"Gespeichert: {t}"); st.balloons(); time.sleep(1.0); st.rerun()
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
                            
                            # --- BUTTONS ---
                            b1, b2 = st.columns([3, 1])
                            if b1.button("ℹ️ Info", key=f"k_{idx}", use_container_width=True): show_book_details(row, ws_books, ws_authors)
                            
                            # REFRESH BUTTON
                            if b2.button("🔄", key=f"r_{idx}", help="Cover neu suchen"):
                                with st.spinner("Suche Cover..."):
                                    c_new, _, _ = fetch_smart_meta(row["Titel"], row["Autor"], ws_logs)
                                    if c_new:
                                        try:
                                            cell = ws_books.find(row["Titel"])
                                            headers = [str(h).lower() for h in ws_books.row_values(1)]
                                            try: c_col = headers.index("cover") + 1
                                            except: c_col = 5
                                            
                                            ws_books.update_cell(cell.row, c_col, c_new)
                                            log_event(ws_logs, f"Cover Update: {row['Titel']}", "UPDATE")
                                            del st.session_state.df_books
                                            st.rerun()
                                        except Exception as e: 
                                            st.error(f"Fehler: {e}")
                                            log_event(ws_logs, f"Cover Fehler: {e}", "ERROR")
                                    else:
                                        st.toast("Kein besseres Cover gefunden.")
                            
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
                        c, g, y = fetch_smart_meta(t, a, ws_logs)
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
