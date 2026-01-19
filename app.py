import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import requests
import time
import urllib.parse
from datetime import datetime
from deep_translator import GoogleTranslator
import google.generativeai as genai

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
            try: 
                if d["Bewertung"] and str(d["Bewertung"]).strip().isdigit(): d["Bewertung"] = int(d["Bewertung"])
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

def update_full_dataframe(ws, new_df):
    current_data = ws.get_all_values()
    headers = [str(h).lower() for h in current_data[0]]
    col_idx = {k: headers.index(k) for k in ["titel","autor","bewertung","notiz","status"] if k in headers}
    if not col_idx: return False
    rows_to_delete = [] 
    for index, row in new_df.iterrows():
        titel = row["Titel"]
        if row.get("Löschen", False):
            try: rows_to_delete.append(ws.find(titel).row)
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
    for r in rows_to_delete: ws.delete_rows(r); time.sleep(0.5)
    return True

# --- API & KI HELPER ---
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

@st.cache_data(show_spinner=False)
def get_ai_book_info(titel, autor):
    """Fragt Google Gemini nach einer Zusammenfassung und Bio"""
    if "gemini_api_key" not in st.secrets:
        return {"teaser": "Fehler: 'gemini_api_key' fehlt in Secrets.", "bio": "-"}
    
    try:
        genai.configure(api_key=st.secrets["gemini_api_key"])
        # WICHTIG: Wir nutzen jetzt das Modell, das mit der neuen Library funktioniert
        model = genai.GenerativeModel('gemini-1.5-flash') 
        
        prompt = f"""
        Du bist ein literarischer Assistent.
        Buch: "{titel}" von {autor}.
        
        Aufgabe 1: Schreibe einen spannenden Teaser über den Inhalt (max 80 Wörter). Keine Spoiler!
        Aufgabe 2: Schreibe eine sehr kurze Biografie über den Autor (max 40 Wörter).
        
        Antworte im JSON Format: {{ "teaser": "...", "bio": "..." }}
        """
        response = model.generate_content(prompt)
        text = response.text.replace("```json", "").replace("```", "").strip()
        import json
        return json.loads(text)
        
    except Exception as e:
        return {"teaser": f"KI-Fehler: {str(e)}", "bio": "Bitte requirements.txt prüfen."}

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

# --- NAVIGATION HELPER ---
def open_detail_view(book_row):
    st.session_state.selected_book = book_row
    st.rerun()

def close_detail_view():
    st.session_state.selected_book = None
    st.rerun()

# --- MAIN ---
def main():
    if "selected_book" not in st.session_state: st.session_state.selected_book = None

    with st.sidebar:
        st.write("🔧 **Einstellungen**")
        if st.button("🔄 Cache leeren", help="Klicken, wenn Daten hängen"): 
            st.session_state.clear(); st.rerun()
        
    st.title("📚 Meine Bibliothek")
    
    client = get_connection()
    if not client: st.error("Secrets fehlen!"); st.stop()
    ws_books, ws_authors = setup_sheets(client)
    
    if "checked" not in st.session_state: check_structure(ws_books); st.session_state.checked=True
    if "df_books" not in st.session_state: 
        with st.spinner("Lade Daten..."): st.session_state.df_books = get_data(ws_books)
    
    df = st.session_state.df_books
    authors = list(set([a for i, row in df.iterrows() if row["Status"] != "Wunschliste" for a in [row["Autor"]] if a]))
    
    # -----------------------------------------------------------
    # ANSICHT 1: DETAIL PAGE (AI POWERED)
    # -----------------------------------------------------------
    if st.session_state.selected_book is not None:
        book = st.session_state.selected_book
        
        if st.button("⬅️ Zurück zur Übersicht"): close_detail_view()
        
        st.header(book["Titel"])
        st.markdown(f"**von {book['Autor']}**")
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            cov = book["Cover"] if book["Cover"] != "-" else "https://via.placeholder.com/200x300?text=No+Cover"
            st.markdown(f'<img src="{cov}" style="width:100%; max-width:220px; border-radius:12px; box-shadow:0 4px 12px rgba(0,0,0,0.3); margin-bottom:20px;">', unsafe_allow_html=True)
            st.info(f"**Deine Bewertung:**\n\n{'★' * int(book['Bewertung'])}")
            if book["Notiz"]:
                st.markdown("#### 📝 Deine Notiz")
                st.write(f"*{book['Notiz']}*")
            
        with col2:
            # KI GENERIERUNG STARTEN
            ai_data = None
            if "gemini_api_key" in st.secrets:
                with st.spinner("✨ Die KI liest das Buch gerade quer..."):
                    ai_data = get_ai_book_info(book["Titel"], book["Autor"])
            else:
                st.warning("⚠️ Kein KI-Schlüssel gefunden.")

            if ai_data:
                st.markdown(f"""
                <div class="ai-box">
                    <h4>📖 Worum geht's?</h4>
                    <p>{ai_data.get('teaser', 'Fehler beim Laden')}</p>
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown(f"""
                <div class="ai-box" style="border-left-color: #2980b9; background-color: #eaf2f8;">
                    <h4>👤 Über den Autor</h4>
                    <p>{ai_data.get('bio', '-')}</p>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("### 🔗 Recherche")
            wiki_book = f"https://de.wikipedia.org/w/index.php?search={urllib.parse.quote(book['Titel'])}"
            wiki_auth = f"https://de.wikipedia.org/w/index.php?search={urllib.parse.quote(book['Autor'])}"
            google_search = f"https://www.google.com/search?q={urllib.parse.quote(book['Titel'] + ' ' + book['Autor'])}"
            
            st.markdown(f"""
            * 🔍 [Google Suche]({google_search})
            * 📖 [Wikipedia: Buch]({wiki_book})
            * 👤 [Wikipedia: Autor]({wiki_auth})
            """)

    # -----------------------------------------------------------
    # ANSICHT 2: NORMALE TABS
    # -----------------------------------------------------------
    else:
        tab_neu, tab_sammlung, tab_merkliste, tab_stats = st.tabs(["✍️ Neu", "🔍 Sammlung", "🔮 Merkliste", "📊 Statistik"])
        
        with tab_neu:
            st.header("Buch hinzufügen")
            with st.form("add_book_form", clear_on_submit=True):
                c1, c2 = st.columns([2, 1])
                with c1:
                    inp = st.text_input("Titel, Autor (mit Komma trennen!)")
                    note = st.text_input("Notiz (optional)")
                with c2:
                    st.write("Bewertung:")
                    rating_idx = st.feedback("stars") 
                
                if st.form_submit_button("💾 Speichern"):
                    if "," in inp:
                        rating_val = (rating_idx + 1) if rating_idx is not None else 0
                        tit, aut = [x.strip() for x in inp.split(",", 1)]
                        final_aut = smart_author(aut, authors)
                        with st.spinner("Verarbeite..."):
                            cov, gen = fetch_meta(tit, final_aut)
                            ws_books.append_row([tit, final_aut, gen, rating_val, cov or "-", datetime.now().strftime("%Y-%m-%d"), note, "Gelesen"])
                            cleanup_author_duplicates_batch(ws_books, ws_authors)
                            del st.session_state.df_books 
                        st.success(f"Gespeichert: {tit}"); time.sleep(1); st.rerun()
                    else: st.error("Formatfehler.")

        with tab_sammlung:
            view = st.radio("Ansicht", ["Kacheln", "Liste"], horizontal=True, label_visibility="collapsed")
            df_show = df[ (df["Status"] == "Gelesen") ].copy()
            q = st.text_input("🔍 Filter (Titel, Autor, Notiz...)", placeholder="Suchen...")
            if q:
                q = q.lower()
                df_show = df_show[df_show["Titel"].str.lower().str.contains(q) | df_show["Autor"].str.lower().str.contains(q) | df_show["Notiz"].str.lower().str.contains(q)]
            
            try: df_show["Hinzugefügt"] = pd.to_datetime(df_show["Hinzugefügt"], errors='coerce')
            except: pass
            df_show = df_show.sort_values(by="Hinzugefügt", ascending=False)

            if view == "Liste":
                for i, row in df_show.iterrows():
                    c1, c2, c3, c4 = st.columns([2,2,1,2])
                    c1.write(f"**{row['Titel']}**")
                    c2.write(row['Autor'])
                    c3.write(f"{row['Bewertung']} ⭐")
                    if c4.button("ℹ️ Details", key=f"dl_{i}"): open_detail_view(row)
                    st.markdown("---")
            else:
                cols = st.columns(3) 
                for i, (idx, row) in enumerate(df_show.iterrows()):
                    with cols[i % 3]:
                        with st.container(border=True):
                            c_img, c_info = st.columns([1, 2])
                            with c_img:
                                cov = row["Cover"] if row["Cover"] != "-" else "https://via.placeholder.com/150x220?text=No+Cover"
                                st.image(cov, use_container_width=True)
                                if st.button("ℹ️ Info", key=f"kd_{idx}"): open_detail_view(row)
                                try: stars = int(row["Bewertung"])
                                except: stars = 0
                                w_key = f"star_widget_{idx}"
                                new_rating_idx = st.feedback("stars", key=w_key)
                                if w_key in st.session_state and st.session_state[w_key] is not None:
                                    user_val = st.session_state[w_key] + 1
                                    if user_val != stars:
                                        update_single_entry(ws_books, row["Titel"], "Bewertung", user_val)
                                        st.toast(f"Gespeichert!"); del st.session_state.df_books; time.sleep(0.5); st.rerun()
                            with c_info:
                                st.subheader(row["Titel"])
                                st.caption(row["Autor"])
                                current_note = row["Notiz"]
                                new_note = st.text_area("Notiz", value=current_note, key=f"note_area_{idx}", label_visibility="collapsed", height=80, placeholder="Notiz...")
                                if new_note != current_note:
                                    update_single_entry(ws_books, row["Titel"], "Notiz", new_note)
                                    st.toast(f"Gespeichert!"); del st.session_state.df_books; time.sleep(0.5); st.rerun()

        with tab_merkliste:
            w_view = st.radio("Ansicht Merkliste", ["Kacheln", "Liste"], horizontal=True, label_visibility="collapsed", key="w_view")
            with st.expander("➕ Neuen Wunsch hinzufügen", expanded=False):
                with st.form("wish_form", clear_on_submit=True):
                    i_w = st.text_input("Titel, Autor")
                    n_w = st.text_input("Notiz")
                    if st.form_submit_button("Auf Merkliste"):
                        if "," in i_w:
                            t, a = [x.strip() for x in i_w.split(",",1)]
                            c, g = fetch_meta(t, a)
                            ws_books.append_row([t, a, g, "", c or "-", datetime.now().strftime("%Y-%m-%d"), n_w, "Wunschliste"])
                            cleanup_author_duplicates_batch(ws_books, ws_authors)
                            del st.session_state.df_books; st.rerun()
                        else: st.error("Bitte Titel, Autor eingeben.")
            
            df_w = df[df["Status"]=="Wunschliste"].copy()
            if not df_w.empty:
                if w_view == "Kacheln":
                    cols = st.columns(3)
                    for i, (idx, row) in enumerate(df_w.iterrows()):
                        with cols[i % 3]:
                            with st.container(border=True):
                                c_img, c_info = st.columns([1, 2])
                                with c_img:
                                    cov = row["Cover"] if row["Cover"] != "-" else "https://via.placeholder.com/150x220?text=No+Cover"
                                    st.image(cov, use_container_width=True)
                                    if st.button("ℹ️ Info", key=f"wkd_{idx}"): open_detail_view(row)
                                    if st.button("✅ Gelesen", key=f"wk_{idx}", use_container_width=True):
                                        cell = ws_books.find(row["Titel"])
                                        ws_books.update_cell(cell.row, 8, "Gelesen")
                                        ws_books.update_cell(cell.row, 6, datetime.now().strftime("%Y-%m-%d"))
                                        cleanup_author_duplicates_batch(ws_books, ws_authors)
                                        del st.session_state.df_books; st.rerun()
                                with c_info:
                                    st.subheader(row["Titel"])
                                    st.caption(row["Autor"])
                                    current_n = row["Notiz"]
                                    new_n = st.text_area("Notiz", value=current_n, key=f"wnote_{idx}", label_visibility="collapsed", height=80, placeholder="Notiz...")
                                    if new_n != current_n:
                                        update_single_entry(ws_books, row["Titel"], "Notiz", new_n)
                                        st.toast("Gespeichert!"); del st.session_state.df_books; time.sleep(0.5); st.rerun()
                else:
                    for i, r in df_w.iterrows():
                        c1, c2, c3, c4 = st.columns([1,3,1,1])
                        c1.image(r["Cover"] if r["Cover"]!="-" else "https://via.placeholder.com/50", width=50)
                        c2.write(f"**{r['Titel']}**\n{r['Autor']}")
                        if c3.button("ℹ️ Info", key=f"wld_{i}"): open_detail_view(r)
                        if c4.button("✅ Gelesen", key=f"wl_{i}"):
                            cell = ws_books.find(r["Titel"])
                            ws_books.update_cell(cell.row, 8, "Gelesen")
                            ws_books.update_cell(cell.row, 6, datetime.now().strftime("%Y-%m-%d"))
                            cleanup_author_duplicates_batch(ws_books, ws_authors)
                            del st.session_state.df_books; st.rerun()
            else: st.info("Merkliste leer.")

        with tab_stats:
            st.header("📊 Deine Lesestatistik")
            df_read = df[df["Status"] != "Wunschliste"]
            df_wish = df[df["Status"] == "Wunschliste"]
            c1, c2, c3 = st.columns(3)
            c1.metric("Gelesene Bücher", len(df_read))
            c2.metric("Auf der Wunschliste", len(df_wish))
            top_author = "Noch niemand"
            if not df_read.empty:
                top_author = df_read["Autor"].mode()[0]
                count = df_read[df_read["Autor"]==top_author].shape[0]
                c3.metric("Lieblingsautor", top_author, f"{count} Bücher")
            st.markdown("---")
            if not df_read.empty:
                auth_counts = df_read["Autor"].value_counts().reset_index()
                auth_counts.columns = ["Autor", "Anzahl Bücher"]
                st.dataframe(auth_counts, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
