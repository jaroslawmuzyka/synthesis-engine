import streamlit as st
import pandas as pd
import io

# Konfiguracja
st.set_page_config(
    page_title="MediaMarkt SEO Analyzer",
    page_icon="🔍",
    layout="wide"
)

# --- MODUŁ LOGOWANIA ---
def check_password():
    """Zwraca `True` jeśli użytkownik podał poprawne hasło."""

    def password_entered():
        if st.session_state["password"] == st.secrets["APP_PASSWORD"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"] 
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Podaj hasło dostępu:", type="password", on_change=password_entered, key="password")
        st.info("Dane potrzebne do zalogowania znajdują się w Monday. Kontakt: jaroslaw.muzyka@performance-group.pl")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Podaj hasło dostępu:", type="password", on_change=password_entered, key="password")
        st.error("😕 Niepoprawne hasło")
        st.info("Dane potrzebne do zalogowania znajdują się w Monday. Kontakt: jaroslaw.muzyka@performance-group.pl")
        return False
    else:
        return True

if not check_password():
    st.stop()

# --- LOGIKA APLIKACJI ---
def get_recommendation(row):
    rank_val = row.get('rank_group')
    has_rank = pd.notnull(rank_val) and str(rank_val).strip() != ''
    
    try:
        rank = float(rank_val)
    except:
        rank = 999
        
    if has_rank and rank <= 10:
        if rank <= 3:
            return "Brak działania"
        elif rank <= 10:
            return "Do optymalizacji"
            
    # Jeżeli brak w TOP 10 wg zaleceń - mapowanie wprost z MM_Asset_Type
    asset_type = str(row.get('MM_Asset_Type', '')).strip()
    # Ewentualnie gdyby brakowało tej kolumny, oddajemy Brak Danych
    return asset_type if asset_type and asset_type.lower() != 'nan' else "Brak Danych"

def get_footprint(row):
    keyword = str(row.get('Keyword', '')).strip()
    asset_type = str(row.get('MM_Asset_Type', '')).strip().lower()
    
    if 'list' in asset_type or 'tematyczny' in asset_type or 'cenowa' in asset_type:
        path = "list/"
    elif 'kategori' in asset_type or 'filtr' in asset_type:
        path = "category/"
    elif 'content' in asset_type:
        path = "content/"
    else:
        path = "inne"
        
    return f"{keyword} site:https://mediamarkt.pl/pl/{path}"

@st.cache_data(show_spinner=False)
def process_data(ahrefs_bytes, ahrefs_name, serp_bytes, serp_name):
    # 1. Odczyt Ahrefs
    if ahrefs_name.endswith('.csv'):
        try:
            df_ahrefs = pd.read_csv(io.BytesIO(ahrefs_bytes), sep=None, engine='python')
        except:
            df_ahrefs = pd.read_csv(io.BytesIO(ahrefs_bytes), sep=';')
    else:
        df_ahrefs = pd.read_excel(io.BytesIO(ahrefs_bytes))
        
    # 2. Odczyt SERP (zakładka Clean Data)
    try:
        df_serp = pd.read_excel(io.BytesIO(serp_bytes), sheet_name='Clean Data')
    except Exception as e:
        return None, f"Błąd podczas analizowania zakładki 'Clean Data' w pliku SERP: {e}"
    
    df_serp.columns = [str(c).strip().lower() for c in df_serp.columns]
    
    # 3. Znalezienie nazwy dla fraz w Ahrefs
    ahrefs_keyword_col = None
    for col in df_ahrefs.columns:
        if str(col).strip().lower() == 'keyword':
            ahrefs_keyword_col = col
            break
            
    if not ahrefs_keyword_col:
        return None, "Plik Ahrefs nie zawiera jednoznacznej kolumny 'Keyword'."
        
    if 'keyword' not in df_serp.columns or 'type' not in df_serp.columns or 'domain' not in df_serp.columns:
        return None, "Plik SERP nie posiada minimalnego wymogu kolumn ('keyword', 'type', 'domain')."
        
    # 4. Fitracja SERPu (mediamarkt.pl, organic)
    df_mm = df_serp[(df_serp['type'] == 'organic') & (df_serp['domain'].astype(str).str.contains('mediamarkt.pl', na=False, case=False))]
    
    rank_col = 'rank_group' if 'rank_group' in df_serp.columns else 'rank_absolute'
    if rank_col not in df_serp.columns:
        return None, "W pliku SERP brakuje kolumny z pozycją (np. rank_group / rank_absolute)."
        
    # Czyste konwersje na liczby
    df_mm = df_mm.copy()
    df_mm[rank_col] = pd.to_numeric(df_mm[rank_col], errors='coerce')
    
    # Sortowanie by wziąć to, co ma najniższy (najlepszy) rank
    df_mm_best = df_mm.sort_values(rank_col).drop_duplicates('keyword', keep='first')
    
    # Ustalamy zakres łączenia
    url_col = 'url' if 'url' in df_serp.columns else 'url_absolute'
    join_cols = ['keyword', rank_col]
    if url_col in df_mm_best.columns:
        join_cols.append(url_col)
        
    df_mm_selected = df_mm_best[join_cols].rename(columns={rank_col: 'rank_group'}) # Nadajemy generyczną nazwę by get_recommendation jej chwycił
    
    # Unifikacja wielkości znaków i spacji by LEFT JOIN był w 100% poprawny
    df_ahrefs['_join_key'] = df_ahrefs[ahrefs_keyword_col].astype(str).str.strip().str.lower()
    df_mm_selected['_join_key'] = df_mm_selected['keyword'].astype(str).str.strip().str.lower()
    
    final_df = df_ahrefs.merge(df_mm_selected, on='_join_key', how='left')
    final_df = final_df.drop(columns=['_join_key', 'keyword'], errors='ignore')
    
    # 5. Generacja wynikowych danych
    final_df['Rekomendacja'] = final_df.apply(get_recommendation, axis=1)
    final_df['Szukana Fraza SERP'] = final_df.apply(get_footprint, axis=1)
    
    # Zmiana nazw kolumn z SERPa dla większej czytelności 
    rename_map = {'rank_group': 'Pozycja_MM', url_col: 'URL_MM'}
    final_df = final_df.rename(columns=rename_map)
    
    return final_df, None

# --- UI (INTERFEJS UŻYTKOWNIKA) ---

st.title("🚀 MediaMarkt SEO Analyzer")
st.markdown("""
Aplikacja agreguje dane z pobranego zestawienia **Ahrefs** oraz **SERP Snapshot**, odnajduje pozycjonowanie na organicznych wynikach dla domeny *mediamarkt.pl* i wyznacza ostateczne wytyczne i rekomendacje do działań optymalizacyjnych.
""")

with st.expander("📖 Instrukcja"):
    st.markdown("""
    1. Wgraj plik **Ahrefs (.csv, .xlsx)** z danymi i kategoryzacją `MM_Asset_Type`. Należy upewnić się, że plik ma kolumnę z frazą ('Keyword').
    2. Wgraj zeszyt z SERPami, plik **SERP Snapshot (.xlsx)**, zawierający zakładkę `Clean Data`.
    3. Kliknij "Uruchom analizę" i pobierz połączone dane z rekomendacjami w xlsx.
    """)

col1, col2 = st.columns(2)
with col1:
    ahrefs_file = st.file_uploader("Wgraj główny plik Ahrefs (.csv, .xlsx)", type=['csv', 'xlsx'])
with col2:
    serp_file = st.file_uploader("Wgraj plik SERP Snapshot (.xlsx)", type=['xlsx'])

if ahrefs_file is not None and serp_file is not None:
    if st.button("Uruchom analizę", type="primary"):
        with st.spinner("Łączenie i mapowanie plików oraz weryfikacja SERP... Może to zająć kilka chwil!"):
            ahrefs_bytes = ahrefs_file.getvalue()
            serp_bytes = serp_file.getvalue()
            
            result_df, error = process_data(ahrefs_bytes, ahrefs_file.name, serp_bytes, serp_file.name)
            
            if error:
                st.error(f"Wystąpił błąd podczas analizy: {error}")
            else:
                st.success(f"Analiza pomyślna! Przetworzono z sukcesem {len(result_df)} fraz z raportu Ahrefs.")
                
                # Szybkie statystyki
                st.subheader("Bieżące statystyki widoczności / działania MediaMarkt (TOP10)")
                
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Wszystkie frazy", len(result_df))
                
                brak_dzialania = len(result_df[result_df['Rekomendacja'] == 'Brak działania'])
                do_optymalizacji = len(result_df[result_df['Rekomendacja'] == 'Do optymalizacji'])
                poza_top10 = len(result_df) - brak_dzialania - do_optymalizacji
                
                c2.metric("🟢 Brak działania (TOP 1-3)", brak_dzialania)
                c3.metric("🟡 Do optymalizacji (TOP 4-10)", do_optymalizacji)
                c4.metric("🔴 Inne działania (Brak w Top 10)", poza_top10)
                
                # Podgląd fragmentu bazy
                st.subheader("Podgląd wyników analizy")
                
                # Nakładanie delikatnych kolorów z podobnego narzędzia URL Matcher
                def style_recommendation(val):
                    if val == "Brak działania":
                        return 'background-color: rgba(46, 204, 113, 0.2); font-weight: bold;'
                    elif val == "Do optymalizacji":
                        return 'background-color: rgba(241, 196, 15, 0.2);'
                    elif val and isinstance(val, str):
                        return 'background-color: rgba(231, 76, 60, 0.1); color: #c0392b;'
                    return ''
                    
                styled_df = result_df.head(100).style.map(style_recommendation, subset=['Rekomendacja'])
                st.dataframe(styled_df, use_container_width=True)
                
                # Eksport do excela
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    result_df.to_excel(writer, index=False, sheet_name='Analiza SEO')
                
                st.download_button(
                    label="📥 Pobierz finalne zestawienie (.xlsx)",
                    data=buffer.getvalue(),
                    file_name="MediaMarkt_SEO_Rekomendacje.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
