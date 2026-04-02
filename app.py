import streamlit as st
import pandas as pd
import io

# Konfiguracja
st.set_page_config(
    page_title="MediaMarkt SEO Analytics",
    page_icon="📈",
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
    rank_val = row.get('rank_absolute')
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
        sample_text = ahrefs_bytes[:2048].decode('utf-8', errors='ignore')
        sep = ';' if sample_text.count(';') > sample_text.count(',') else ','
        df_ahrefs = pd.read_csv(io.BytesIO(ahrefs_bytes), sep=sep, engine='c')
    else:
        df_ahrefs = pd.read_excel(io.BytesIO(ahrefs_bytes))
        
    # 2. Odczyt SERP
    def needed_cols(col_name):
        c = str(col_name).strip().lower()
        return c in ['keyword', 'type', 'domain', 'rank_group', 'rank_absolute', 'url', 'url_absolute']
    
    try:
        df_serp = pd.read_excel(
            io.BytesIO(serp_bytes), 
            sheet_name='Clean Data',
            usecols=needed_cols
        )
    except Exception as e:
        return None, None, None, f"Błąd podczas analizowania zakładki 'Clean Data' w pliku SERP: {e}"
    
    df_serp.columns = [str(c).strip().lower() for c in df_serp.columns]
    
    ahrefs_keyword_col = None
    for col in df_ahrefs.columns:
        if str(col).strip().lower() == 'keyword':
            ahrefs_keyword_col = col
            break
            
    if not ahrefs_keyword_col:
        return None, None, None, "Plik Ahrefs nie zawiera jednoznacznej kolumny 'Keyword'."
        
    if 'keyword' not in df_serp.columns or 'type' not in df_serp.columns or 'domain' not in df_serp.columns:
        return None, None, None, "Plik SERP nie posiada kolumn (keyword, type, domain)."
        
    if 'rank_absolute' not in df_serp.columns:
        return None, None, None, "Plik SERP wygenerował błąd braku wymaganej kolumny 'rank_absolute'."

    # --- GENERACJA WIDOKU SERP DLA ANALITYKI ZANIM GO WYTNIEMY --
    global_serp_types = df_serp['type'].value_counts().reset_index()
    global_serp_types.columns = ['Typ SERP', 'Ilość Wystąpień']
    
    # 3. Zmodyfikowana generacja wykrojów per analiza SERP (dla Zakładki 2)
    df_ahrefs['_join_key'] = df_ahrefs[ahrefs_keyword_col].astype(str).str.strip().str.lower()
    df_serp['_join_key'] = df_serp['keyword'].astype(str).str.strip().str.lower()
    
    kat_cols = ['L1_Stage', 'L2_Intent', 'L3_MM_Segment', 'MM_Action', 'MM_Asset_Type']
    
    # Merge tylko dla kolumn potrzebnych do wykresów (oszczędzanie RAM)
    cols_to_extract = ['_join_key'] + [c for c in kat_cols if c in df_ahrefs.columns]
    df_serp_mapped = df_serp.merge(df_ahrefs[cols_to_extract], on='_join_key', how='inner')
    
    serp_cat_analytics = {}
    for col in kat_cols:
        if col in df_serp_mapped.columns:
            grouped_serp = df_serp_mapped.groupby([col, 'type']).size().reset_index(name='Ilość Wystąpień')
            serp_cat_analytics[col] = grouped_serp

    # 4. Fitracja SERPu i przesuwanie wyłącznie do MM, i tylko organic dla głównej analizy
    df_mm = df_serp[(df_serp['type'] == 'organic') & (df_serp['domain'].astype(str).str.contains('mediamarkt.pl', na=False, case=False))].copy()
    
    # Upewniamy się, że to integer!
    df_mm['rank_absolute'] = pd.to_numeric(df_mm['rank_absolute'], errors='coerce')
    
    # Sortowanie by wziąć to, co ma najniższy rank_absolute
    df_mm_best = df_mm.sort_values('rank_absolute').drop_duplicates('keyword', keep='first')
    
    url_col = 'url' if 'url' in df_serp.columns else 'url_absolute'
    join_cols = ['keyword', 'rank_absolute']
    if url_col in df_mm_best.columns:
        join_cols.append(url_col)
        
    df_mm_selected = df_mm_best[join_cols]
    
    # Dołączanie - formatowanie
    df_mm_selected['_join_key'] = df_mm_selected['keyword'].astype(str).str.strip().str.lower()
    
    final_df = df_ahrefs.merge(df_mm_selected, on='_join_key', how='left')
    final_df = final_df.drop(columns=['_join_key', 'keyword'], errors='ignore')
    
    # Logika dopasowań
    final_df['Rekomendacja'] = final_df.apply(get_recommendation, axis=1)
    final_df['Szukana Fraza SERP'] = final_df.apply(get_footprint, axis=1)
    
    # Optymalizacja wyświetlania do pełnych liczb całkowitych (typ Int64)
    final_df['Pozycja_MM'] = pd.to_numeric(final_df['rank_absolute'], errors='coerce').astype('Int64')
    
    # Likwidacja brudnych kolumn i estetyczne nazewnictwo
    final_df = final_df.drop(columns=['rank_absolute'], errors='ignore')
    if url_col in final_df.columns:
        final_df = final_df.rename(columns={url_col: 'URL_MM'})
    
    return final_df, global_serp_types, serp_cat_analytics, None

# --- UI (INTERFEJS UŻYTKOWNIKA) ---

st.title("🚀 MediaMarkt Wszechstronny Dashboard SEO")
st.markdown("""
Aplikacja agreguje ogromne zbiory z Ahrefs i wyników SERP, łącząc je ze sobą na podstawie absolutnego rankingu dla środowiska domeny `mediamarkt.pl`. Dostarcza szczegółowe widoki danych i masę analiz podsumowujących.
""")

with st.sidebar:
    st.header("📂 Wgrywanie Plików")
    ahrefs_file = st.file_uploader("Raport Ahrefs (.csv, .xlsx)", type=['csv', 'xlsx'])
    serp_file = st.file_uploader("SERP Snapshot (.xlsx)", type=['xlsx'])
    
    if ahrefs_file and serp_file:
        run_btn = st.button("Uruchom Opracowanie Danych", type="primary", use_container_width=True)
    else:
        run_btn = False

if run_btn and ahrefs_file and serp_file:
    with st.spinner("Przerabianie plików binarie w chmurze (to darmowy serwer, może to potrwać dłuższą chwilę)..."):
        ahrefs_bytes = ahrefs_file.getvalue()
        serp_bytes = serp_file.getvalue()
        
        result_df, serp_analytics_df, serp_cat_analytics, error = process_data(ahrefs_bytes, ahrefs_file.name, serp_bytes, serp_file.name)
        
        if error:
            st.error(f"Wystąpił błąd układania danych: {error}")
        else:
            st.session_state['ready_data'] = result_df
            st.session_state['serp_analytics'] = serp_analytics_df
            st.session_state['serp_cat_analytics'] = serp_cat_analytics
            st.session_state['processed'] = True
            st.rerun()

if st.session_state.get('processed', False):
    st.success("✅ Kompilacja potężnej bazy danych przebiegła z sukcesem!")
    
    # Inicjalizacja ZAKŁADEK
    tab_main, tab_insights, tab_charts, tab_asset, tab_data = st.tabs([
        "📊 Dashboard Główny", 
        "💡 Wnioski Biznesowe",
        "📈 Analizy Złożone", 
        "📑 Segmenty MM_Asset_Type",
        "🗄️ Baza Danych (Interaktywna)"
    ])
    
    result_df = st.session_state['ready_data']
    
    # Stylowanie do DataFrame (wyrzucamy formatowanie ułamków na Pozycja_MM!)
    def style_dataframe(df):
        # Aplikacja gradientów na rekomendacjach
        def highlight_recommendation(val):
            if val == "Brak działania": return 'background-color: rgba(46, 204, 113, 0.2); font-weight: bold;'
            if val == "Do optymalizacji": return 'background-color: rgba(241, 196, 15, 0.2);'
            if val and isinstance(val, str): return 'background-color: rgba(231, 76, 60, 0.1); color: #c0392b;'
            return ''
        return df.style.map(highlight_recommendation, subset=['Rekomendacja'])
    
    with tab_main:
        st.subheader("Bieżące statystyki widoczności (TOP10)")
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Wielkość Analizy (Wszystkie Frazy)", len(result_df))
        
        brak_dzialania = len(result_df[result_df['Rekomendacja'] == 'Brak działania'])
        do_optymalizacji = len(result_df[result_df['Rekomendacja'] == 'Do optymalizacji'])
        poza_top10 = len(result_df) - brak_dzialania - do_optymalizacji
        
        c2.metric("🟢 Brak działania (TOP 1-3)", brak_dzialania)
        c3.metric("🟡 Do optymalizacji (TOP 4-10)", do_optymalizacji)
        c4.metric("🔴 Inne / Baza Klasyfikacji", poza_top10)
        
        st.markdown("---")
        
        # Przycisk pobierania umiejscowiony na Dashboardzie Głównym
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            result_df.to_excel(writer, index=False, sheet_name='Analiza SEO')
        
        st.download_button(
            label="📥 Mimo wszystko stąd pobierz Finalne Zestawienie (.xlsx)",
            data=buffer.getvalue(),
            file_name="MediaMarkt_SEO_Zestawienie_Analityczne.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    with tab_insights:
        st.header("Lejek Optymalizacyjny: Insight'y i Wnioski zebranych danych")
        st.markdown("Zgodnie z ułożeniem UX: **Gdzie jesteśmy? → Co poprawić w pierwszej kolejności? → Czego nam brakuje na rynku?**")
        
        vol_col = 'Volume' if 'Volume' in result_df.columns else None
        if vol_col:
            target_df = result_df.copy()
            target_df['_vol_calc'] = pd.to_numeric(target_df[vol_col], errors='coerce').fillna(0)
            
            total_vol = target_df['_vol_calc'].sum()
            vol_top3 = target_df[target_df['Rekomendacja'] == 'Brak działania']['_vol_calc'].sum()
            vol_opt = target_df[target_df['Rekomendacja'] == 'Do optymalizacji']['_vol_calc'].sum()
            vol_gap = target_df[~target_df['Rekomendacja'].isin(['Brak działania', 'Do optymalizacji'])]['_vol_calc'].sum()
            
            pct_top3 = vol_top3 / total_vol * 100 if total_vol > 0 else 0
            pct_opt = vol_opt / total_vol * 100 if total_vol > 0 else 0
            pct_gap = vol_gap / total_vol * 100 if total_vol > 0 else 0
            
            st.subheader("1. Gdzie jesteśmy? (Wolumen Rynkowy vs Udział MediaMarkt)")
            co1, co2, co3 = st.columns(3)
            co1.metric(f"🚀 Zabezpieczony Potencjał (TOP1-3)", f"{pct_top3:.1f} %", f"{vol_top3:,.0f} ruchu")
            co2.metric(f"🛠️ Wolumen do Optymalizacji (TOP4-10)", f"{pct_opt:.1f} %", f"{vol_opt:,.0f} ruchu")
            co3.metric(f"⚠️ Luka Rynkowa (Poza TOP10)", f"{pct_gap:.1f} %", f"{vol_gap:,.0f} ruchu", delta_color="inverse")
            
            st.markdown("---")
            
            st.subheader("2. Priorytetyzacja: Najszybsze Zwycięstwa (Low-Hanging Fruits)")
            st.write("Skup się najpierw na tych frazach. Znajdują się one zaledwie na pozycjach TOP4-TOP10, ale posiadają gigantyczny napęd na ruch `Volume`.")
            
            df_quick_wins = target_df[target_df['Rekomendacja'] == 'Do optymalizacji'].sort_values('_vol_calc', ascending=False)
            cols_to_show_qw = ['Keyword', 'Pozycja_MM', vol_col, 'MM_Asset_Type', 'L2_Intent', 'URL_MM']
            
            st.dataframe(df_quick_wins[[c for c in cols_to_show_qw if c in df_quick_wins.columns]].head(50), use_container_width=True)

            st.markdown("---")

            st.subheader("3. Luka Treści: Czego oczekuje rynek, a gdzie nas nie ma? (Brak w Top10)")
            st.write("Oto potężny rynkowy wolumen dla zapytań, których my organicznie nie obsługujemy. Spójrz na wymagane przez poszczególne frazy zasoby w kolumnach typów i przekazuj je ekspertom do wdrożeń.")
            
            df_gap = target_df[~target_df['Rekomendacja'].isin(['Brak działania', 'Do optymalizacji'])].sort_values('_vol_calc', ascending=False)
            cols_to_show_gap = ['Keyword', vol_col, 'MM_Asset_Type', 'Rekomendacja', 'Szukana Fraza SERP']
            
            st.dataframe(df_gap[[c for c in cols_to_show_gap if c in df_gap.columns]].head(100), use_container_width=True)
            
        else:
            st.error("Brak kolumny Volume do sporządzenia zaawansowanych wniosków biznesowych.")
            
    with tab_charts:
        st.header("Mikroskop SERP i Wykresy Intencji")
        
        # 1. Jakie typy zalewają badany rynek wyszukiwań
        st.subheader("Które kafelki dominują we wszystkich odczytanych plikach SERP?")
        st.bar_chart(st.session_state['serp_analytics'].set_index('Typ SERP'))
        
        st.markdown("---")
        st.subheader("Analizy wertykalne (Wolumeny i zbiory wg kategorii)")
        st.write("Wybierz odpowiednią klasyfikację z Ahrefs i spójrz jak dystrybuują się po niej zebrane zasoby wyszukiwań na rynku.")
        
        kat_cols = ['L1_Stage', 'L2_Intent', 'L3_MM_Segment', 'MM_Action', 'MM_Asset_Type']
        
        # Tworzy dedykowany podgląd zakładkami dla oszczędzenia okna SelectBoxa
        sub_tabs = st.tabs(kat_cols)
        
        # Rozbicie logiki na zadeklarowane subtabs
        for i, col in enumerate(kat_cols):
            with sub_tabs[i]:
                if col in result_df.columns:
                    vol_col = 'Volume' if 'Volume' in result_df.columns else None
                        
                    agg_dict = {'Keyword': 'count'}
                    if vol_col:
                        # Wymuszamy liczbowy format wolumenów dla poprawnego przeliczania (nie string np)
                        result_df[f'_numeric_{vol_col}'] = pd.to_numeric(result_df[vol_col], errors='coerce').fillna(0)
                        agg_dict[f'_numeric_{vol_col}'] = 'sum'
                            
                    grouped = result_df.groupby(col).agg(agg_dict).reset_index()
                    grouped = grouped.rename(columns={'Keyword': 'Ilość Rozpoznanych Zapuszczeń'})
                    
                    if vol_col:
                         grouped = grouped.rename(columns={f'_numeric_{vol_col}': 'Skumulowany Popyt / Volume'})
                    
                    col1, col2 = st.columns(2)
                    with col1:
                         st.write(f"📈 Suma wygenerowanych fraz ze względu na **{col}**:")
                         st.bar_chart(grouped.set_index(col)['Ilość Rozpoznanych Zapuszczeń'])
                    with col2:
                         if vol_col:
                             st.write(f"🔥 Ile potencjalnego Wolumenu skrywa dany podział **{col}**:")
                             st.bar_chart(grouped.set_index(col)['Skumulowany Popyt / Volume'])
                
                # Dodanie dystrybucji typów SERP per zakładka:
                st.write(f"📊 **Rozkład typów wyników w SERP dla podziału: {col}**")
                if col in st.session_state.get('serp_cat_analytics', {}):
                    df_types_grouped = st.session_state['serp_cat_analytics'][col]
                    if not df_types_grouped.empty:
                        # W st.bar_chart definiujemy X jako naszą kolumnę podziału, Y jako wolumeny, a Color uwypukla nam odrębną ścieżkę jako TYP z SERPU.
                        st.bar_chart(df_types_grouped, x=col, y='Ilość Wystąpień', color='type')
                    else:
                        st.info("Baza odcięła wyniki dopasowania.")
                else:
                    st.info("Brak danych krzyżowych SERP dla tej kolumny.")

    with tab_asset:
        st.header("Mikroskop Segmentów MM_Asset_Type")
        if 'MM_Asset_Type' in result_df.columns:
            segments = result_df['MM_Asset_Type'].dropna().unique().tolist()
            if segments:
                st.write("W poniższych zakładkach znajdziesz zawężone dane per wyselekcjonowany typ zasobu.")
                seg_tabs = st.tabs([str(s) for s in segments])
                for idx, segment_val in enumerate(segments):
                    with seg_tabs[idx]:
                        seg_df = result_df[result_df['MM_Asset_Type'] == segment_val]
                        count = len(seg_df)
                        vol_col = 'Volume' if 'Volume' in seg_df.columns else None
                        
                        cl1, cl2 = st.columns(2)
                        cl1.metric("Wyłapanych fraz w segmencie", count)
                        if vol_col:
                            total_vol = sum(pd.to_numeric(seg_df[vol_col], errors='coerce').fillna(0))
                            cl2.metric("Skumulowany Wolumen (Suma)", total_vol)
                        
                        st.dataframe(style_dataframe(seg_df), use_container_width=True)
            else:
                st.info("Brak przypisanych typów (MM_Asset_Type) w zebranej bazie.")
        else:
            st.error("Kolumna 'MM_Asset_Type' nie została odnaleziona w bazie.")
            
    with tab_data:
         st.header("Interaktywna Przeglądarka Tabelaryczna")
         st.markdown("Ten widżet to pełna baza pozbawiona restrykcji `na górne 100 wpisów`. Tabela obsługuje natywne strzałki kolumn (do sortowania) oraz lupkę w prawym górnym jej krańcu do dedykowanego wyszukiwania poszczególnych komórek.")
         
         s_df = style_dataframe(result_df)
         st.dataframe(s_df, use_container_width=True, height=750)
