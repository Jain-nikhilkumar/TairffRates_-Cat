import streamlit as st
import pandas as pd
import duckdb
import io
from datetime import datetime

# --- APP CONFIGURATION ---
st.set_page_config(
    page_title="CORE | Tariff Intelligence",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- NEON ENTERPRISE CSS ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap');

    /* Global Transitions */
    * { font-family: 'Outfit', sans-serif; transition: all 0.2s ease; }

    /* Dark Theme Background */
    .stApp {
        background: radial-gradient(circle at 50% 50%, #1a1f2c 0%, #0d1117 100%);
        color: #e6edf3;
    }

    /* Professional Sidebar */
    [data-testid="stSidebar"] {
        background-color: #0d1117 !important;
        border-right: 1px solid #30363d;
    }

    /* Glassmorphism Cards */
    .glass-card {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 15px;
        padding: 10px;
        margin-bottom: 15px;
        backdrop-filter: blur(10px);
    }

    /* Gradient Title */
    .gradient-text {
        background: linear-gradient(90deg, #00f2ff, #0072ff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
        font-size: 48px;
        letter-spacing: -1px;
    }

    /* Tab Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        background-color: transparent;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: rgba(255,255,255,0.05);
        border-radius: 8px 8px 0 0;
        padding: 10px 25px;
        color: #8b949e;
    }
    .stTabs [data-baseweb="tab--active"] {
        background-color: rgba(0, 242, 255, 0.1);
        color: #00f2ff !important;
        border-bottom: 2px solid #00f2ff !important;
    }

    /* Neon Buttons */
    .stButton>button {
        background: linear-gradient(90deg, #0072ff 0%, #00c6ff 100%) !important;
        color: white !important;
        border: none !important;
        font-weight: 600 !important;
        border-radius: 10px !important;
        padding: 12px 30px !important;
        box-shadow: 0 4px 15px rgba(0, 114, 255, 0.3);
    }
    .stButton>button:hover {
        box-shadow: 0 6px 25px rgba(0, 242, 255, 0.5);
        transform: translateY(-2px);
    }

    /* Service Status Pulse */
    .status-pulse {
        height: 10px; width: 10px;
        background-color: #00ff88;
        border-radius: 50%;
        display: inline-block;
        margin-right: 10px;
        box-shadow: 0 0 10px #00ff88;
        animation: pulse 1.5s infinite;
    }
    @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.4; } 100% { opacity: 1; } }
    </style>
    """, unsafe_allow_html=True)



def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Template')
    return output.getvalue()

@st.cache_resource
def get_engine():
    con = duckdb.connect(database=':memory:')
    con.execute("CREATE VIEW data AS SELECT * FROM 'master_data.parquet'")
    return con

db = get_engine()

# --- SIDEBAR NAV ---
with st.sidebar:
    st.markdown("<div class='status-pulse'></div><b>SYSTEM ONLINE</b>", unsafe_allow_html=True)
    st.caption("Latency: 14ms | Region: Global")
    st.markdown("---")

    # st.markdown("<h2 style='color:#00f2ff;'>⚡ CORE ENGINE</h2>", unsafe_allow_html=True)
    # st.markdown("<br>", unsafe_allow_html=True)
    
    rows = db.execute("SELECT COUNT(*) FROM data").fetchone()[0]
    st.write(f"📂 **Database:** {rows:,} Records")
    st.markdown("---")
    
   

# --- MAIN PAGE ---
st.markdown('<h1 class="gradient-text">🚢 Tariff Intelligence Portal</h1>', unsafe_allow_html=True)
st.markdown("<p style='color:#8b949e; font-size:18px;'>Enterprise Data Mapping & Hierarchical Compliance Engine</p>", unsafe_allow_html=True)

t1, t2 = st.tabs(["🔍 DISCOVERY EXPLORER", "📤 BULK EXTRACTION"])

# --- TAB 1: DISCOVERY ---
with t1:
    st.markdown("### 🔍 Filter")
    
    # 1. Advanced Configuration (Column Selection)
    with st.expander("⚙️ View Settings (Select Columns to Display/Export)"):
        # Get all column names from the master data
        all_master_cols = db.execute("DESCRIBE data").df()['column_name'].tolist()
        
        # Allow user to pick columns (Defaulting to the most important ones)
        view_cols = st.multiselect(
            "Select Columns:", 
            options=all_master_cols,
            default=["HS Code", "Country", "L1", "L2", "L3", "L4", "Revised Reciprocal Tariffs (To go in effect on 01-Aug-2025)"]
        )

    # 2. Filtering Logic
    st.write(f"**Hierarchical Drill-Down**")
    c1, c2, c3, c4 = st.columns(4) 
    
    l1_list = db.execute("SELECT DISTINCT L1 FROM data WHERE L1 IS NOT NULL ORDER BY L1").df()['L1'].tolist()
    s_l1 = c1.selectbox("Market Segment", ["All Segments"] + l1_list)

    l2_q = f"SELECT DISTINCT L2 FROM data WHERE L1='{s_l1}' ORDER BY L2" if s_l1 != "All Segments" else "SELECT DISTINCT L2 FROM data ORDER BY L2"
    s_l2 = c2.selectbox("Product Group", ["All Groups"] + db.execute(l2_q).df()['L2'].tolist())

    l3_q = f"SELECT DISTINCT L3 FROM data WHERE L2='{s_l2}' ORDER BY L3" if s_l2 != "All Groups" else "SELECT DISTINCT L3 FROM data ORDER BY L3"
    s_l3 = c3.selectbox("Product Class", ["All Classes"] + db.execute(l3_q).df()['L3'].tolist())

    l4_q = f"SELECT DISTINCT L4 FROM data WHERE L3='{s_l3}' ORDER BY L4" if s_l3 != "All Classes" else "SELECT DISTINCT L4 FROM data ORDER BY L4"
    s_l4 = c4.selectbox("Specific Item", ["All Items"] + db.execute(l4_q).df()['L4'].tolist())

    search = st.text_input("⚡ Global Search", placeholder="Enter HS Code, Country, or Keyword...")
    st.markdown('</div>', unsafe_allow_html=True)

    # 3. Dynamic SQL Query Builder
    # We use double quotes around column names to handle spaces/special characters
    selected_cols_sql = ", ".join([f'"{c}"' for c in view_cols]) if view_cols else "*"
    
    base_query = f"SELECT {selected_cols_sql} FROM data WHERE 1=1"
    
    if s_l1 != "All Segments": base_query += f" AND L1 = '{s_l1}'"
    if s_l2 != "All Groups": base_query += f" AND L2 = '{s_l2}'"
    if s_l3 != "All Classes": base_query += f" AND L3 = '{s_l3}'"
    if s_l4 != "All Items": base_query += f" AND L4 = '{s_l4}'"
    if search: 
        base_query += f" AND (CAST(\"HS Code\" AS VARCHAR) LIKE '%{search}%' OR \"Country\" LIKE '%{search}%')"

    # 4. Result Preview (Limited to 2000 for speed)
    data_preview = db.execute(base_query + " LIMIT 1000").df()
    
    # 5. UI for Results & Download
    col_stat, col_dl = st.columns([3, 1])
    with col_stat:
        st.markdown(f"**Showing {len(data_preview)} matching records in preview.**")
    
    with col_dl:
        # For the download, we execute the query WITHOUT the 2000 limit
        if st.button("📊 Prepare Full Export"):
            full_data = db.execute(base_query).df()
            excel_data = to_excel(full_data)
            st.download_button(
            label="📥 Download Results (Excel)",
            data=excel_data,
            file_name=f"Filtered_Tariff_Data_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    st.dataframe(data_preview, use_container_width=True, height=500)

# --- TAB 2: BULK ENGINE ---
# --- TAB 2: BULK ENGINE ---
with t2:
    # --- HEADER / INSTRUCTIONS ---
    st.markdown("""
        <div class="glass-card">
            <h5 style='color:#00f2ff; margin-bottom:1px;'>🚀 Bulk Extraction Engine</h5>
            <p style='color:#8b949e; font-size:11x;'>
                Upload your dataset to map <b>L1-L4 Hierarchies</b> and <b>Reciprocal Tariffs</b> automatically. 
                The engine uses <i>Fuzzy Column Detection</i> to find your HS Codes and Countries even if they are named differently.
            </p>
        </div>
    """, unsafe_allow_html=True)

    left, right = st.columns([1, 2], gap="large")

    # --- LEFT COLUMN: CONTROL PANEL ---
    with left:
        st.markdown("<p class='step-header'>STEP 1: PREPARATION</p>", unsafe_allow_html=True)
        with st.container():
            st.info("💡 Your file must contain at least an **HS Code** column. Including a **Country** column enables territory-specific tariff mapping.")
            st.download_button(
            label="📥 Download Reference Template",
            data=to_excel(pd.DataFrame(columns=["HS Code", "Country"])),
            file_name="core_mapping_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        st.markdown("<br><p class='step-header'>STEP 2: DATA UPLOAD</p>", unsafe_allow_html=True)
        uploaded = st.file_uploader("Drop CSV or XLSX file here", type=['xlsx', 'csv'], label_visibility="collapsed")
        
        if uploaded:
            st.markdown("<br><p class='step-header'>STEP 3: ENRICHMENT CONFIG</p>", unsafe_allow_html=True)
            all_cols = db.execute("DESCRIBE data").df()['column_name'].tolist()
            
            # Smart defaults for the user
            to_fetch = st.multiselect(
                "Select Fields to Extract:", 
                options=all_cols, 
                default=["HS Code", "Country", "L1", "L2", "L3", "L4", "Revised Reciprocal Tariffs (To go in effect on 01-Aug-2025)"],
                help="Choose which columns from the master database you want to add to your file."
            )
            
            st.markdown("<br>", unsafe_allow_html=True)
            run_btn = st.button("🚀 INITIATE EXTRACTION ENGINE")

    # --- RIGHT COLUMN: PROCESSING & PREVIEW ---
    with right:
        if uploaded and run_btn:
            # Create a professional processing area
            st.markdown("<p class='step-header'>ENGINE STATUS & PREVIEW</p>", unsafe_allow_html=True)
            
            with st.status("🔮 Analyzing Schema & Mapping Data...", expanded=True) as status:
                try:
                    # 1. Load Data
                    if uploaded.name.endswith('xlsx'):
                        df_in = pd.read_excel(uploaded)
                    else:
                        df_in = pd.read_csv(uploaded)
                    
                    # 2. Fuzzy Column Matching Logic
                    def find_col(possible_names, actual_cols):
                        for p in possible_names:
                            for a in actual_cols:
                                if p.upper() in str(a).upper(): return a
                        return None

                    hs_key = find_col(["HS", "CODE", "COMMODITY"], df_in.columns)
                    ct_key = find_col(["COUNTRY", "NATION", "ORIGIN", "CTRY"], df_in.columns)

                    if not hs_key:
                        st.error("❌ **Critical Failure:** No 'HS Code' column detected. Please check your file headers.")
                        st.stop()

                    # 3. Transparent Feedback
                    status.write(f"🔎 Detected **'{hs_key}'** as primary HS Key.")
                    if ct_key:
                        status.write(f"🌍 Detected **'{ct_key}'** as Country Key. Enabling territory-specific tariffs.")
                    else:
                        status.write("⚠️ No Country detected. Defaulting to unique HS Code mapping.")
                    # 4. Show Matching Type to User
                    if ct_key:
                        status.write("🔒 Using STRICT HS+Country match per row")
                    else:
                        status.write("⚡ Using ROW-WISE prefix match for HS Code based on input length")
                                        # 4. Data Standardization
                    
                    df_in[hs_key] = df_in[hs_key].astype(str).str.strip()
                    # Create a column with HS code length per row
                    df_in['HS_LEN'] = df_in[hs_key].str.len()
                    if ct_key:
                        df_in[ct_key] = df_in[ct_key].astype(str).str.strip()
                    
                    db.register('upload_data', df_in)

                    # 5. Build Dynamic SQL
                    if ct_key:
                        # STRICT HS + Country match
                        # Only select uploaded columns + user-selected enrichment columns
                        enrich_cols_sql = ", ".join([f'm."{c}"' for c in to_fetch if c not in df_in.columns])

                        sql = f"""
                            SELECT u.*, {enrich_cols_sql}
                            FROM upload_data u
                            LEFT JOIN data m
                            ON u."{hs_key}" = m."HS Code"
                            AND u."{ct_key}" = m."Country"
                        """
                    else:
                        # ROW-WISE PREFIX MATCH for HS only
                        enrich_cols_sql = ", ".join([f'm."{c}"' for c in to_fetch if c != "HS Code"])

                        sql = f"""
                            SELECT *
                            FROM (
                                SELECT 
                                    u.*, 
                                    {enrich_cols_sql},
                                    ROW_NUMBER() OVER (
                                        PARTITION BY u."{hs_key}" 
                                        ORDER BY LENGTH(m."HS Code") DESC
                                    ) as rn
                                FROM upload_data u
                                LEFT JOIN data m
                                ON LEFT(m."HS Code", u.HS_LEN) = u."{hs_key}"
                            ) t
                            WHERE rn = 1
                        """
                    # 6. Execute and Fill Missing
                    final_res = db.execute(sql).df().fillna("N/A")
                    
                    status.update(label="✅ EXTRACTION SUCCESSFUL", state="complete", expanded=False)
                    
                    # --- SUCCESS UI ---
                    st.success(f"Successfully processed {len(final_res)} rows.")
                    
                    st.dataframe(
                        final_res.head(100), 
                        use_container_width=True, 
                        height=350
                    )
                    
                    # Custom Download Section
                    st.markdown("### 💾 Finalized Report")
                    st.write("Your enriched file is ready for download.")
                    excel_data = to_excel(final_res)
                    st.download_button(
                    label="📥 DOWNLOAD ENRICHED DATASET (EXCEL)",
                    data=excel_data,
                    file_name=f"CORE_Enriched_Report_{datetime.now().strftime('%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
                except Exception as e:
                    st.error(f"Engine Failure: {str(e)}")
                    st.info("Check if your Excel file has empty headers or hidden sheets.")
        
        else:
            # Placeholder for when no file is processed
            st.markdown(f"""
                <div style="height:450px; display:flex; flex-direction:column; align-items:center; justify-content:center; border:1px dashed rgba(255,255,255,0.1); border-radius:15px; background: rgba(255,255,255,0.02);">
                    <img src="https://cdn-icons-png.flaticon.com/512/1167/1167092.png" width="80" style="opacity:0.3; filter: grayscale(1);">
                    <p style="color:#484f58; font-size:18px; margin-top:20px;">Awaiting Dataset Input</p>
                    <p style="color:#30363d; font-size:14px;">Configure and run the engine on the left to see results.</p>
                </div>
            """, unsafe_allow_html=True)