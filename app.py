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

# --- ADAPTIVE ENTERPRISE CSS ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap');

    /* Global Transitions */
    * { font-family: 'Outfit', sans-serif; transition: all 0.2s ease; }

    /* Glassmorphism Card - ADAPTIVE */
    .glass-card {
        background: rgba(120, 120, 120, 0.05);
        border: 1px solid rgba(120, 120, 120, 0.2);
        border-radius: 12px;
        padding: 12px;
        margin-bottom: 10px;
        backdrop-filter: blur(10px);
    }

    /* HIGH-CONTRAST DATAFRAME FIX */
    [data-testid="stDataFrame"] {
        background: transparent !important;
    }

    /* Force a Deep Blue hover - Visible on both Light and Dark */
    [data-testid="stDataFrame"] div[role="gridcell"]:hover {
        background-color: #1E3A8A !important; /* Deep Navy Blue */
        color: #FFFFFF !important;           /* Force White Text on hover */
    }

    /* Standard cell text - Adaptive (White in Dark / Black in Light) */
    [data-testid="stDataFrame"] div[role="gridcell"] {
        color: var(--text-color) !important;
    }

    /* Column Header Contrast Fix */
    [data-testid="stDataFrame"] div[role="columnheader"] {
        background-color: rgba(120, 120, 120, 0.15) !important;
        color: var(--text-color) !important;
        font-weight: 800 !important;
    }

    /* Scrollbar Visibility for Dark/Light */
    ::-webkit-scrollbar-thumb {
        background: #0072ff !important;
        border-radius: 10px;
    }
    /* Gradient Title - Works on both */
    .gradient-text {
        background: linear-gradient(90deg, #0072ff, #00c6ff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
        font-size: 42px;
        letter-spacing: -1px;
    }

    /* Tab Styling Fixes */
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding: 10px 20px;
        font-weight: 600;
    }

    /* Neon Buttons */
    .stButton>button {
        background: linear-gradient(90deg, #0072ff 0%, #00c6ff 100%) !important;
        color: white !important;
        border: none !important;
        font-weight: 600 !important;
        border-radius: 8px !important;
        padding: 10px 24px !important;
        box-shadow: 0 4px 12px rgba(0, 114, 255, 0.2);
    }
    
    /* Ensure Sidebar text remains legible */
    [data-testid="stSidebar"] {
        border-right: 1px solid rgba(120,120,120,0.2);
    }

    /* Step Header Styling */
    .step-header {
        font-size: 0.8rem;
        font-weight: 800;
        color: #0072ff;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 5px;
    }

    /* Service Status Pulse */
    .status-pulse {
        height: 10px; width: 10px;
        background-color: #00ff88;
        border-radius: 50%;
        display: inline-block;
        margin-right: 8px;
        box-shadow: 0 0 8px #00ff88;
        animation: pulse 1.5s infinite;
    }
    @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.4; } 100% { opacity: 1; } }
    </style>
    """, unsafe_allow_html=True)

# --- HELPER FUNCTIONS ---
def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
    return output.getvalue()

@st.cache_resource
def get_engine():
    con = duckdb.connect(database=':memory:')
    # Ensure the file path is correct based on your repo
    con.execute("CREATE VIEW data AS SELECT * FROM 'master_data.parquet'")
    return con

db = get_engine()

# --- SIDEBAR NAV ---
with st.sidebar:
    # High-visibility warning that works on light and dark
    st.markdown(
        """
        <div style='background: rgba(255, 184, 0, 0.15); border: 1px solid #FFB800; padding: 10px; border-radius: 8px; text-align: center;'>
            <span style='color: #FFB800 !important; font-weight: 800; font-size: 12px; text-transform: uppercase; letter-spacing: 1px;'>
                ⚠️ Test Mode Active
            </span>
        </div>
        """, 
        unsafe_allow_html=True
    )
    st.markdown("---")
    st.markdown("<div class='status-pulse'></div><b>ENGINE STATUS: ONLINE</b>", unsafe_allow_html=True)
    st.caption("Latency: 14ms | Uptime: 99.9%")
    st.markdown("---")

    rows = db.execute("SELECT COUNT(*) FROM data").fetchone()[0]
    st.write(f"📂 **Database:** {rows:,} Records")
    st.markdown("---")

# --- MAIN PAGE HEADER ---
st.markdown('<h1 class="gradient-text">🚢 Tariff Intelligence Portal</h1>', unsafe_allow_html=True)
st.markdown("<p style='font-size:1.1rem; opacity: 0.8;'>Unified Extraction & Compliance Mapping Engine</p>", unsafe_allow_html=True)

tab1, tab2 = st.tabs(["🔍 DISCOVERY EXPLORER", "📤 BULK EXTRACTION"])

# --- TAB 1: DISCOVERY ---
with tab1:
    with st.expander("⚙️ View Settings (Customize Columns)"):
        all_master_cols = db.execute("DESCRIBE data").df()['column_name'].tolist()
        view_cols = st.multiselect(
            "Select Columns to Display:", 
            options=all_master_cols,
            default=["HS Code", "Country", "L1", "L2", "L3", "L4"]
        )

    # Filtering UI
    st.markdown("<p class='step-header'>Hierarchical Search</p>", unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4) 
    
    l1_list = db.execute("SELECT DISTINCT L1 FROM data WHERE L1 IS NOT NULL ORDER BY L1").df()['L1'].tolist()
    s_l1 = c1.selectbox("Market Segment", ["All Segments"] + l1_list)

    l2_q = f"SELECT DISTINCT L2 FROM data WHERE L1='{s_l1}' ORDER BY L2" if s_l1 != "All Segments" else "SELECT DISTINCT L2 FROM data ORDER BY L2"
    s_l2 = c2.selectbox("Product Group", ["All Groups"] + db.execute(l2_q).df()['L2'].tolist())

    l3_q = f"SELECT DISTINCT L3 FROM data WHERE L2='{s_l2}' ORDER BY L3" if s_l2 != "All Groups" else "SELECT DISTINCT L3 FROM data ORDER BY L3"
    s_l3 = c3.selectbox("Product Class", ["All Classes"] + db.execute(l3_q).df()['L3'].tolist())

    l4_q = f"SELECT DISTINCT L4 FROM data WHERE L3='{s_l3}' ORDER BY L4" if s_l3 != "All Classes" else "SELECT DISTINCT L4 FROM data ORDER BY L4"
    s_l4 = c4.selectbox("Specific Item", ["All Items"] + db.execute(l4_q).df()['L4'].tolist())

    search = st.text_input("🎯 Global Lookup", placeholder="Enter HS Code, Country, or Product Keyword...")

    # Dynamic SQL Logic
    sel_sql = ", ".join([f'"{c}"' for c in view_cols]) if view_cols else "*"
    query = f"SELECT {sel_sql} FROM data WHERE 1=1"
    if s_l1 != "All Segments": query += f" AND L1 = '{s_l1}'"
    if s_l2 != "All Groups": query += f" AND L2 = '{s_l2}'"
    if s_l3 != "All Classes": query += f" AND L3 = '{s_l3}'"
    if s_l4 != "All Items": query += f" AND L4 = '{s_l4}'"
    if search: 
        query += f" AND (CAST(\"HS Code\" AS VARCHAR) LIKE '{search}%' OR \"Country\" LIKE '%{search}%')"

    data_preview = db.execute(query + " LIMIT 1000").df()
    
    col_stat, col_dl = st.columns([3, 1])
    with col_stat:
        st.write(f"**Found {len(data_preview)} matches in preview.**")
    
    with col_dl:
        if st.button("📊 Export Current View"):
            full_data = db.execute(query).df()
            st.download_button(
                label="📥 Download Excel",
                data=to_excel(full_data),
                file_name=f"Tariff_Explorer_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

    st.dataframe(data_preview, use_container_width=True, height=450)

# --- TAB 2: BULK ENGINE ---
with tab2:
    st.markdown("""
        <div class="glass-card">
            <h5 style='margin:0; color:#0072ff;'>🚀 Bulk Extraction Engine</h5>
            <p style='font-size:1.5rem; opacity:0.7;'>Automated HS Code and Country-specific tariff mapping.</p>
        </div>
    """, unsafe_allow_html=True)

    left, right = st.columns([1, 2], gap="large")

    with left:
        st.markdown("<p class='step-header'>1. Preparation</p>", unsafe_allow_html=True)
        st.download_button(
            label="📥 Download Excel Template",
            data=to_excel(pd.DataFrame(columns=["HS Code", "Country"])),
            file_name="tariff_mapping_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        
        st.markdown("<br><p class='step-header'>2. Upload Dataset</p>", unsafe_allow_html=True)
        uploaded = st.file_uploader("Upload Excel or CSV", type=['xlsx', 'csv'], label_visibility="collapsed")
        
        if uploaded:
            st.markdown("<br><p class='step-header'>3. Configuration</p>", unsafe_allow_html=True)
            all_cols = db.execute("DESCRIBE data").df()['column_name'].tolist()
            to_fetch = st.multiselect("Enrichment Fields:", all_cols, 
                                      default=["HS Code", "Country", "L1", "L2"])
            run_btn = st.button("🚀 INITIATE EXTRACTION")

    with right:
        if uploaded and run_btn:
            with st.status("Engine Processing...", expanded=True) as status:
                try:
                    df_in = pd.read_excel(uploaded) if uploaded.name.endswith('xlsx') else pd.read_csv(uploaded)
                    
                    def find_col(p_names, actual):
                        for p in p_names:
                            for a in actual:
                                if p.upper() in str(a).upper(): return a
                        return None

                    hs_key = find_col(["HS", "CODE", "COMMODITY"], df_in.columns)
                    ct_key = find_col(["COUNTRY", "NATION", "ORIGIN"], df_in.columns)

                    if not hs_key:
                        st.error("No HS Code column found.")
                        st.stop()

                    df_in[hs_key] = df_in[hs_key].astype(str).str.strip()
                    df_in['HS_LEN'] = df_in[hs_key].str.len()
                    if ct_key:
                        df_in[ct_key] = df_in[ct_key].astype(str).str.strip()
                    
                    db.register('upload_data', df_in)

                    if ct_key:
                        enrich_cols = ", ".join([f'm."{c}"' for c in to_fetch if c not in df_in.columns])
                        sql = f"""SELECT u.*, {enrich_cols} FROM upload_data u 
                                  LEFT JOIN data m ON u."{hs_key}" = m."HS Code" AND u."{ct_key}" = m."Country" """
                    else:
                        enrich_cols = ", ".join([f'm."{c}"' for c in to_fetch if c != "HS Code"])
                        sql = f"""SELECT * FROM (
                                    SELECT u.*, {enrich_cols}, 
                                    ROW_NUMBER() OVER (PARTITION BY u."{hs_key}" ORDER BY LENGTH(m."HS Code") DESC) as rn
                                    FROM upload_data u LEFT JOIN data m ON LEFT(m."HS Code", u.HS_LEN) = u."{hs_key}"
                                  ) WHERE rn = 1"""
                    
                    final_res = db.execute(sql).df().fillna("N/A")
                    status.update(label="EXTRACTION SUCCESS", state="complete")
                    
                    st.success(f"Processed {len(final_res)} rows.")
                    st.dataframe(final_res.head(100), use_container_width=True)
                    
                    st.download_button(
                        label="💾 Download Final Excel Report",
                        data=to_excel(final_res),
                        file_name="Tariff_Enriched_Report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                except Exception as e:
                    st.error(f"Error: {e}")
        else:
            st.markdown("""
                <div style="height:400px; display:flex; align-items:center; justify-content:center; border:2px dashed rgba(120,120,120,0.2); border-radius:15px;">
                    <p style="color:gray;">Awaiting input data...</p>
                </div>
            """, unsafe_allow_html=True)