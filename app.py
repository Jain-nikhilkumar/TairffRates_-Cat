import streamlit as st
import pandas as pd
import duckdb
import io
import plotly.express as px
from datetime import datetime

# --- 1. APP CONFIGURATION ---
st.set_page_config(
    page_title="CORE | Tariff Intelligence",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 2. ADAPTIVE ENTERPRISE CSS ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap');

    /* Global Transitions & Font */
    * { font-family: 'Outfit', sans-serif; transition: all 0.2s ease; }

    /* Glassmorphism Card - ADAPTIVE */
    .glass-card {
        background: rgba(120, 120, 120, 0.05);
        border: 1px solid rgba(120, 120, 120, 0.2);
        border-radius: 12px;
        padding: 15px;
        margin-bottom: 10px;
        backdrop-filter: blur(10px);
    }

    /* HIGH-CONTRAST DATAFRAME FIX (Universal for Dark/Light) */
    [data-testid="stDataFrame"] { background: transparent !important; }

    /* Force a Deep Blue hover with White Text to solve visibility issues */
    [data-testid="stDataFrame"] div[role="gridcell"]:hover,
    [data-testid="stDataFrame"] div[role="row"]:hover {
        background-color: #1E3A8A !important; /* Deep Navy Blue */
        color: #FFFFFF !important;           /* Forced White Text */
    }

    /* Standard cell text - Uses Streamlit's theme variable */
    [data-testid="stDataFrame"] div[role="gridcell"] {
        color: var(--text-color) !important;
    }

    /* Column Header Contrast Fix */
    [data-testid="stDataFrame"] div[role="columnheader"] {
        background-color: rgba(120, 120, 120, 0.15) !important;
        color: var(--text-color) !important;
        font-weight: 800 !important;
    }

    /* Gradient Title & Neon Accents */
    .gradient-text {
        background: linear-gradient(90deg, #0072ff, #00c6ff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800; font-size: 42px; letter-spacing: -1px;
    }

    .stButton>button {
        background: linear-gradient(90deg, #0072ff 0%, #00c6ff 100%) !important;
        color: white !important; border-radius: 8px !important;
        padding: 10px 24px !important; box-shadow: 0 4px 12px rgba(0, 114, 255, 0.2);
    }
    
    .step-header { font-size: 0.85rem; font-weight: 800; color: #0072ff; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 5px; }

    /* Service Status Pulse */
    .status-pulse {
        height: 10px; width: 10px; background-color: #00ff88;
        border-radius: 50%; display: inline-block; margin-right: 8px;
        box-shadow: 0 0 8px #00ff88; animation: pulse 1.5s infinite;
    }
    @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.4; } 100% { opacity: 1; } }
    </style>
    """, unsafe_allow_html=True)

# --- 3. DATABASE & HELPERS ---
@st.cache_resource
def get_engine():
    con = duckdb.connect(database=':memory:')
    con.execute("CREATE VIEW data AS SELECT * FROM 'master_data.parquet'")
    return con

def to_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
    return output.getvalue()

db = get_engine()

# --- 4. FORMATTING CONFIG ---
# Formats decimals (0.15 -> 15.00%) and Currency
# --- EXPANDED COLUMN FORMATTING CONFIG ---
numeric_format_config = {
    # 1. Percentage Columns (Formatted with 2 decimals and %)
    "Old Tariff (Dec 2024)": st.column_config.NumberColumn(format="%.2f%%"),
    "Sec 232 Tariffs": st.column_config.NumberColumn(format="%.2f%%"),
    "Sec 232 Copper": st.column_config.NumberColumn(format="%.2f%%"),
    "Mexican Tomatoes (in effect from 14-Jul-2025)": st.column_config.NumberColumn(format="%.2f%%"),
    "Canadian Energy": st.column_config.NumberColumn(format="%.2f%%"),
    "Canadian & Mexican Potash": st.column_config.NumberColumn(format="%.2f%%"),
    "IEEPA Tariffs on China": st.column_config.NumberColumn(format="%.2f%%"),
    "IEEPA Tariffs on Mexico & Canada": st.column_config.NumberColumn(format="%.2f%%"),
    "Reciprocal Tariff": st.column_config.NumberColumn(format="%.2f%%"),
    "90 Day Pause on All others": st.column_config.NumberColumn(format="%.2f%%"),
    "Revised Reciprocal Tariffs": st.column_config.NumberColumn(format="%.2f%%"),
    "Electronics Exemptions": st.column_config.NumberColumn(format="%.2f%%"),
    "Annex II Exemptions": st.column_config.NumberColumn(format="%.2f%%"),
    "If Reciprocal Tariff were applied": st.column_config.NumberColumn(format="%.2f%%"),
    "Tariff as of 10-April-2025": st.column_config.NumberColumn(format="%.2f%%"),
    "Revised Reciprocal Tariffs (To go in effect on 01-Aug-2025)": st.column_config.NumberColumn(format="%.2f%%"),

    # 2. Currency/Total Columns (Formatted with $ sign and commas)
    "2024 Imports for Consumption": st.column_config.NumberColumn(format="$%.2f"),
    "US Import $": st.column_config.NumberColumn(format="$%.2f"),
    "Total Tariff Paid On Dec 2024": st.column_config.NumberColumn(format="$%.2f"),
    "Total Tariff Paid On Recoprocal Tariff": st.column_config.NumberColumn(format="$%.2f"),
    "Total Tariff Paid On 10-April-2025": st.column_config.NumberColumn(format="$%.2f"),
}
# --- 5. SIDEBAR NAV ---
with st.sidebar:
    st.markdown(
        "<div style='background: rgba(255, 184, 0, 0.15); border: 1px solid #FFB800; padding: 10px; border-radius: 8px; text-align: center;'>"
        "<span style='color: #FFB800 !important; font-weight: 800; font-size: 12px;'>⚠️ TEST MODE ACTIVE</span></div>", 
        unsafe_allow_html=True
    )
    st.markdown("---")
    st.markdown("<div class='status-pulse'></div><b>SYSTEM: ONLINE</b>", unsafe_allow_html=True)
    st.caption("Latency: 12ms | Region: Global")
    st.markdown("---")
    rows = db.execute("SELECT COUNT(*) FROM data").fetchone()[0]
    st.write(f"📂 **Database:** {rows:,} Records")

# --- 6. MAIN PAGE HEADER ---
st.markdown('<h1 class="gradient-text">🚢 Tariff Intelligence Portal</h1>', unsafe_allow_html=True)
st.markdown("<p style='font-size:1.1rem; opacity: 0.8;'>Unified Extraction & Compliance Mapping Engine</p>", unsafe_allow_html=True)

tab1, tab2 = st.tabs(["🔍 DISCOVERY EXPLORER", "📤 BULK EXTRACTION"])

# --- TAB 1: DISCOVERY ---
with tab1:
    with st.expander("⚙️ View Settings (Select Columns)"):
        all_master_cols = db.execute("DESCRIBE data").df()['column_name'].tolist()
        view_cols = st.multiselect("Visible Fields:", options=all_master_cols,
                                  default=["HS Code", "Country", "L1", "L2", "L3", "L4", "Revised Reciprocal Tariffs (To go in effect on 01-Aug-2025)"])

    st.markdown("<p class='step-header'>Hierarchical Search</p>", unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4) 
    
    l1_list = db.execute("SELECT DISTINCT L1 FROM data WHERE L1 != 'N/A' ORDER BY L1").df()['L1'].tolist()
    s_l1 = c1.selectbox("Market Segment", ["All Segments"] + l1_list)
    l2_q = f"SELECT DISTINCT L2 FROM data WHERE L1='{s_l1}' ORDER BY L2" if s_l1 != "All Segments" else "SELECT DISTINCT L2 FROM data ORDER BY L2"
    s_l2 = c2.selectbox("Product Group", ["All Groups"] + db.execute(l2_q).df()['L2'].tolist())
    l3_q = f"SELECT DISTINCT L3 FROM data WHERE L2='{s_l2}' ORDER BY L3" if s_l2 != "All Groups" else "SELECT DISTINCT L3 FROM data ORDER BY L3"
    s_l3 = c3.selectbox("Product Class", ["All Classes"] + db.execute(l3_q).df()['L3'].tolist())
    l4_q = f"SELECT DISTINCT L4 FROM data WHERE L3='{s_l3}' ORDER BY L4" if s_l3 != "All Classes" else "SELECT DISTINCT L4 FROM data ORDER BY L4"
    s_l4 = c4.selectbox("Specific Item", ["All Items"] + db.execute(l4_q).df()['L4'].tolist())

    search = st.text_input("🎯 Global Lookup", placeholder="Search HS Code (starts with) or Country...")

    # Build SQL logic (HS starts with input)
    sel_sql = ", ".join([f'"{c}"' for c in view_cols]) if view_cols else "*"
    query = f"SELECT {sel_sql} FROM data WHERE 1=1"
    if s_l1 != "All Segments": query += f" AND L1 = '{s_l1}'"
    if s_l2 != "All Groups": query += f" AND L2 = '{s_l2}'"
    if s_l3 != "All Classes": query += f" AND L3 = '{s_l3}'"
    if s_l4 != "All Items": query += f" AND L4 = '{s_l4}'"
    if search: query += f" AND (CAST(\"HS Code\" AS VARCHAR) LIKE '{search}%' OR \"Country\" LIKE '%{search}%')"

    data_preview = db.execute(query + " LIMIT 1000").df()

    # --- FIX: Convert 0.15 to 15.00 for display ---
    # Updated list to include all your new percentage columns
    pct_cols = [
        "Old Tariff (Dec 2024)", "Sec 232 Tariffs", "Sec 232 Copper", 
        "Mexican Tomatoes (in effect from 14-Jul-2025)", "Canadian Energy", 
        "Canadian & Mexican Potash", "IEEPA Tariffs on China", 
        "IEEPA Tariffs on Mexico & Canada", "Reciprocal Tariff", 
        "90 Day Pause on All others", "Revised Reciprocal Tariffs", 
        "Electronics Exemptions", "Annex II Exemptions", 
        "If Reciprocal Tariff were applied", "Tariff as of 10-April-2025", 
        "Revised Reciprocal Tariffs (To go in effect on 01-Aug-2025)"
    ]
    for col in pct_cols:
        if col in data_preview.columns:
            data_preview[col] = data_preview[col] * 100

    # --- TAB 1: ANALYTICS (SAFE MODE) ---
    if not data_preview.empty:
        m1, m2, m3 = st.columns(3)

        # Safe Charts
        if "Country" in data_preview.columns and "2024 Imports for Consumption" in data_preview.columns:
            c_left, c_right = st.columns(2)
            with c_left:
                top_countries = data_preview.groupby("Country")["2024 Imports for Consumption"].sum().nlargest(10).reset_index()
                fig1 = px.bar(top_countries, x="2024 Imports for Consumption", y="Country", orientation='h', title="Top 10 Partners by Value")
                st.plotly_chart(fig1, use_container_width=True)
            with c_right:
                if "L1" in data_preview.columns:
                    fig2 = px.pie(data_preview, names="L1", values="2024 Imports for Consumption", hole=0.4, title="Value Split by Industry")
                    st.plotly_chart(fig2, use_container_width=True)

    # Export Button
    if not data_preview.empty:
        if st.button("📊 Export Filtered View to Excel"):
            full_export = db.execute(query).df()
            st.download_button("📥 Click to Download .xlsx", to_excel(full_export), "Tariff_Search.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    st.dataframe(data_preview, use_container_width=True, height=450, column_config=numeric_format_config)

# --- TAB 2: BULK ENGINE ---
# --- TAB 2: BULK ENGINE ---
with tab2:
    st.markdown("<div class='glass-card'><h5 style='margin:0; color:#0072ff;'>🚀 Bulk Extraction Engine</h5><p style='font-size:0.9rem; opacity:0.7;'>Automated HS Code and Country tariff mapping.</p></div>", unsafe_allow_html=True)
    l, r = st.columns([1, 2], gap="large")
    with l:
        st.markdown("<p class='step-header'>1. Preparation</p>", unsafe_allow_html=True)
        st.download_button("📥 Get Template", to_excel(pd.DataFrame(columns=["HS Code", "Country"])), "template.xlsx", use_container_width=True)
        st.markdown("<br><p class='step-header'>2. Upload Dataset</p>", unsafe_allow_html=True)
        uploaded = st.file_uploader("Upload File", type=['xlsx', 'csv'], label_visibility="collapsed")
        if uploaded:
            st.markdown("<br><p class='step-header'>3. Configuration</p>", unsafe_allow_html=True)
            # Fetch all column names for the multiselect
            all_master_cols = db.execute("DESCRIBE data").df()['column_name'].tolist()
            to_fetch = st.multiselect("Enrichment Fields:", all_master_cols, default=["HS Code", "Country", "L1", "Revised Reciprocal Tariffs (To go in effect on 01-Aug-2025)"])
            run_btn = st.button("🚀 INITIATE EXTRACTION")
    with r:
        if uploaded and run_btn:
            with st.status("Engine Processing...") as status:
                try:
                    df_in = pd.read_excel(uploaded) if uploaded.name.endswith('xlsx') else pd.read_csv(uploaded)
                    def find_col(names, actual):
                        for p in names:
                            for a in actual:
                                if p.upper() in str(a).upper(): return a
                        return None
                    hs_k = find_col(["HS", "CODE"], df_in.columns)
                    ct_k = find_col(["COUNTRY", "NATION"], df_in.columns)
                    if not hs_k: st.error("No HS Code column found."); st.stop()
                    
                    df_in[hs_k] = df_in[hs_k].astype(str).str.strip()
                    df_in['HS_LEN'] = df_in[hs_k].str.len()
                    if ct_key := ct_k: df_in[ct_key] = df_in[ct_key].astype(str).str.strip()
                    db.register('upload_data', df_in)

                    # --- DYNAMIC COLUMN SELECTION LOGIC ---
                    # We only pull columns from master 'm' that the user explicitly selected 
                    # and which are NOT already in the user's uploaded file 'u'
                    enrich_cols_sql = ", ".join([f'm."{c}"' for c in to_fetch if c not in df_in.columns])
                    
                    # Ensure there's a leading comma if we have extra columns to add
                    enrich_select = f", {enrich_cols_sql}" if enrich_cols_sql else ""

                    if ct_k:
                        # Logic: HS + Country Join
                        sql = f"""
                            SELECT u.* {enrich_select} 
                            FROM upload_data u 
                            LEFT JOIN data m ON u."{hs_k}" = m."HS Code" AND u."{ct_k}" = m."Country"
                        """
                    else:
                        # Logic: HS Prefix Join
                        sql = f"""
                            SELECT * FROM (
                                SELECT u.* {enrich_select}, 
                                ROW_NUMBER() OVER (PARTITION BY u."{hs_k}" ORDER BY LENGTH(m."HS Code") DESC) as rn 
                                FROM upload_data u 
                                LEFT JOIN data m ON LEFT(m."HS Code", u.HS_LEN) = u."{hs_k}"
                            ) WHERE rn = 1
                        """
                    
                    final_res = db.execute(sql).df()
                    
                    # --- CLEANUP ---
                    # Remove the row number helper column so it doesn't appear in Excel
                    if 'rn' in final_res.columns:
                        final_res = final_res.drop(columns=['rn'])
                    
                    # Remove the internal HS_LEN helper
                    if 'HS_LEN' in final_res.columns:
                        final_res = final_res.drop(columns=['HS_LEN'])

                    # --- FIX: Convert 0.15 to 15.00 for display ---
                    # Updated list to include all your new percentage columns
                    pct_cols = [
                        "Old Tariff (Dec 2024)", "Sec 232 Tariffs", "Sec 232 Copper", 
                        "Mexican Tomatoes (in effect from 14-Jul-2025)", "Canadian Energy", 
                        "Canadian & Mexican Potash", "IEEPA Tariffs on China", 
                        "IEEPA Tariffs on Mexico & Canada", "Reciprocal Tariff", 
                        "90 Day Pause on All others", "Revised Reciprocal Tariffs", 
                        "Electronics Exemptions", "Annex II Exemptions", 
                        "If Reciprocal Tariff were applied", "Tariff as of 10-April-2025", 
                        "Revised Reciprocal Tariffs (To go in effect on 01-Aug-2025)"
                    ]
                    for col in pct_cols:
                        if col in final_res.columns:
                            final_res[col] = pd.to_numeric(final_res[col], errors='coerce') * 100
                    
                    final_res = final_res.fillna("N/A")
                    st.success(f"Processed {len(final_res)} rows.")
                    
                    st.dataframe(final_res.head(100), use_container_width=True, column_config=numeric_format_config)
                    
                    st.download_button(
                        label="💾 Download Excel Report", 
                        data=to_excel(final_res), 
                        file_name="Enriched_Report.xlsx", 
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                        use_container_width=True
                    )
                except Exception as e: st.error(f"Error: {e}")
        else:
            st.markdown("<div style='height:400px; border:2px dashed rgba(120,120,120,0.2); border-radius:15px; display:flex; align-items:center; justify-content:center; color:gray;'>Awaiting Input</div>", unsafe_allow_html=True)