import streamlit as st
import requests
import pandas as pd
import plotly.express as px

# --- Configuration ---
API_BASE_URL = st.sidebar.text_input("API Base URL", value="http://localhost:8000", help="The FastAPI backend URL")

# --- Page Setup ---
st.set_page_config(page_title="Sales Pipeline Dashboard", page_icon="📊", layout="wide")

st.title("📊 Sales Pipeline Dashboard")
st.markdown("Explore sales data powered by BigQuery and Google Cloud Storage.")

# --- Sidebar Navigation ---
page = st.sidebar.radio(
    "Navigate",
    ["Data Pipeline", "Manage Table", "Customer Transactions", "Top Products", "Dataset Overview"],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.caption("ADJ Project 2 — Cloud Pipeline")


def call_api(endpoint: str, params: dict | None = None):
    """Helper to call the FastAPI backend and handle errors."""
    try:
        resp = requests.get(f"{API_BASE_URL}{endpoint}", params=params, timeout=30)
        resp.raise_for_status()
        return resp.json(), None
    except requests.exceptions.ConnectionError:
        return None, "Could not connect to the API. Make sure the FastAPI server is running."
    except requests.exceptions.HTTPError as e:
        detail = ""
        try:
            detail = e.response.json().get("detail", "")
        except Exception:
            pass
        return None, f"API error ({e.response.status_code}): {detail or str(e)}"
    except Exception as e:
        return None, str(e)

# ============================================================
# Page: Data Pipeline
# ============================================================
if page == "Data Pipeline":
    st.header("⚙️ Data Pipeline — CSV to Parquet & Upload")
    st.write(
        "Convert source CSV files into a single Parquet file and upload it to Google Cloud Storage. "
        "This triggers the `/convert` endpoint on the API."
    )

    st.warning("This will overwrite the existing Parquet file in GCS. Proceed with caution.")

    if st.button("Run Pipeline", type="primary"):
        with st.spinner("Running conversion and upload — this may take a moment…"):
            try:
                resp = requests.post(f"{API_BASE_URL}/convert", timeout=120)
                resp.raise_for_status()
                result = resp.json()
                st.success(result.get("message", "Pipeline completed successfully."))
            except requests.exceptions.ConnectionError:
                st.error("Could not connect to the API. Make sure the FastAPI server is running.")
            except requests.exceptions.HTTPError as e:
                st.error(f"Pipeline failed: {e}")
            except Exception as e:
                st.error(str(e))


# ============================================================
# Page: Manage Table
# ============================================================
elif page == "Manage Table":
    st.header("🛠️ Manage BigQuery Table")

    # --- Create Table ---
    st.subheader("➕ Create BigQuery Table")
    st.write(
        "Create (or replace) the `sales-data` external table in BigQuery, "
        "linked to the Parquet file in Google Cloud Storage."
    )

    if st.button("Create Table", type="primary"):
        with st.spinner("Creating table…"):
            data, err = call_api("/creating_table")

        if err:
            st.error(err)
        else:
            st.success("The `sales-data` external table has been created in BigQuery.")

    st.divider()

    # --- Delete Table ---
    st.subheader("🗑️ Delete BigQuery Table")
    st.write("Delete the `sales-data` external table from BigQuery.")

    st.error(
        "**Destructive action** — This will permanently delete the table from BigQuery. "
        "You can recreate it from the 'Create BigQuery Table' section above."
    )

    confirm = st.checkbox("I understand this will delete the table")

    if st.button("Delete Table", type="primary", disabled=not confirm):
        with st.spinner("Deleting table…"):
            try:
                resp = requests.delete(f"{API_BASE_URL}/delete-table", timeout=30)
                resp.raise_for_status()
                st.success("The `sales-data` table has been deleted from BigQuery.")
            except requests.exceptions.ConnectionError:
                st.error("Could not connect to the API. Make sure the FastAPI server is running.")
            except requests.exceptions.HTTPError as e:
                st.error(f"Delete failed: {e}")
            except Exception as e:
                st.error(str(e))

# ============================================================
# Page: Customer Transactions
# ============================================================
elif page == "Customer Transactions":
    st.header("🔍 Customer Transaction Lookup")
    st.write("Search for the most recent transactions by Customer ID.")

    col1, col2 = st.columns([3, 1])
    with col1:
        customer_id = st.text_input("Customer ID", placeholder="e.g. C001")
    with col2:
        st.markdown("<div style='margin-top: 1.65rem;'></div>", unsafe_allow_html=True)
        search = st.button("Search", type="primary", use_container_width=True)

    if search and customer_id:
        with st.spinner("Querying BigQuery…"):
            data, err = call_api("/query", {"CustomerID": customer_id})

        if err:
            st.error(err)
        elif not data:
            st.info(f"No transactions found for Customer **{customer_id}**.")
        else:
            df = pd.DataFrame(data)
            st.success(f"Found **{len(df)}** transaction(s) for Customer **{customer_id}**.")

            st.dataframe(df, use_container_width=True, hide_index=True)

            # Quick visual if data is present
            if "TotalAmount" in df.columns and "Transaction_Date" in df.columns:
                fig = px.bar(
                    df,
                    x="Transaction_Date",
                    y="TotalAmount",
                    color="ProductName",
                    title="Spending Over Time",
                    labels={"TotalAmount": "Total Amount ($)", "Transaction_Date": "Date"},
                )
                fig.update_layout(xaxis_type="category")
                st.plotly_chart(fig, use_container_width=True)

    elif search:
        st.warning("Please enter a Customer ID.")


# ============================================================
# Page: Top Products
# ============================================================
elif page == "Top Products":
    st.header("🏆 Top Products by Quantity Sold")

    n = st.slider("Number of products to show", min_value=1, max_value=8, value=5)
    
    if st.button("Load Top Products", type="primary"):
        with st.spinner("Querying BigQuery…"):
            data, err = call_api("/top-n-products-by-quantity", {"n": n})

        if err:
            st.error(err)
        elif not data:
            st.info("No product data returned.")
        else:
            df = pd.DataFrame(data)
            st.success(f"Showing top **{len(df)}** product(s).")

            col_table, col_chart = st.columns([1, 2])

            with col_table:
                st.dataframe(df, use_container_width=True, hide_index=True)

            with col_chart:
                fig = px.bar(
                    df,
                    x="Quantity",
                    y="ProductName",
                    orientation="h",
                    title=f"Top {n} Products by Quantity",
                    labels={"Quantity": "Units Sold", "ProductName": "Product"},
                    color="Quantity",
                    color_continuous_scale="blues",
                )
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # --- Top Products by Revenue ---
    st.header("💰 Top Products by Revenue")

    n_rev = st.slider("Number of products to show", min_value=1, max_value=8, value=5, key="n_rev")

    if st.button("Load Top Products by Revenue", type="primary"):
        with st.spinner("Querying BigQuery…"):
            data, err = call_api("/top-n-products-by-revenue", {"n": n_rev})

        if err:
            st.error(err)
        elif not data:
            st.info("No product data returned.")
        else:
            df = pd.DataFrame(data)
            st.success(f"Showing top **{len(df)}** product(s) by revenue.")

            col_table, col_chart = st.columns([1, 2])

            with col_table:
                st.dataframe(df, use_container_width=True, hide_index=True)

            with col_chart:
                fig = px.bar(
                    df,
                    x="Total_Sales",
                    y="ProductName",
                    orientation="h",
                    title=f"Top {n_rev} Products by Revenue",
                    labels={"Total_Sales": "Total Sales ($)", "ProductName": "Product"},
                    color="Total_Sales",
                    color_continuous_scale="greens",
                )
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig, use_container_width=True)
    
    st.divider()

    # --- Top Products by Revenue ---
    st.header("💰 Sales by Region")

    if st.button("Load Total Sales by Region", type="primary"):
        with st.spinner("Querying BigQuery…"):
            data, err = call_api("/sales-by-region")

        if err:
            st.error(err)
        elif not data:
            st.info("No product data returned.")
        else:
            df = pd.DataFrame(data)
            st.success(f"Displaying Total Sales by Region")

            bar_table, bar_chart = st.columns([1, 2])
            with bar_table:
                st.dataframe(df, use_container_width=True, hide_index=True)
            
            with bar_chart:
                fig = px.bar(
                    df,
                    x = "Region",
                    y = "Total_Sales",
                    title=f"Total Sales by Region",
                    labels={"Region": "Region", "Total_Sales": "Total Sales"},
                    color="Total_Sales",
                    color_continuous_scale="greens"
                )
                # fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig, use_container_width=True)


# ============================================================
# Page: Dataset Overview
# ============================================================
elif page == "Dataset Overview":
    st.header("📋 Dataset Overview")

    if st.button("Get Total Record Count", type="primary"):
        with st.spinner("Querying BigQuery…"):
            data, err = call_api("/total-file-length")

        if err:
            st.error(err)
        elif data:
            count = data[0].get("Total file length", "N/A")
            st.metric(label="Total Records in Dataset", value=f"{count:,}" if isinstance(count, (int, float)) else count)
        else:
            st.info("No data returned.")