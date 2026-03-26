# run this with: python -m streamlit run
import streamlit as st
import pandas as pd
from app_functionality import run_pipeline, delete_table_bigquery, query_sales_data, get_top_N_products_by_quantity,get_top_N_products_by_revenue, get_sales_by_region, get_sales_over_time, experimental_plain_text_query, query_bigquery
st.title("Sales Data Dashboard")

# Set background image
def set_bg(img_url):
    css = f"""
    <style>
    .stApp {{
        background-image: url('{img_url}');
        background-size: cover;
        background-position: center;
        background-repeat: no-repeat;
    }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

# session state to monitor what page the user is on
if "page" not in st.session_state:
    st.session_state.page = "home"

# Home page with descriptions of resources and buttons to navigate to different pages
def home():
    set_bg("https://imgs.search.brave.com/Fk3GEt9ZUF7DhzO9AsorcQ2jS_6DncEXkKktAGhLwYk/rs:fit:860:0:0:0/g:ce/aHR0cHM6Ly9zdGF0/aWMudmVjdGVlenku/Y29tL3N5c3RlbS9y/ZXNvdXJjZXMvdGh1/bWJuYWlscy8wMDIv/MDYzLzAyMy9zbWFs/bC9hYnN0cmFjdC10/ZWNobm9sb2d5LWJh/Y2tncm91bmQtd2l0/aC1iaWctZGF0YS1p/bnRlcm5ldC1jb25u/ZWN0aW9uLWFic3Ry/YWN0LXNlbnNlLW9m/LXNjaWVuY2UtYW5k/LXRlY2hub2xvZ3kt/YW5hbHl0aWNzLWNv/bmNlcHQtZ3JhcGhp/Yy1kZXNpZ24taWxs/dXN0cmF0aW9uLXZl/Y3Rvci5qcGc")
    st.header("Available Resources")
    st.subheader("GCP Setup Resources")
    st.write("  - Convert CSV to Parquet")
    st.write("  - Create table in BigQuery")
    st.write("  - Delete Data from BigQuery")

    st.subheader("Data Analysis Resources")
    st.write("  - Query Sales Data")
    st.write("  - Get Top N Products by Quantity")
    st.write("  - Get Top N Products by Revenue")

    # buttons to navigate
    if st.button("Setup GCP"):
        st.session_state.page = "gcp_setup"
    if st.button("Data Analysis"):
        st.session_state.page = "data_analysis"


# page with all our GCP stuff
def gcp_setup():
    set_bg("https://imgs.search.brave.com/0WxkNghrTCt2nNry80Aj8jPgaWLPVoJmgBVQqx0USas/rs:fit:860:0:0:0/g:ce/aHR0cHM6Ly93YWxs/cGFwZXJhY2Nlc3Mu/Y29tL2Z1bGwvOTQ3/ODQxOC5wbmc")
    st.header("GCP Setup Resources")
    # --- Description of GCP setup steps ---
    with st.expander("Start ETL Pipeline here"):
        st.write("""
        The following will happen:
        1. Transforms your local CSV file into parquet format
        2. Uploads the parquet file to GCS
        3. Creates an external table in BigQuery
        """)
    
        if st.button("Run ETL Pipeline"):
            st.write("Running pipeline please wait...")
            test = run_pipeline()
            if test == "OK":
                st.success("ETL Pipeline started successfully!")
            else:
                st.error(f"ETL Pipeline failed: {test}")

    with st.expander("Delete tables from BigQuery"):
        st.write("""
        This will delete the external table in BigQuery
        """)
        if st.button("Delete BigQuery Table"):
            st.write("Deleting BigQuery table please wait...")
            delete_table_bigquery()
            st.success("BigQuery table deleted successfully!")


    log_file_path = "./../reporting.log"

    with st.expander("View Utility Report File"):
        st.write("Showing contents of the log file:")

        try:
            # open and read the log file
            with open(log_file_path, "r") as f:
                log_text = f.read()

            # display the log text in Streamlit
            st.text(log_text)  

        except FileNotFoundError:
            st.error(f"Log file not found: {log_file_path}")
    if st.button("Home"):
        st.session_state.page = "home"

# data analysis page with all our data analysis resources
def data_analysis():
    set_bg("https://imgs.search.brave.com/wvsLWVoESuUraDWUh6iDqueD-8AvyZAtYzWEzykY-RA/rs:fit:860:0:0:0/g:ce/aHR0cHM6Ly9zdGF0/aWMudmVjdGVlenku/Y29tL3N5c3RlbS9y/ZXNvdXJjZXMvdGh1/bWJuYWlscy8wMDMv/MDMwLzgwOS9zbWFs/bC9iaWctZGF0YS1h/bmFseXNpcy1pc29t/ZXRyaWMtYmFja2dy/b3VuZC1pbGx1c3Ry/YXRpb24tdmVjdG9y/LmpwZw")
    with st.expander("Query Sales Data"):
        st.write("""
        This will query the sales data table in BigQuery and return the results as a dataframe
        """)
        customerid = st.text_input("Enter Customer ID to query sales data for:")
        if st.button("Query Sales Data"): # when button is pressed, run the query and return results
            st.write("Querying sales data please wait...")
            result = query_sales_data(customerid)
            if isinstance(result, dict) and "error" in result:
                st.error(f"Query failed: {result['error']}")
            else:
                st.success("Query successful!")
                st.dataframe(pd.DataFrame(result))

    with st.expander("Get Top N Products by Quantity"):
        st.write("""
        This will query the sales data table in BigQuery and return the top N products by quantity sold
        """)
        n = st.number_input("Enter N to query top N products by quantity sold:", min_value=1, step=1)
        if st.button("Get Top N Products by Quantity"): # when button is pressed, run the query and return results
            st.write("Querying top N products by quantity sold please wait...")
            result = get_top_N_products_by_quantity(n)
            if isinstance(result, dict) and "error" in result:
                st.error(f"Query failed: {result['error']}")
            else:
                st.success("Query successful!")
                df = pd.DataFrame(result)
                st.bar_chart(df.set_index('ProductName')['Quantity'])
                # keep df in case we want to display it as a table as well
                st.dataframe(df)

    with st.expander("Get Top N Products by Revenue"):
        st.write("""
        This will query the sales data table in BigQuery and return the top N products by revenue
        """)
        n = st.number_input("Enter N to query top N products by revenue:", min_value=1, step=1)
        if st.button("Get Top N Products by Revenue"): # when button is pressed, run the query and return results
            st.write("Querying top N products by revenue please wait...")
            result = get_top_N_products_by_revenue(n)
            if isinstance(result, dict) and "error" in result:
                st.error(f"Query failed: {result['error']}")
            else:
                st.success("Query successful!")
                df = pd.DataFrame(result)
                # display as barchart
                st.bar_chart(df.set_index('ProductName')['Total_Sales'])
                # keep df in case we want to display it as a table as well
                st.dataframe(df)

    with st.expander("Get Sales By Region"):
        st.write("""
        This will query the sales data table in BigQuery and return the sales data by region
        """)
        if st.button("Get Sales By Region"): # when button is pressed, run the query and return results
            st.write("Querying sales by region please wait...")
            result = get_sales_by_region()
            if isinstance(result, dict) and "error" in result:
                st.error(f"Query failed: {result['error']}")
            else:
                st.success("Query successful!")
                df = pd.DataFrame(result)
                # display as barchart
                st.bar_chart(df.set_index('Region')['Total_Sales'])
                # keep df in case we want to display it as a table as well
                st.dataframe(df)

    with st.expander("Get Sales Over Time"):
        st.write("""
        This will query the sales data table in BigQuery and return the sales data over time grouped by month
        """)
        if st.button("Get Sales Over Time"): # when button is pressed, run the query and return results
            st.write("Querying sales Over time please wait...")
            result = get_sales_over_time()
            if isinstance(result, dict) and "error" in result:
                st.error(f"Query failed: {result['error']}")
            else:
                st.success("Query successful!")
                df = pd.DataFrame(result)
                # display as line graph
                st.line_chart(df.set_index('month')['total_sales'])
                # keep df in case we want to display it as a table as well
                st.dataframe(df)

    with st.expander("Generate and Run SQL from Plain English"):
        st.write("""
        Step 1: Describe your query in plain English. GPT will generate a safe BigQuery SQL query.
        Step 2: Review the SQL. If correct, click "Run SQL" to execute and view results.
        """)

        #  get input from user
        user_request = st.text_area("Describe your query in plain English:")

        # start running BQ on gpt
        generated_sql = ""
        stripped_sql = ""
        if st.button("Generate SQL"):
            if not user_request.strip():
                st.warning("Please enter a description of the query.")
            else:
                st.info("Generating SQL, please wait...")
                try:
                    # Call GPT function
                    generated_output = experimental_plain_text_query(user_request)
                    
                    # Check for ERROR
                    if generated_output.strip().upper().startswith("ERROR"):
                        st.error(generated_output.strip())
                    else:
                        # Safely extract SQL after BIG_QUERY_SQL_HERE:
                        parts = generated_output.split("BIG_QUERY_SQL_HERE:")
                        if len(parts) < 2:
                            st.error("No SQL found in GPT output.")
                        else:
                            stripped_sql = parts[1].strip()
                            # Remove code fences
                            if stripped_sql.startswith("```"):
                                stripped_sql = stripped_sql[3:].lstrip()
                                if stripped_sql.lower().startswith("sql"):
                                    stripped_sql = stripped_sql[3:].lstrip()
                            if stripped_sql.endswith("```"):
                                stripped_sql = stripped_sql[:-3].rstrip()
                            
                            st.subheader("Generated SQL:")
                            st.code(stripped_sql, language="sql")
                except Exception as e:
                    st.error(f"An unexpected error occurred: {e}")

            print("STRIPPED SQL LINE 217: " + stripped_sql)
            # run sql on bq
            # Use st.text_area to keep the SQL between reruns

            # Ensure generated_sql is persisted
            # if "generated_sql" not in st.session_state:
            st.session_state.generated_sql = stripped_sql
            print("LINE 224: " + st.session_state.generated_sql)

            # Input area
            st.session_state.generated_sql = st.text_area(
                "SQL Query", value=st.session_state.generated_sql, height=200
            )

        # Button to run SQL
        if st.button("Run SQL"):
            if st.session_state.generated_sql.startswith("ERROR"):
                st.error(st.session_state.generated_sql)
            else:
                try:
                    print("STRIPPED SQL LINE 236: " + st.session_state.generated_sql)
                    st.write("Running SQL, please wait...")
                    df = query_bigquery(st.session_state.generated_sql)  # your BigQuery function st.session_state.generated_sql
                    st.success("Query executed successfully!")
                    st.dataframe(df)
                except Exception as e:
                    st.error(f"Failed to run query: {e}")

    if st.button("Home"):
        st.session_state.page = "home"
# Session state tracker
if st.session_state.page == "home":
    home()
elif st.session_state.page == "gcp_setup":
    gcp_setup()
elif st.session_state.page == "data_analysis":
    data_analysis()