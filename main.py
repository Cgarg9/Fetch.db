import psycopg2
import requests 
import streamlit as st 
import pandas as pd 
import functools 
import time  

# Groq API details
GROQ_MODEL = "llama-3.3-70b-versatile" 
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_API_KEY = "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"

# NeonDB connection URL
DB_URL = "postgresql://neondb_owner:npg_kOZ6yR1EMVor@ep-aged-haze-a5gwd062-pooler.us-east-2.aws.neon.tech/neondb?sslmode=require"

# Initialize session state
if "sql_query" not in st.session_state:
    st.session_state["sql_query"] = ""
if "query_result" not in st.session_state:
    st.session_state["query_result"] = []
if "explanation" not in st.session_state:
    st.session_state["explanation"] = ""


# Custom cache for fetch_table_schema
schema_cache = {}

def get_db_connection(retries=3, delay=2):
    """Obtain a DB connection with simple retry logic."""
    for attempt in range(retries):
        try:
            return psycopg2.connect(DB_URL)
        except psycopg2.Error as e:
            if attempt == retries - 1:
                raise e
            time.sleep(delay)

def call_groq_api(data, headers, retries=3, delay=2):
    """Call Groq API with simple retry logic."""
    for attempt in range(retries):
        try:
            response = requests.post(GROQ_API_URL, headers=headers, json=data)
            if response.status_code == 200:
                return response
            else:
                raise requests.RequestException(f"Groq API returned {response.status_code}")
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise e
            time.sleep(delay)

@st.cache_data(show_spinner=False)
def fetch_table_schema():
    """Fetch table names and column details dynamically from NeonDB."""
    start_time = time.time()  # Start timing
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
    """)

    schema_info = {}
    for table_name, column_name, data_type in cursor.fetchall():
        if table_name not in schema_info:
            schema_info[table_name] = []
        schema_info[table_name].append((column_name, data_type))

    conn.close()
    end_time = time.time()  # End timing
    print(f"fetch_table_schema executed in {end_time - start_time:.4f} seconds")

    return schema_info

def create_indexes():
    """Dynamically create indexes on foreign keys and frequently queried columns."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Fetch existing tables
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    existing_tables = {row[0] for row in cursor.fetchall()}

    # Fetch foreign keys from the database schema
    cursor.execute("""
        SELECT
            tc.table_name, kcu.column_name
        FROM
            information_schema.table_constraints AS tc
        JOIN
            information_schema.key_column_usage AS kcu
        ON
            tc.constraint_name = kcu.constraint_name
        WHERE
            tc.constraint_type = 'FOREIGN KEY'
    """)

    foreign_keys = cursor.fetchall()

    # Create indexes for foreign keys
    for table, column in foreign_keys:
        if table in existing_tables:  # Check if table exists
            index_name = f"{table.lower()}_{column.lower()}_fk_idx"
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS {index_name}
                ON {table} ({column});
            """)

    # Optionally, add indexes for other frequently queried columns
    frequently_queried_columns = [
        ("Genres", "Name"),
        ("Albums", "Title"),
    ]

    for table, column in frequently_queried_columns:
        if table in existing_tables:  # Check if table exists
            index_name = f"{table.lower()}_{column.lower()}_idx"
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS {index_name}
                ON {table} ({column});
            """)

    conn.commit()
    conn.close()

# Call create_indexes during initialization
create_indexes()

def generate_sql_query_groq(user_prompt, schema_info):
    """Uses Groq API to dynamically generate a valid SQL query based on schema."""
    
    schema_str = "\n".join([
        f"Table: {table}, Columns: {', '.join([col[0] for col in columns])}"
        for table, columns in schema_info.items()
    ])

    # Handle specific requests like fetching database names
    if "fetch all database names" in user_prompt.lower():
        return "SELECT datname FROM pg_database WHERE datistemplate = false;"

    # Handle requests for all attributes in a specific table
    if "all attributes in" in user_prompt.lower():
        table_name = user_prompt.lower().split("all attributes in")[-1].strip()
        return f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table_name}';
        """

    prompt = f"""
    You are an expert in SQL with knowledge of the Chinook database.
    
    Given the following database schema, generate a valid SQL query based on the user request.
    
    Schema:
    {schema_str}

    User Request:
    "{user_prompt}"

    Ensure:
    - The query is correct based on schema.
    - Use correct table relationships (e.g., Tracks, Genres, Albums).
    - Prioritize tables directly relevant to the request.
    - Do NOT make up table names or columns.
    - If the user request is unrelated to the database schema, return "SELECT NULL WHERE FALSE;".
    - Do NOT respond conversationally or provide explanations.

    Provide only the SQL query without explanations.
    """

    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}

    data = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 200
    }

    response = call_groq_api(data, headers)

    if response.status_code == 200:
        final_query = response.json()["choices"][0]["message"]["content"].strip()
        print("Generated SQL Query:", repr(final_query))  # Debugging print
        return final_query
    else:
        return f"Error: {response.json()}"

# Custom cache for execute_sql_with_cache
query_cache = {}

@st.cache_data(show_spinner=False)
def execute_sql_with_cache(query: str):
    """Executes the SQL query with caching using Streamlit's cache."""
    query = query.strip()  # Normalize query string
    start_time = time.time()  # Start timing
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    conn.close()
    end_time = time.time()  # End timing
    print(f"execute_sql_with_cache executed in {end_time - start_time:.4f} seconds")
    return result, colnames

def execute_sql(query):
    """Executes the SQL query with retry logic for fixes."""
    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        try:
            clean_query = query.strip("```sql").strip("```")
            start_time = time.time()  # Start timing
            result = execute_sql_with_cache(clean_query)
            end_time = time.time()  # End timing
            print(f"execute_sql executed in {end_time - start_time:.4f} seconds")
            return result
        except psycopg2.Error as e:
            if retry_count >= max_retries - 1:
                return f"SQL Execution Error after {max_retries} retries: {str(e)}", []
            retry_count += 1
            query = suggest_query_fix(query, str(e))

    return "Execution failed.", []


def suggest_query_fix(query, error_message):
    """Suggests modifications based on the error and regenerates a query."""
    
    # Fetch the schema to guide the query fix
    schema_info = fetch_table_schema()
    
    schema_str = "\n".join([
        f"Table: {table}, Columns: {', '.join([col[0] for col in columns])}"
        for table, columns in schema_info.items()
    ])

    prompt = f"""
    The following SQL query resulted in an error:
    {query}

    Error Message:
    {error_message}

    Here is the correct database schema:
    {schema_str}

    Modify the query to ensure it works correctly based on the schema. Do NOT invent table names or columns. 
    Provide only the corrected SQL query without any explanations.
    """
    
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    data = {"model": GROQ_MODEL, "messages": [{"role": "user", "content": prompt}], "max_tokens": 200}

    response = call_groq_api(data, headers)
    
    if response.status_code == 200:
        new_query = response.json()["choices"][0]["message"]["content"].strip()
        if new_query != query:
            return new_query
        else:
            return "SELECT 'Query Fix Failed' AS error;"
    else:
        return query  # Return the original query if it fails


def explain_sql_query(query):
    """Explains the SQL query in simple terms."""
    prompt = f"""
    Explain the following SQL query in simple terms:
    {query}
    
    Provide a short, clear explanation for a non-technical user.
    """
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    data = {"model": GROQ_MODEL, "messages": [{"role": "user", "content": prompt}], "max_tokens": 200}

    # Corrected the json parameter
    response = call_groq_api(data, headers)
    if response.status_code == 200:
        return response.json()["choices"][0]["message"]["content"].strip()
    else:
        return "Explanation not available."


# Streamlit UI
st.title("🎵 LLM SQL Agent for Chinook DB")

schema_info = fetch_table_schema()
user_prompt = st.text_input("🔍 Enter your query:", "List all songs where genre is metal")

if st.button("Generate & Execute SQL"):
    st.session_state["sql_query"] = generate_sql_query_groq(user_prompt, schema_info)
    st.code(st.session_state["sql_query"], language="sql")
    st.session_state["query_result"], column_names = execute_sql(st.session_state["sql_query"])
    st.session_state["columns"] = column_names

if "sql_query" in st.session_state and st.session_state["sql_query"]:
    st.code(st.session_state["sql_query"], language="sql")

if "query_result" in st.session_state and st.session_state["query_result"]:
    st.write("📊 **Query Result:**")
    df = pd.DataFrame(st.session_state["query_result"], columns=st.session_state.get("columns", []))
    st.dataframe(df)

if st.button("Explain Query"):
    st.session_state["explanation"] = explain_sql_query(st.session_state["sql_query"])
    st.write("📝 **Query Explanation:**")
    st.write(st.session_state["explanation"])

# if st.button("Test fetch_table_schema Cache"):
#     # Call repeatedly to test cache hits.
#     start = time.time()
#     schema1 = fetch_table_schema()
#     t1 = time.time() - start
#     start = time.time()
#     schema2 = fetch_table_schema()
#     t2 = time.time() - start
#     st.write(f"First call: {t1:.4f}s, Second call: {t2:.4f}s")

# if st.button("Test Repeated Queries"):
#     repeated_query = "SELECT * FROM genre;"  # or any query you'd like to benchmark
#     num_tests = 5
#     for i in range(num_tests):
#         start = time.time()
#         _ = execute_sql_with_cache(repeated_query)
#         duration = time.time() - start
#         st.write(f"Run {i+1}: {duration:.4f} seconds")

