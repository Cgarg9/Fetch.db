import psycopg2
import requests 
import streamlit as st 
import pandas as pd 

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


def fetch_table_schema():
    """Fetch table names and column details dynamically from NeonDB."""
    conn = psycopg2.connect(DB_URL)
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
    return schema_info

def generate_sql_query_groq(user_prompt, schema_info):
    """Uses Groq API to dynamically generate a valid SQL query based on schema."""
    
    schema_str = "\n".join([
        f"Table: {table}, Columns: {', '.join([col[0] for col in columns])}"
        for table, columns in schema_info.items()
    ])

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

    Provide only the SQL query without explanations.
    """

    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}

    data = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 200
    }

    response = requests.post(GROQ_API_URL, headers=headers, json=data)

    if response.status_code == 200:
        return response.json()["choices"][0]["message"]["content"].strip()
    else:
        return f"Error: {response.json()}"

def execute_sql(query):
    """Executes the SQL query with retry logic for fixes."""
    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        try:
            clean_query = query.strip("```sql").strip("```")
            conn = psycopg2.connect(DB_URL)
            cursor = conn.cursor()
            cursor.execute(clean_query)
            result = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            conn.close()
            return result, colnames
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

    response = requests.post(GROQ_API_URL, headers=headers, json=data)
    
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

    response = requests.post(GROQ_API_URL, headers=headers, json=data)
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

