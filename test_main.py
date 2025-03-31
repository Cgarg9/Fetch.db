import unittest
from unittest.mock import patch, MagicMock
import main

class TestMainFunctions(unittest.TestCase):
    @patch("main.psycopg2.connect")
    def test_fetch_table_schema(self, mock_connect):
        # Prepare a fake cursor and connection that returns sample schema rows
        fake_rows = [
            ("table1", "column1", "text"),
            ("table1", "column2", "integer"),
            ("table2", "colA", "text")
        ]
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = fake_rows
        
        # Set up connection context manager behavior
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        # Call the function, which should use the mocked connection
        schema = main.fetch_table_schema()
        expected = {
            "table1": [("column1", "text"), ("column2", "integer")],
            "table2": [("colA", "text")]
        }
        self.assertEqual(schema, expected)

    @patch("main.requests.post")
    def test_explain_sql_query(self, mock_post):
        # Create a fake response for the GROQ API call
        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.json.return_value = {
            "choices": [{
                "message": {"content": "This query returns all rows."}
            }]
        }
        mock_post.return_value = fake_response
        
        explanation = main.explain_sql_query("SELECT * FROM table;")
        self.assertEqual(explanation, "This query returns all rows.")

if __name__ == '__main__':
    unittest.main()