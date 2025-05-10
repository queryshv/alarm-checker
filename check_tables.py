import psycopg2

def list_tables():
    try:
        conn = psycopg2.connect(
            dbname="raven",
            user="postgres",
            password="2336",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = cur.fetchall()
        print("\nTables found in database:")
        for table in tables:
            print(f"- {table[0]}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_tables() 