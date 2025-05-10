import psycopg2
from datetime import datetime, timedelta
import pandas as pd
from tabulate import tabulate

def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname="raven",
            user="postgres",
            password="2336",
            host="localhost",
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def get_all_alarm_data(conn):
    query = """
    SELECT 
        DATE(tm) as date,
        SUM(CASE WHEN aflag = 1 THEN 1 ELSE 0 END) as type1_count,
        SUM(CASE WHEN aflag = 2 THEN 1 ELSE 0 END) as type2_count
    FROM lane005_alarm
    WHERE aflag IN (1, 2)
    GROUP BY DATE(tm)
    ORDER BY DATE(tm) DESC;
    """
    
    try:
        with conn.cursor() as cur:
            print("Querying alarm data from lane005_alarm...")
            cur.execute(query)
            results = cur.fetchall()
            return results
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

def main():
    # Database connection
    conn = connect_to_db()
    if not conn:
        return

    try:
        # Get all alarm data
        results = get_all_alarm_data(conn)
        
        if results:
            # Convert results to DataFrame
            df = pd.DataFrame(results, columns=['date', 'type1_count', 'type2_count'])
            
            if df.empty:
                print("\nNo alarms found in the database")
                return
            
            # Calculate total alarms per day
            df['total'] = df['type1_count'] + df['type2_count']
            
            # Format the output
            print("\nDaily Alarm Summary:")
            for _, row in df.iterrows():
                type1 = int(row['type1_count'])
                type2 = int(row['type2_count'])
                total = int(row['total'])
                remain_text = "remains" if total == 1 else "remain"
                print(f"On {row['date'].strftime('%d %B %Y')}, {total} alarm{'' if total == 1 else 's'} {remain_text} (Type 1: {type1}, Type 2: {type2})")
            
            # Show overall statistics
            print("\nOverall Statistics:")
            print(f"Total days with alarms: {len(df)}")
            print(f"Total alarms: {df['total'].sum():.0f}")
            print(f"Average alarms per day: {df['total'].mean():.2f}")
            
            # Show summary by alarm type
            print("\nSummary by Alarm Type:")
            print(f"Type 1 alarms: {df['type1_count'].sum():.0f}")
            print(f"Type 2 alarms: {df['type2_count'].sum():.0f}")
            print(f"Total alarms: {df['total'].sum():.0f}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main() 