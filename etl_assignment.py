import sqlite3
import pandas as pd

DB_PATH = 'Data Engineer_ETL Assignment.db'
CSV_PATH_SQL = 'output_sql.csv'
CSV_PATH_PANDAS = 'output_python.csv'

# 1. Connect to the SQLite3 database
def get_connection():
    return sqlite3.connect(DB_PATH)

# 2. Solution using SQL

def extract_with_sql():
    query = '''
    SELECT c.customer_id AS Customer, c.age AS Age, i.item_name AS Item, SUM(COALESCE(o.quantity, 0)) AS Quantity
    FROM customers c
    JOIN sales s ON c.customer_id = s.customer_id
    JOIN orders o ON s.sales_id = o.sales_id
    JOIN items i ON o.item_id = i.item_id
    WHERE c.age BETWEEN 18 AND 35
      AND o.quantity IS NOT NULL
    GROUP BY c.customer_id, c.age, i.item_name
    HAVING SUM(COALESCE(o.quantity, 0)) > 0
    ORDER BY c.customer_id, i.item_name
    '''
    with get_connection() as conn:
        df = pd.read_sql_query(query, conn)
    df.to_csv(CSV_PATH_SQL, sep=';', index=False)
    print(f"SQL solution written to {CSV_PATH_SQL}")

# 2. Solution Using Pandas

def extract_with_pandas():
    with get_connection() as conn:
        customers = pd.read_sql_query('SELECT * FROM customers', conn)
        sales = pd.read_sql_query('SELECT * FROM sales', conn)
        orders = pd.read_sql_query('SELECT * FROM orders', conn)
        items = pd.read_sql_query('SELECT * FROM items', conn)

    # Merge tables
    df = (orders
          .merge(sales, on='sales_id')
          .merge(customers, on='customer_id')
          .merge(items, on='item_id'))
    # Filter age
    df = df[(df['age'] >= 18) & (df['age'] <= 35)]
    # Remove NULL quantities
    df = df[df['quantity'].notnull()]
    # Group and sum
    result = (df.groupby(['customer_id', 'age', 'item_name'], as_index=False)
                .agg({'quantity': 'sum'}))
    # Remove zero quantities
    result = result[result['quantity'] > 0]
    # Rename columns
    result = result.rename(columns={
        'customer_id': 'Customer',
        'age': 'Age',
        'item_name': 'Item',
        'quantity': 'Quantity'
    })
    # Ensure integer quantities
    result['Quantity'] = result['Quantity'].astype(int)
    # Sort
    result = result.sort_values(['Customer', 'Item'])
    result.to_csv(CSV_PATH_PANDAS, sep=';', index=False)
    print(f"Python (Pandas) solution written to {CSV_PATH_PANDAS}")

if __name__ == '__main__':
    extract_with_sql()
    extract_with_pandas()
