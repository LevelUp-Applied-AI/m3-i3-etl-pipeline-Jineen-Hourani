
"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames.

    Args:
        engine: SQLAlchemy engine connected to the amman_market database

    Returns:
        dict: {"customers": df, "products": df, "orders": df, "order_items": df}
    """
    print("--- Stage 1: Extraction ---")
    tables = ["customers", "products", "orders", "order_items"]
    data_dict = {}
    
    for table in tables:
        data_dict[table] = pd.read_sql_table(table, engine)
        print(f"Extracted {len(data_dict[table])} rows from {table}")
        
    return data_dict


def transform(data_dict):
    """Transform raw data into customer-level analytics summary.

    Steps:
    1. Join orders with order_items and products
    2. Compute line_total (quantity * unit_price)
    3. Filter out cancelled orders (status = 'cancelled')
    4. Filter out suspicious quantities (quantity > 100)
    5. Aggregate to customer level: total_orders, total_revenue,
       avg_order_value, top_category

    Args:
        data_dict: dict of DataFrames from extract()

    Returns:
        DataFrame: customer-level summary with columns:
            customer_id, customer_name, city, total_orders,
            total_revenue, avg_order_value, top_category
    """
    print("\n--- Stage 2: Transformation ---")
    
    df_customers = data_dict['customers']
    df_products = data_dict['products']
    df_orders = data_dict['orders']
    df_items = data_dict['order_items']

    # Join tables
    df_merged = pd.merge(df_orders, df_items, on="order_id")
    df_merged = pd.merge(df_merged, df_products, on="product_id")
    
    # Calculate line totals
    df_merged['unit_price'] = df_merged['unit_price'].astype(float)
    df_merged['line_total'] = df_merged['quantity'] * df_merged['unit_price']

    # Data filtering
    df_filtered = df_merged[
        (df_merged['status'] != 'cancelled') & 
        (df_merged['quantity'] <= 100)
    ].copy()
    
    print(f"Rows after filtering: {len(df_filtered)}")

    # Basic customer aggregation
    customer_summary = df_filtered.groupby('customer_id').agg(
        total_orders=('order_id', 'nunique'),
        total_revenue=('line_total', 'sum')
    ).reset_index()

    # Calculate average order value
    customer_summary['avg_order_value'] = customer_summary['total_revenue'] / customer_summary['total_orders']

    # Find top category per customer
    cat_revenue = df_filtered.groupby(['customer_id', 'category'])['line_total'].sum().reset_index()
    top_cat = cat_revenue.sort_values(['customer_id', 'line_total'], ascending=[True, False])\
                         .drop_duplicates('customer_id')[['customer_id', 'category']]
    top_cat.columns = ['customer_id', 'top_category']

    # Final merge with customer details
    final_df = pd.merge(customer_summary, top_cat, on='customer_id')
    final_df = pd.merge(final_df, df_customers[['customer_id', 'customer_name', 'city']], on='customer_id')

    column_order = ['customer_id', 'customer_name', 'city', 'total_orders', 'total_revenue', 'avg_order_value', 'top_category']
    return final_df[column_order]


def validate(df):
    """Run data quality checks on the transformed DataFrame.

    Checks:
    - No nulls in customer_id or customer_name
    - total_revenue > 0 for all customers
    - No duplicate customer_ids
    - total_orders > 0 for all customers

    Args:
        df: transformed customer summary DataFrame

    Returns:
        dict: {check_name: bool} for each check

    Raises:
        ValueError: if any critical check fails
    """
    print("\n--- Stage 3: Validation ---")
    checks = {
        "No nulls in customer_id or customer_name": df[['customer_id', 'customer_name']].isnull().sum().sum() == 0,
        "total_revenue > 0 for all customers": (df['total_revenue'] > 0).all(),
        "No duplicate customer_id values": df['customer_id'].is_unique,
        "total_orders > 0 for all customers": (df['total_orders'] > 0).all()
    }
    
    for check, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"{check}: {status}")
        if not passed:
            raise ValueError(f"Critical check failed: {check}")
            
    return checks


def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file.

    Args:
        df: validated customer summary DataFrame
        engine: SQLAlchemy engine
        csv_path: path for CSV output
    """
    print("\n--- Stage 4: Loading ---")
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    # Load to Database
    df.to_sql('customer_analytics', engine, if_exists='replace', index=False)
    
    # Save to CSV
    df.to_csv(csv_path, index=False)
    
    print(f"Loaded {len(df)} rows to database and {csv_path}")


def main():
    """Orchestrate the ETL pipeline: extract -> transform -> validate -> load."""
    # Database connection setup
    db_url = "postgresql+psycopg://postgres:postgres@localhost:5432/amman_market"
    engine = create_engine(db_url)
    
    try:
        # Step 1: Extract
        data = extract(engine)
        
        # Step 2: Transform
        transformed_df = transform(data)
        
        # Step 3: Validate
        validate(transformed_df)
        
        # Step 4: Load
        load(transformed_df, engine, "output/customer_analytics.csv")
        
        print("\nETL Pipeline finished successfully.")
        
    except Exception as e:
        print(f"\nETL Pipeline failed: {e}")


if __name__ == "__main__":
    main()
