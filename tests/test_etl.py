"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""
import pandas as pd
import pytest
from etl_pipeline import transform, validate

def test_transform_filters_cancelled():
    """Create test DataFrames with a cancelled order. Confirm it's excluded."""
    # Create mock data
    data_dict = {
        "customers": pd.DataFrame({"customer_id": [1], "customer_name": ["Test"], "city": ["Amman"]}),
        "products": pd.DataFrame({"product_id": [10], "category": ["Tech"], "unit_price": [100.0]}),
        "orders": pd.DataFrame({
            "order_id": [1, 2], 
            "customer_id": [1, 1], 
            "status": ["completed", "cancelled"] # One valid, one cancelled
        }),
        "order_items": pd.DataFrame({
            "order_id": [1, 2], 
            "product_id": [10, 10], 
            "quantity": [1, 1]
        })
    }
    
    result_df = transform(data_dict)
    
    # Assertions: Only one order should remain (the completed one)
    assert len(result_df) == 1
    assert result_df.iloc[0]['total_orders'] == 1

def test_transform_filters_suspicious_quantity():
    """Create test DataFrames with quantity > 100. Confirm it's excluded."""
    # Create mock data
    data_dict = {
        "customers": pd.DataFrame({"customer_id": [1], "customer_name": ["Test"], "city": ["Amman"]}),
        "products": pd.DataFrame({"product_id": [10], "category": ["Tech"], "unit_price": [10.0]}),
        "orders": pd.DataFrame({"order_id": [1, 2], "customer_id": [1, 1], "status": ["completed", "completed"]}),
        "order_items": pd.DataFrame({
            "order_id": [1, 2], 
            "product_id": [10, 10], 
            "quantity": [5, 150] # 150 is suspicious (> 100)
        })
    }
    
    result_df = transform(data_dict)
    
    # Assertions: The row with quantity 150 should be filtered out
    assert len(result_df) == 1
    assert result_df.iloc[0]['total_orders'] == 1

def test_validate_catches_nulls():
    """Create a DataFrame with null customer_id. Confirm validate() raises ValueError."""
    # Create a DataFrame that violates the "No Nulls" rule
    invalid_df = pd.DataFrame({
        "customer_id": [None, 2], # Contains a Null
        "customer_name": ["Alice", "Bob"],
        "city": ["Amman", "Salt"],
        "total_orders": [1, 2],
        "total_revenue": [100.0, 200.0],
        "avg_order_value": [100.0, 100.0],
        "top_category": ["Tech", "Tech"]
    })
    
    # Assert that calling validate raises a ValueError
    with pytest.raises(ValueError, match="Critical check failed"):
        validate(invalid_df)