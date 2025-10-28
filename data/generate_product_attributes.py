# Credit Claude Sonnet 4.5 for help with creating this Script.

import os
import pandas as pd
from sqlalchemy import create_engine, text


def get_mysql_engine():
    """Create MySQL engine with credentials from environment or defaults."""
    uid = os.environ.get("MYSQL_UID", "root")
    pwd = os.environ.get("MYSQL_PWD", "<YOUR_PASSWORD>")
    host = os.environ.get("MYSQL_HOST", "localhost")
    db = os.environ.get("MYSQL_DB", "adventureworks")
    conn_str = f"mysql+pymysql://{uid}:{pwd}@{host}/{db}"
    return create_engine(conn_str, pool_recycle=3600)


def fetch_product_base():
    """Fetch product base data from MySQL."""
    sql = """
    SELECT 
      p.ProductID,
      p.Name AS product_name,
      p.ListPrice,
      p.StandardCost,
      p.Size,
      p.SizeUnitMeasureCode,
      p.Weight,
      p.WeightUnitMeasureCode,
      p.SellStartDate,
      p.SellEndDate,
      p.SafetyStockLevel,
      p.ReorderPoint,
      sc.Name AS subcategory_name,
      c.Name  AS category_name
    FROM product p
    LEFT JOIN productsubcategory sc ON p.ProductSubcategoryID = sc.ProductSubcategoryID
    LEFT JOIN productcategory c ON sc.ProductCategoryID = c.ProductCategoryID;
    """
    engine = get_mysql_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    return df


def get_season(month):
    """Return season name for a given month number."""
    if month in [12, 1, 2]:
        return "Winter"
    elif month in [3, 4, 5]:
        return "Spring"
    elif month in [6, 7, 8]:
        return "Summer"
    elif month in [9, 10, 11]:
        return "Fall"
    else:
        return None


def get_price_band(price):
    """Return price band category based on list price."""
    if price <= 100:
        return "0-100"
    elif price <= 500:
        return "100-500"
    elif price <= 1500:
        return "500-1500"
    else:
        return "1500+"


def get_brand_tier(price):
    """Return brand tier based on list price."""
    if price < 100:
        return "Budget"
    elif price < 500:
        return "Standard"
    elif price < 1500:
        return "Premium"
    else:
        return "Luxury"


def get_marketing_segment(category):
    """Map product category to marketing segment."""
    mapping = {
        "Bikes": "Performance",
        "Components": "Components",
        "Clothing": "Apparel",
        "Accessories": "Accessories",
    }
    return mapping.get(category, None)


def parse_size(size_value):
    """Try to parse size as a number, return None if not possible."""
    try:
        return float(size_value)
    except (ValueError, TypeError):
        return None


def get_size_category(size_value):
    """Categorize size into Small/Medium/Large."""
    size_num = parse_size(size_value)
    if size_num is None:
        return None
    elif size_num <= 48:
        return "Small"
    elif size_num <= 56:
        return "Medium"
    else:
        return "Large"


def convert_weight_to_kg(weight, unit_code):
    """Convert weight to kilograms based on unit code."""
    if pd.isna(weight):
        return None
    
    try:
        weight_val = float(weight)
        unit = str(unit_code).strip().upper() if pd.notna(unit_code) else ""
        
        if unit.startswith("LB"):
            return round(weight_val * 0.453592, 2)
        elif unit.startswith("G"):
            return round(weight_val / 1000.0, 2)
        else:
            return round(weight_val, 2)
    except (ValueError, TypeError):
        return None


def calculate_price_ratio(list_price, standard_cost):
    """Calculate MSRP to cost ratio."""
    if pd.isna(standard_cost) or standard_cost <= 0:
        return None
    try:
        return round(float(list_price) / float(standard_cost), 2)
    except (ValueError, TypeError, ZeroDivisionError):
        return None


def derive_attributes(df):
    """Derive marketing and technical attributes from product base data."""
    results = []
    
    for _, row in df.iterrows():
        # Parse dates
        sell_start = pd.to_datetime(row["SellStartDate"])
        sell_end = pd.to_datetime(row["SellEndDate"])
        
        # Launch year and season
        launch_year = sell_start.year if pd.notna(sell_start) else None
        month = sell_start.month if pd.notna(sell_start) else None
        season = get_season(month) if month else None
        
        # Price-based attributes
        price = row["ListPrice"]
        price_band = get_price_band(price)
        brand_tier = get_brand_tier(price)
        
        # Marketing segment
        marketing_segment = get_marketing_segment(row["category_name"])
        
        # Online-only flag (accessories <= $100)
        online_only = row["category_name"] == "Accessories" and price <= 100
        
        # Discontinued flag
        is_discontinued = pd.notna(sell_end)
        
        # Size category
        size_category = get_size_category(row["Size"])
        
        # Weight in kg
        weight_kg = convert_weight_to_kg(row["Weight"], row["WeightUnitMeasureCode"])
        
        # MSRP to cost ratio
        msrp_ratio = calculate_price_ratio(price, row["StandardCost"])
        
        # Build output row
        results.append({
            "ProductID": row["ProductID"],
            "MarketingSegment": marketing_segment,
            "BrandTier": brand_tier,
            "PriceBand": price_band,
            "OnlineOnly": online_only,
            "Season": season,
            "LaunchYear": launch_year,
            "SizeCategory": size_category,
            "WeightKg": weight_kg,
            "IsDiscontinued": is_discontinued,
            "SafetyStockLevel": row["SafetyStockLevel"],
            "ReorderPoint": row["ReorderPoint"],
            "MsrpToCostRatio": msrp_ratio,
        })
    
    return pd.DataFrame(results)


def main():
    """Main function to generate product_attributes.csv from MySQL."""
    df = fetch_product_base()
    attrs = derive_attributes(df)    
    out_path = os.path.join(os.path.dirname(__file__), "product_attributes.csv")
    attrs.to_csv(out_path, index=False)

    print(f"Wrote {len(attrs)} rows to {out_path}")


if __name__ == "__main__":
    main()


