import pandas as pd

def append_ecommerce_columns():
    """
    Simple column append: Take first 1500 rows from ecommerce file and 
    append the 3 ecommerce columns to the Combined file (row by row)
    """
    
    print("Starting simple column append...")
    
    # File paths
    ecommerce_file = '2025-06-01 10k Unique e-commerce websites -csv.csv'
    combined_file = '1-1500URLs_Combined.csv'
    output_file = '1-1.5k+10k_merged.csv'
    
    # Read the ecommerce file (skip job ID header, take first 1500 rows)
    print("Reading ecommerce data...")
    ecommerce_df = pd.read_csv(ecommerce_file, skiprows=1, nrows=1500)
    print(f"Loaded first {len(ecommerce_df)} rows from ecommerce file")
    
    # Read the combined file
    print("Reading combined data...")
    combined_df = pd.read_csv(combined_file)
    print(f"Loaded {len(combined_df)} rows from combined file")
    
    # Verify row counts match
    if len(ecommerce_df) != len(combined_df):
        print(f"⚠️  Warning: Row count mismatch!")
        print(f"   Ecommerce: {len(ecommerce_df)} rows")
        print(f"   Combined: {len(combined_df)} rows")
        return None
    
    # Extract the 3 ecommerce columns we want
    ecommerce_columns = ecommerce_df[['crawl_date', 'ecommerce_platform', 'best_popularity_rank']].copy()
    
    # Reset index to ensure proper alignment
    ecommerce_columns.reset_index(drop=True, inplace=True)
    combined_df.reset_index(drop=True, inplace=True)
    
    # Append the ecommerce columns to the combined dataframe
    print("Appending ecommerce columns...")
    merged_df = pd.concat([combined_df, ecommerce_columns], axis=1)
    
    # Verify results
    print(f"✅ Success!")
    print(f"   Original combined columns: {len(combined_df.columns)}")
    print(f"   Added ecommerce columns: {len(ecommerce_columns.columns)}")
    print(f"   Final columns: {len(merged_df.columns)}")
    print(f"   Final rows: {len(merged_df)}")
    
    # Save the result
    print(f"Saving to: {output_file}")
    merged_df.to_csv(output_file, index=False)
    
    # Show column names
    print(f"\nFinal column names:")
    for i, col in enumerate(merged_df.columns, 1):
        print(f"   {i:2d}. {col}")
    
    # Show sample data
    print(f"\nSample of merged data:")
    sample_cols = ['url', 'ecommerce_platform', 'best_popularity_rank', 'gtm_detected']
    print(merged_df[sample_cols].head(3).to_string())
    
    print(f"\n🎉 Column append completed successfully!")
    print(f"Output: {output_file}")
    
    return merged_df

if __name__ == "__main__":
    result = append_ecommerce_columns()