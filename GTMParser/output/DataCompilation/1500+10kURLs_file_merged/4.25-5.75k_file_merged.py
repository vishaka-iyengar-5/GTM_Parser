import pandas as pd

def append_ecommerce_columns_set2():
    """
    Simple column append for Set 2: 
    - Combined file row 1 maps to ecommerce file line 4253
    - Take 1500 rows starting from line 4253 in ecommerce file
    - Append the 3 ecommerce columns to the Combined file (row by row)
    """
    
    print("Starting simple column append for Set 2...")
    
    # File paths
    ecommerce_file = '2025-06-01 10k Unique e-commerce websites -csv.csv'
    combined_file = '4250-5750URLs_Combined.csv'
    output_file = 'set_2.csv'
    
    # Read the ecommerce file starting from line 4253, take 1500 rows
    # We need to specify the correct column names since we're skipping the header
    print("Reading ecommerce data starting from line 4253...")
    ecommerce_df = pd.read_csv(
        ecommerce_file, 
        skiprows=4252, 
        nrows=1500, 
        header=None,  # No header in the skipped data
        names=['website_url', 'crawl_date', 'ecommerce_platform', 'best_popularity_rank']  # Specify column names
    )
    print(f"Loaded {len(ecommerce_df)} rows from ecommerce file (starting at line 4253)")
    
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
    
    # Check if ecommerce data has the expected columns
    expected_cols = ['website_url', 'crawl_date', 'ecommerce_platform', 'best_popularity_rank']
    missing_cols = [col for col in expected_cols if col not in ecommerce_df.columns]
    if missing_cols:
        print(f"❌ Missing columns in ecommerce data: {missing_cols}")
        print(f"Available columns: {list(ecommerce_df.columns)}")
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
    
    # Show sample data to verify mapping
    print(f"\nSample of merged data (first 3 rows):")
    sample_cols = ['url', 'ecommerce_platform', 'best_popularity_rank', 'gtm_detected']
    print(merged_df[sample_cols].head(3).to_string())
    
    # Show URL comparison to verify alignment
    print(f"\nURL alignment verification:")
    print("Combined file URLs vs Ecommerce file URLs:")
    for i in range(3):
        combined_url = merged_df['url'].iloc[i] if 'url' in merged_df.columns else 'N/A'
        ecommerce_url = ecommerce_df['website_url'].iloc[i] if 'website_url' in ecommerce_df.columns else 'N/A'
        match = "✅" if combined_url == ecommerce_url else "❌"
        print(f"   Row {i+1}: {match}")
        print(f"      Combined: {combined_url}")
        print(f"      Ecommerce: {ecommerce_url}")
    
    print(f"\n🎉 Column append for Set 2 completed successfully!")
    print(f"Output: {output_file}")
    
    return merged_df

if __name__ == "__main__":
    result = append_ecommerce_columns_set2()