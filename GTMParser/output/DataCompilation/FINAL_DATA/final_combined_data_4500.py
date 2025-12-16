import pandas as pd
import os

def combine_final_sets(input_folder, output_file):
    """
    Combines the 3 final CSV sets into one master file.
    
    Args:
        input_folder: Path to folder containing the 3 set files
        output_file: Full path for the final combined CSV file
    """
    
    # Define the files in order
    files_to_combine = [
        "set_1.csv",
        "set_2.csv", 
        "set_3.csv"
    ]
    
    print("Combining final data sets...")
    print("=" * 50)
    
    dataframes = []
    total_rows = 0
    
    for i, filename in enumerate(files_to_combine):
        file_path = os.path.join(input_folder, filename)
        
        print(f"\nProcessing: {filename}")
        
        if not os.path.exists(file_path):
            print(f"  ❌ File not found: {file_path}")
            continue
            
        try:
            if i == 0:
                # First file: keep header
                df = pd.read_csv(file_path)
                rows_added = len(df)
                print(f"  ✅ Added {rows_added} rows (with header)")
            else:
                # Subsequent files: skip header
                df = pd.read_csv(file_path, skiprows=1, header=None)
                df.columns = dataframes[0].columns
                rows_added = len(df)
                print(f"  ✅ Added {rows_added} rows (header skipped)")
            
            dataframes.append(df)
            total_rows += rows_added
            print(f"     Running total: {total_rows} rows")
            
        except Exception as e:
            print(f"  ❌ Error reading {filename}: {e}")
    
    if not dataframes:
        print("\n❌ No valid files were processed.")
        return
    
    # Combine all dataframes
    print("\n" + "=" * 50)
    print("Combining all data...")
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Save combined file
    combined_df.to_csv(output_file, index=False)
    
    print(f"\n🎉 SUCCESS!")
    print("=" * 50)
    print(f"📁 Combined {len(dataframes)} files")
    print(f"📄 Final file: {output_file}")
    print(f"📊 Total rows: {len(combined_df)} (including header)")
    print(f"📈 Data rows: {len(combined_df)} + header = {len(combined_df) + 1} total lines")
    
    # Verify file was created
    if os.path.exists(output_file):
        file_size = os.path.getsize(output_file)
        print(f"💾 File size: {file_size:,} bytes")
        print(f"✅ File successfully created!")
    else:
        print(f"❌ Error: Output file was not created")

# Main execution
if __name__ == "__main__":
    # Set your paths
    input_folder = "/Users/vishakaiyengar/GTMParser/output/DataCompilation/FINAL_DATA"
    output_file = "/Users/vishakaiyengar/GTMParser/output/DataCompilation/FINAL_DATA/final_combined_data_4500.csv"
    
    combine_final_sets(input_folder, output_file)