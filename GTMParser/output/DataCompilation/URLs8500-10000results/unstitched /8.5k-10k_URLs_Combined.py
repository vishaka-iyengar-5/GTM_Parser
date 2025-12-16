import pandas as pd
import os
import glob
import re

def combine_csv_files(input_folder, output_file):
    """
    Combines multiple CSV files in order based on URL ranges in filename.
    
    Args:
        input_folder: Path to folder containing CSV files
        output_file: Full path for the output combined CSV file
    """
    
    # Get all CSV files from the input folder
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {input_folder}")
        return
    
    print(f"Found {len(csv_files)} CSV files")
    
    # Sort files by the URL range numbers in filename
    def get_range_start(filename):
        # Extract the starting number from patterns like "8501-8550.csv" or "9951-10000.csv"
        match = re.search(r'(\d{4,5})-\d{4,5}\.csv$', filename)
        if match:
            return int(match.group(1))
        return 0
    
    csv_files.sort(key=get_range_start)
    
    print("\nFiles will be combined in this order:")
    for i, file in enumerate(csv_files, 1):
        print(f"{i:2d}. {os.path.basename(file)}")
    
    # Read and combine files
    dataframes = []
    
    for i, file in enumerate(csv_files):
        print(f"\nProcessing: {os.path.basename(file)}")
        
        try:
            if i == 0:
                # First file: keep header
                df = pd.read_csv(file)
                print(f"  └─ Added {len(df)} rows (with header)")
            else:
                # Subsequent files: skip header
                df = pd.read_csv(file, skiprows=1, header=None)
                df.columns = dataframes[0].columns
                print(f"  └─ Added {len(df)} rows (header skipped)")
            
            dataframes.append(df)
            
        except Exception as e:
            print(f"  └─ Error reading {file}: {e}")
    
    if not dataframes:
        print("No valid CSV files were processed.")
        return
    
    # Combine all dataframes
    print("\nCombining all files...")
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # Save combined file
    combined_df.to_csv(output_file, index=False)
    
    print(f"\n✅ Success!")
    print(f"Combined {len(csv_files)} files")
    print(f"Saved to: {output_file}")
    print(f"Total rows: {len(combined_df)} (including header)")

# Main execution
if __name__ == "__main__":
    # Set your paths here
    input_folder = "."  # Current directory since you're running from unstitched folder
    output_file = "/Users/vishakaiyengar/GTMParser/output/DataCompilation/8500-10000URLs_Combined.csv"
    
    combine_csv_files(input_folder, output_file)