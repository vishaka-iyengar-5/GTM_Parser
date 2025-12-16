import pandas as pd
import os
import glob
import re

def combine_csv_files(input_folder, output_folder, output_filename, min_range_start=None):
    """
    Combines multiple CSV files in order based on URL ranges in filename.
    
    Args:
        input_folder: Path to folder containing CSV files
        output_folder: Path where combined file should be saved
        output_filename: Name for the combined CSV file (without .csv extension)
        min_range_start: Minimum range start number to include (e.g., 4251)
    """
    
    # Get all CSV files from the input folder
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {input_folder}")
        return
    
    # Filter files with URL range pattern and optionally by minimum range
    filtered_files = []
    range_pattern = r'- (\d{4})-(\d{4})\.csv$'
    
    for file in csv_files:
        basename = os.path.basename(file)
        match = re.search(range_pattern, basename)
        if match:
            start_range = int(match.group(1))
            end_range = int(match.group(2))
            
            # Apply minimum range filter if specified
            if min_range_start is None or start_range >= min_range_start:
                filtered_files.append((file, start_range, end_range))
    
    if not filtered_files:
        print(f"No files found matching the range pattern (min start: {min_range_start})")
        return
    
    # Sort by starting range number
    filtered_files.sort(key=lambda x: x[1])
    
    print(f"Found {len(filtered_files)} files to combine:")
    for i, (file, start, end) in enumerate(filtered_files, 1):
        print(f"{i:2d}. {os.path.basename(file)} (URLs {start}-{end})")
    
    # Read and combine files
    dataframes = []
    
    for i, (file, start, end) in enumerate(filtered_files):
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
    
    # Check for and remove duplicates
    initial_count = len(combined_df)
    combined_df = combined_df.drop_duplicates()
    duplicate_count = initial_count - len(combined_df)
    
    if duplicate_count > 0:
        print(f"⚠️  Removed {duplicate_count} duplicate rows")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Save combined file
    output_path = os.path.join(output_folder, f"{output_filename}.csv")
    combined_df.to_csv(output_path, index=False)
    
    print(f"\n✅ Success!")
    print(f"Combined {len(filtered_files)} files")
    print(f"URL range: {filtered_files[0][1]}-{filtered_files[-1][2]}")
    print(f"Saved to: {output_path}")
    print(f"Total rows: {len(combined_df)} (plus 1 header)")

# Main execution
if __name__ == "__main__":
    # Configuration for URLs 8501-10000
    input_folder = "/Users/vishakaiyengar/GTMParser/output/DataCompilation/URLs8500-10000results/unstitched"
    output_folder = "/Users/vishakaiyengar/GTMParser/output/DataCompilation"
    output_filename = "8500-10000URLs_Combined"
    min_range_start = 8501  # Include files starting from 8501
    
    combine_csv_files(input_folder, output_folder, output_filename, min_range_start)