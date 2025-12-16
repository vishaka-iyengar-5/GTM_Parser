import pandas as pd
import os
import glob
import re

def combine_csv_files(input_folder, output_folder, output_filename):
    """
    Combines multiple CSV files in natural order based on filename patterns.
    
    Args:
        input_folder: Path to folder containing CSV files
        output_folder: Path where combined file should be saved
        output_filename: Name for the combined CSV file (without .csv extension)
    """
    
    # Get all CSV files from the input folder
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {input_folder}")
        return
    
    # Simple natural sort based on numbers in filename
    def extract_sort_number(filename):
        basename = os.path.basename(filename)
        
        # For "full_ecommerce_1-100.csv" -> return 1
        if "1-100" in basename:
            return 1
            
        # For "full_ecommerce_b2-2_" -> return 2
        if re.search(r'b(\d+)-\d+', basename):
            match = re.search(r'b(\d+)-\d+', basename)
            return int(match.group(1))
            
        # For files with range like "1200-1250" -> return 1200
        if re.search(r'(\d{4})-(\d{4})', basename):
            match = re.search(r'(\d{4})-(\d{4})', basename)
            return int(match.group(1))
            
        # Fallback: extract first number found
        numbers = re.findall(r'\d+', basename)
        return int(numbers[0]) if numbers else 999
    
    # Sort files by the extracted numbers
    csv_files.sort(key=extract_sort_number)
    
    print("Files will be combined in this order:")
    for i, file in enumerate(csv_files, 1):
        print(f"{i:2d}. {os.path.basename(file)}")
    
    # Read and combine files
    dataframes = []
    
    for i, file in enumerate(csv_files):
        print(f"Processing: {os.path.basename(file)}")
        
        try:
            if i == 0:
                # First file: keep header
                df = pd.read_csv(file)
            else:
                # Subsequent files: skip header
                df = pd.read_csv(file, skiprows=1, header=None)
                df.columns = dataframes[0].columns
            
            dataframes.append(df)
            print(f"  └─ Added {len(df)} rows")
            
        except Exception as e:
            print(f"  └─ Error reading {file}: {e}")
    
    if not dataframes:
        print("No valid CSV files were processed.")
        return
    
    # Combine all dataframes
    print("\nCombining all files...")
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # Create output directory if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Save combined file
    output_path = os.path.join(output_folder, f"{output_filename}.csv")
    combined_df.to_csv(output_path, index=False)
    
    print(f"\n✅ Success!")
    print(f"Combined {len(csv_files)} files")
    print(f"Saved to: {output_path}")
    print(f"Total rows: {len(combined_df)} (plus 1 header)")

# Main execution
if __name__ == "__main__":
    # Update these paths for different folders
    input_folder = "/Users/vishakaiyengar/GTMParser/output/DataCompilation/URLs1-1500results"
    output_folder = "/Users/vishakaiyengar/GTMParser/output/DataCompilation"
    output_filename = "1-1500URLs_Combined"
    
    combine_csv_files(input_folder, output_folder, output_filename)