import pandas as pd
from collections import Counter

# Read the final combined CSV file
df = pd.read_csv('/Users/vishakaiyengar/GTMParser/output/DataCompilation/FINAL_DATA/final_combined_data_4500.csv')

print("Processing GTM Events Analysis...")

# =============================================================================
# STEP 1: Overall GTM Events Analysis
# =============================================================================

print("Step 1: Analyzing unique GTM events...")

# Parse through entire gtm_events column
all_events = []
total_event_entries = 0

for index, row in df.iterrows():
    gtm_events = row['gtm_events']
    if pd.notna(gtm_events) and gtm_events != 'not_applicable' and str(gtm_events).strip() != '':
        total_event_entries += 1
        # Split by comma and clean each event
        events = [event.strip() for event in str(gtm_events).split(',')]
        for event in events:
            if event and event != '':
                all_events.append(event)

# Count frequency of each event
event_counts = Counter(all_events)
unique_events_list = sorted(list(set(all_events)))

# Create Step 1 results DataFrame (CLEAN - no summary mixed in)
step1_data = []
for event in unique_events_list:
    frequency = event_counts[event]
    percentage = round((frequency / len(all_events)) * 100, 2)
    step1_data.append([event, frequency, f"{percentage}%"])

# Save Step 1 results (clean CSV)
step1_df = pd.DataFrame(step1_data, columns=['Event_Name', 'Frequency', 'Percentage'])
step1_df.to_csv('Unique_GTM_events.csv', index=False)
print("✓ Step 1 results saved to: Unique_GTM_events.csv")

# =============================================================================
# STEP 2: Data Segregation into Groups A, B, C
# =============================================================================

print("Step 2: Segregating data into groups...")

# Group A: WITH consent mode (gtm_detected=True AND consent_mode=True)
group_a = df[(df['gtm_detected'] == True) & (df['consent_mode'] == True)].copy()

# Group B: WITHOUT consent mode (gtm_detected=True AND consent_mode=False)  
group_b = df[(df['gtm_detected'] == True) & (df['consent_mode'] == False)].copy()

# Group C: NO GTM (gtm_detected=False)
group_c = df[df['gtm_detected'] == False].copy()

# Save clean CSV files (no summary rows mixed in)
group_a.to_csv('2.2.x-Group_A.csv', index=False)
group_b.to_csv('2.2.x-Group_B.csv', index=False)
group_c.to_csv('2.2.x-Group_C.csv', index=False)

print("✓ Group A results saved to: 2.2.x-Group_A.csv")
print("✓ Group B results saved to: 2.2.x-Group_B.csv") 
print("✓ Group C results saved to: 2.2.x-Group_C.csv")

# =============================================================================
# CREATE SEPARATE SUMMARY FILE
# =============================================================================

print("Creating summary file...")

# Create comprehensive summary
summary_content = f"""GTM EVENTS ANALYSIS SUMMARY
{'='*60}

OVERALL ANALYSIS (Step 1):
- Total websites analyzed: {len(df)}
- Websites with GTM events: {total_event_entries}
- Total unique events found: {len(unique_events_list)}
- Total event occurrences: {len(all_events)}

DATA SEGREGATION (Step 2):
{'='*60}

Group A - Websites WITH consent mode (GTM=True AND Consent=True):
- Total websites: {len(group_a)}
- Percentage of total: {round((len(group_a)/len(df))*100, 2)}%
- File: 2.2.x-Group_A.csv

Group B - Websites WITHOUT consent mode (GTM=True AND Consent=False):
- Total websites: {len(group_b)}
- Percentage of total: {round((len(group_b)/len(df))*100, 2)}%
- File: 2.2.x-Group_B.csv

Group C - Websites with NO GTM (GTM=False):
- Total websites: {len(group_c)}
- Percentage of total: {round((len(group_c)/len(df))*100, 2)}%
- File: 2.2.x-Group_C.csv

VERIFICATION:
- Group A + Group B + Group C = {len(group_a) + len(group_b) + len(group_c)}
- Original total = {len(df)}
- Match: {'✓' if len(group_a) + len(group_b) + len(group_c) == len(df) else '✗'}

FILES CREATED:
{'='*60}
1. Unique_GTM_events.csv - All unique events with frequencies
2. 2.2.x-Group_A.csv - Clean data for consent mode users
3. 2.2.x-Group_B.csv - Clean data for non-consent mode users  
4. 2.2.x-Group_C.csv - Clean data for non-GTM users
5. Groups_Summary.txt - This summary file
"""

# Save summary to text file
with open('Groups_Summary.txt', 'w') as f:
    f.write(summary_content)

print("✓ Summary saved to: Groups_Summary.txt")

# Print final summary to terminal
print(f"\n" + "="*60)
print("ANALYSIS COMPLETE - CLEAN FILES CREATED")
print("="*60)
print(f"Total websites: {len(df)}")
print(f"Group A (WITH consent mode): {len(group_a)} websites ({round((len(group_a)/len(df))*100, 1)}%)")
print(f"Group B (WITHOUT consent mode): {len(group_b)} websites ({round((len(group_b)/len(df))*100, 1)}%)")
print(f"Group C (NO GTM): {len(group_c)} websites ({round((len(group_c)/len(df))*100, 1)}%)")
print(f"Unique GTM events found: {len(unique_events_list)}")
print("="*60)
print("Ready for Step 3 - Event Frequency Analysis!")