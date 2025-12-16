import pandas as pd

# Read the final combined CSV file - UPDATE THIS PATH
df = pd.read_csv('/Users/vishakaiyengar/GTMParser/output/DataCompilation/FINAL_DATA/final_combined_data_4500.csv')

print("Step 1: Overall GTM Events Analysis")
print("=" * 60)
print(f"Total websites analyzed: {len(df)}")

# Step 1: Parse through entire gtm_events column
all_unique_events = set()
total_event_entries = 0

for index, row in df.iterrows():
    gtm_events = row['gtm_events']
    if pd.notna(gtm_events) and gtm_events != 'not_applicable' and str(gtm_events).strip() != '':
        total_event_entries += 1
        # Split by comma and clean each event
        events = [event.strip() for event in str(gtm_events).split(',')]
        for event in events:
            if event and event != '':
                all_unique_events.add(event)

# Convert set to sorted list
unique_events_list = sorted(list(all_unique_events))

# Step 1 Results
print(f"\nGTM Events Analysis Results:")
print(f"Websites with GTM events: {total_event_entries}")
print(f"Total unique events found: {len(unique_events_list)}")

print(f"\nComplete list of unique GTM events:")
print(', '.join(unique_events_list))

# Display as a formatted table
print(f"\nUnique GTM Events Table:")
print("=" * 60)
for i, event in enumerate(unique_events_list, 1):
    print(f"{i:3d}. {event}")
print("=" * 60)