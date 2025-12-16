import pandas as pd
from collections import Counter

print("Step 3: Event Frequency Analysis for Groups A & B...")

# =============================================================================
# READ CLEAN GROUP DATA
# =============================================================================

# Read the clean Group A and Group B CSV files
group_a_df = pd.read_csv('2.2.x-Group_A.csv')
group_b_df = pd.read_csv('2.2.x-Group_B.csv')

print(f"✓ Group A loaded: {len(group_a_df)} websites")
print(f"✓ Group B loaded: {len(group_b_df)} websites")

# =============================================================================
# GROUP A EVENT FREQUENCY ANALYSIS
# =============================================================================

print("\nAnalyzing Group A events...")

# Extract all events from Group A
group_a_events = []
group_a_websites_with_events = 0

for index, row in group_a_df.iterrows():
    gtm_events = row['gtm_events']
    if pd.notna(gtm_events) and gtm_events != 'not_applicable' and str(gtm_events).strip() != '':
        group_a_websites_with_events += 1
        # Split by comma and clean each event
        events = [event.strip() for event in str(gtm_events).split(',')]
        for event in events:
            if event and event != '':
                group_a_events.append(event)

# Count frequency and create DataFrame
group_a_event_counts = Counter(group_a_events)
group_a_unique_events = len(set(group_a_events))

# Create frequency table sorted by frequency (highest first)
group_a_freq_data = []
for event, frequency in group_a_event_counts.most_common():
    percentage = round((frequency / len(group_a_events)) * 100, 2)
    group_a_freq_data.append([event, frequency, f"{percentage}%"])

# Save Group A frequency table
group_a_freq_df = pd.DataFrame(group_a_freq_data, columns=['Event_Name', 'Frequency', 'Percentage'])
group_a_freq_df.to_csv('2.2.1-Group_A+EventFrequency.csv', index=False)
print("✓ Group A event frequency saved to: 2.2.1-Group_A+EventFrequency.csv")

# =============================================================================
# GROUP B EVENT FREQUENCY ANALYSIS  
# =============================================================================

print("Analyzing Group B events...")

# Extract all events from Group B
group_b_events = []
group_b_websites_with_events = 0

for index, row in group_b_df.iterrows():
    gtm_events = row['gtm_events']
    if pd.notna(gtm_events) and gtm_events != 'not_applicable' and str(gtm_events).strip() != '':
        group_b_websites_with_events += 1
        # Split by comma and clean each event
        events = [event.strip() for event in str(gtm_events).split(',')]
        for event in events:
            if event and event != '':
                group_b_events.append(event)

# Count frequency and create DataFrame
group_b_event_counts = Counter(group_b_events)
group_b_unique_events = len(set(group_b_events))

# Create frequency table sorted by frequency (highest first)
group_b_freq_data = []
for event, frequency in group_b_event_counts.most_common():
    percentage = round((frequency / len(group_b_events)) * 100, 2)
    group_b_freq_data.append([event, frequency, f"{percentage}%"])

# Save Group B frequency table
group_b_freq_df = pd.DataFrame(group_b_freq_data, columns=['Event_Name', 'Frequency', 'Percentage'])
group_b_freq_df.to_csv('2.2.2-Group_B+EventFrequency.csv', index=False)
print("✓ Group B event frequency saved to: 2.2.2-Group_B+EventFrequency.csv")

# =============================================================================
# CREATE COMPREHENSIVE SUMMARY FILE
# =============================================================================

print("Creating comprehensive summary...")

# Read existing summary content
try:
    with open('Groups_Summary.txt', 'r') as f:
        existing_summary = f.read()
except FileNotFoundError:
    existing_summary = "Previous summary not found."

# Create Step 3 summary content
step3_summary = f"""

STEP 3 - EVENT FREQUENCY ANALYSIS:
{'='*60}

Group A - Websites WITH consent mode:
- Websites with events: {group_a_websites_with_events}
- Total event occurrences: {len(group_a_events)}
- Unique events: {group_a_unique_events}
- Average events per website: {round(len(group_a_events)/group_a_websites_with_events, 2) if group_a_websites_with_events > 0 else 0}

Top 10 most frequent events in Group A:"""

for i, (event, freq) in enumerate(group_a_event_counts.most_common(10)):
    percentage = round((freq / len(group_a_events)) * 100, 2)
    step3_summary += f"\n  {i+1:2d}. {event} - {freq} times ({percentage}%)"

step3_summary += f"""

Group B - Websites WITHOUT consent mode:
- Websites with events: {group_b_websites_with_events}
- Total event occurrences: {len(group_b_events)}
- Unique events: {group_b_unique_events}
- Average events per website: {round(len(group_b_events)/group_b_websites_with_events, 2) if group_b_websites_with_events > 0 else 0}

Top 10 most frequent events in Group B:"""

for i, (event, freq) in enumerate(group_b_event_counts.most_common(10)):
    percentage = round((freq / len(group_b_events)) * 100, 2)
    step3_summary += f"\n  {i+1:2d}. {event} - {freq} times ({percentage}%)"

step3_summary += f"""

COMPARISON INSIGHTS:
{'='*60}
- Group A has {group_a_unique_events} unique events vs Group B has {group_b_unique_events} unique events
- Group A avg events/website: {round(len(group_a_events)/group_a_websites_with_events, 2) if group_a_websites_with_events > 0 else 0}
- Group B avg events/website: {round(len(group_b_events)/group_b_websites_with_events, 2) if group_b_websites_with_events > 0 else 0}

FILES CREATED IN STEP 3:
- 2.2.1-Group_A+EventFrequency.csv
- 2.2.2-Group_B+EventFrequency.csv
- 2.2.x-Summary.txt (this file)
"""

# Combine existing and new content
comprehensive_summary = existing_summary + step3_summary

# Save comprehensive summary
with open('2.2.x-Summary.txt', 'w') as f:
    f.write(comprehensive_summary)

print("✓ Comprehensive summary saved to: 2.2.x-Summary.txt")

# =============================================================================
# TERMINAL QUICK REVIEW
# =============================================================================

print(f"\n" + "="*60)
print("STEP 3 ANALYSIS COMPLETE")
print("="*60)
print(f"Group A (WITH consent mode):")
print(f"  - {group_a_websites_with_events} websites with events")
print(f"  - {len(group_a_events)} total events, {group_a_unique_events} unique")
print(f"  - Top event: {group_a_event_counts.most_common(1)[0][0]} ({group_a_event_counts.most_common(1)[0][1]} times)")

print(f"\nGroup B (WITHOUT consent mode):")
print(f"  - {group_b_websites_with_events} websites with events") 
print(f"  - {len(group_b_events)} total events, {group_b_unique_events} unique")
print(f"  - Top event: {group_b_event_counts.most_common(1)[0][0]} ({group_b_event_counts.most_common(1)[0][1]} times)")
print("="*60)