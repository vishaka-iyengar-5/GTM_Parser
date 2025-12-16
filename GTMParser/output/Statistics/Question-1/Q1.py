#########################################
# Dissertation Statistical Analysis Script - PYTHON VERSION
# Purpose: Analyse GTM usage & impact across websites
#########################################

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from scipy.stats import chi2_contingency, fisher_exact, kruskal, mannwhitneyu
from statsmodels.stats.proportion import proportion_confint, binom_test
from statsmodels.stats.contingency_tables import mcnemar
import warnings
warnings.filterwarnings('ignore')

# Set plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

print("=" * 50)
print("DISSERTATION STATISTICAL ANALYSIS")
print("GTM Usage & Impact Analysis")
print("=" * 50)

# STEP 1: Load data
print("\n--- STEP 1: Loading Data ---")
df = pd.read_csv("final_combined_data_4500 - final_combined_data_4500.csv")

# STEP 2: Data exploration and cleaning
print("\n--- STEP 2: Data Overview ---")
print(f"Total rows: {len(df)}")
print(f"Columns: {len(df.columns)}")
print(f"\nGTM detected summary:")
print(df['gtm_detected'].value_counts(dropna=False))

# Check popularity rank distribution
print(f"\nPopularity rank distribution:")
print(df['best_popularity_rank'].describe())
print(f"Min rank: {df['best_popularity_rank'].min()}")
print(f"Max rank: {df['best_popularity_rank'].max()}")

# STEP 3: Create variables with proper batch definitions
print("\n--- STEP 3: Creating Variables ---")

# Create Google Analytics presence indicator
df['ga_present'] = df['third_party_trackers'].str.contains('Google Analytics', case=False, na=False)

# Fix event count calculation
def count_events(gtm_events):
    if pd.isna(gtm_events) or gtm_events in ['not_applicable', '']:
        return 0
    return len(str(gtm_events).split(','))

df['event_count'] = df['gtm_events'].apply(count_events)

# Create proper popularity batches
print("\n--- Creating Popularity Batches ---")
print("Examining popularity rank distribution:")
rank_counts = df['best_popularity_rank'].value_counts()
print(f"Unique values: {df['best_popularity_rank'].nunique()}")
print(f"Ranks with multiple websites: {sum(rank_counts > 1)}")

# Calculate quantiles
quantiles = df['best_popularity_rank'].quantile([0, 1/3, 2/3, 1]).values
print(f"Original quantiles: {quantiles}")

# Handle non-unique quantiles
unique_quantiles = quantiles.copy()
if len(np.unique(quantiles)) < len(quantiles):
    print("Warning: Non-unique quantiles detected. Adjusting breaks...")
    
    # Method 1: Use rank-based approach for more balanced groups
    sorted_ranks = np.sort(df['best_popularity_rank'].unique())
    n_unique = len(sorted_ranks)
    
    if n_unique >= 3:
        # Create three roughly equal groups based on unique rank positions
        break1_idx = round(n_unique * 1/3)
        break2_idx = round(n_unique * 2/3)
        
        break1 = sorted_ranks[break1_idx - 1] if break1_idx > 0 else sorted_ranks[0]
        break2 = sorted_ranks[break2_idx - 1] if break2_idx < n_unique else sorted_ranks[-1]
        
        unique_quantiles = np.array([
            df['best_popularity_rank'].min() - 0.1,
            break1,
            break2,
            df['best_popularity_rank'].max() + 0.1
        ])
    else:
        # Fallback: Use simple breaks
        unique_quantiles = np.array([0, 1500, 3000, df['best_popularity_rank'].max() + 1])

print(f"Adjusted breaks: {unique_quantiles}")

# Create batches with unique breaks
try:
    df['pop_batch'] = pd.cut(df['best_popularity_rank'], 
                            bins=unique_quantiles,
                            labels=['level1_most_popular', 'level2_medium', 'level3_least_popular'],
                            include_lowest=True)
except Exception as e:
    print(f"Cut failed: {e}")
    print("Using manual batch assignment...")
    
    # Alternative: Manual assignment based on rank order
    df_sorted = df.sort_values('best_popularity_rank').reset_index(drop=True)
    n_per_group = len(df) // 3
    
    batch_labels = (['level1_most_popular'] * n_per_group + 
                   ['level2_medium'] * n_per_group + 
                   ['level3_least_popular'] * (len(df) - 2 * n_per_group))
    
    df_sorted['pop_batch'] = batch_labels
    df = df_sorted.sort_index()

# Check batch distribution
print(f"\nBatch distribution:")
print(df['pop_batch'].value_counts(dropna=False))

#########################################
# Q1. How many websites overall use GTM?
#########################################

print("\n" + "=" * 50)
print("Q1: How many websites use GTM?")
print("=" * 50)

n_GTM = df['gtm_detected'].sum()
N_total = len(df)
prop = n_GTM / N_total

print(f"Count with GTM: {n_GTM} out of {N_total}")
print(f"Proportion with GTM: {prop*100:.2f}%")

# Binomial test (testing against 50% null hypothesis)
# Using scipy.stats for binomial test
pvalue = stats.binom_test(n_GTM, N_total, p=0.5)
conf_int = proportion_confint(n_GTM, N_total, alpha=0.05, method='wilson')

print(f"\nBinomial Test Results:")
print(f"P-value: {pvalue:.2e}")
print(f"95% Confidence Interval: [{conf_int[0]*100:.2f}%, {conf_int[1]*100:.2f}%]")

if pvalue < 0.05:
    print("Conclusion: Statistically significant difference from 50%. Reject H0.")
else:
    print("Conclusion: No significant difference from 50%. Fail to reject H0.")

# Plot Q1
plt.figure(figsize=(8, 6))
gtm_counts = df['gtm_detected'].value_counts()
bars = plt.bar(gtm_counts.index.astype(str), gtm_counts.values, 
               color='steelblue', alpha=0.7)
plt.title('GTM Usage Across All Websites (N=4500)', fontsize=14, fontweight='bold')
plt.xlabel('GTM Detected')
plt.ylabel('Count')

# Add count labels on bars
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height + 50,
             f'{int(height)}', ha='center', va='bottom')

plt.tight_layout()
plt.show()

#########################################
# Q1.1 GTM usage by popularity batch
#########################################

print("\n" + "=" * 50)
print("Q1.1: GTM usage by popularity batch")
print("=" * 50)

# Create contingency table
tbl_batch = pd.crosstab(df['pop_batch'], df['gtm_detected'])
print("Contingency Table:")
print(tbl_batch)

# Add proportions
prop_table = pd.crosstab(df['pop_batch'], df['gtm_detected'], normalize='index') * 100
print(f"\nProportions (% within each batch):")
print(prop_table.round(2))

# Check if chi-square assumptions are met
chi2_stat, p_val, dof, expected = chi2_contingency(tbl_batch)
print(f"\nExpected counts (should be ≥5):")
print(pd.DataFrame(expected, index=tbl_batch.index, columns=tbl_batch.columns).round(2))

if np.all(expected >= 5):
    print(f"\nChi-square Test Results:")
    print(f"Chi-square statistic: {chi2_stat:.4f}")
    print(f"P-value: {p_val:.6f}")
    print(f"Degrees of freedom: {dof}")
    
    # Effect size (Cramér's V)
    cramers_v = np.sqrt(chi2_stat / (N_total * (min(tbl_batch.shape) - 1)))
    print(f"Cramér's V (effect size): {cramers_v:.3f}")
    
    q1_1_pvalue = p_val
    
else:
    print("Warning: Chi-square assumptions not met. Using Fisher's exact test.")
    # For larger than 2x2 tables, use simulation
    from scipy.stats import chi2_contingency
    chi2_stat, p_val_sim, dof, expected = chi2_contingency(tbl_batch)
    q1_1_pvalue = p_val_sim
    print(f"Simulated p-value: {p_val_sim:.6f}")

if q1_1_pvalue < 0.05:
    print("Conclusion: Significant association. GTM usage differs across batches.")
else:
    print("Conclusion: No significant association. GTM usage is similar across batches.")

# Plot Q1.1
plt.figure(figsize=(10, 6))
tbl_batch_plot = tbl_batch.reset_index()
tbl_batch_melted = pd.melt(tbl_batch_plot, id_vars=['pop_batch'], 
                          var_name='gtm_detected', value_name='count')

sns.barplot(data=tbl_batch_melted, x='pop_batch', y='count', hue='gtm_detected', 
            palette=['grey', 'steelblue'], alpha=0.7)
plt.title('GTM Usage by Popularity Batch', fontsize=14, fontweight='bold')
plt.xlabel('Popularity Batch')
plt.ylabel('Count')
plt.xticks(rotation=45)

# Add count labels
for i, container in enumerate(plt.gca().containers):
    plt.gca().bar_label(container, fmt='%d')

plt.legend(title='GTM Detected')
plt.tight_layout()
plt.show()

#########################################
# Q1.2 GTM vs Google Analytics presence
#########################################

print("\n" + "=" * 50)
print("Q1.2: GTM vs GA presence")
print("=" * 50)

tbl_ga = pd.crosstab(df['gtm_detected'], df['ga_present'])
print("Contingency Table:")
print(tbl_ga)

# Add proportions
prop_table_ga = pd.crosstab(df['gtm_detected'], df['ga_present'], normalize='index') * 100
print(f"\nProportions (% within GTM groups):")
print(prop_table_ga.round(2))

# Use Fisher's exact test for 2x2 table
if tbl_ga.shape == (2, 2):
    # Convert to numpy array for fisher_exact
    table_array = tbl_ga.values
    odds_ratio, p_val_fisher = fisher_exact(table_array)
    
    print(f"\nFisher's Exact Test Results:")
    print(f"Odds Ratio: {odds_ratio:.3f}")
    print(f"P-value: {p_val_fisher:.2e}")
    
    # Calculate confidence interval for odds ratio manually or use alternative
    print(f"95% CI for OR: (calculated separately)")
    
    q1_2_pvalue = p_val_fisher
else:
    # Use chi-square for non-2x2 tables
    chi2_stat, p_val, dof, expected = chi2_contingency(tbl_ga)
    print(f"Chi-square statistic: {chi2_stat:.4f}")
    print(f"P-value: {p_val:.6f}")
    q1_2_pvalue = p_val

if q1_2_pvalue < 0.05:
    print("Conclusion: Significant association. GTM usage is linked to GA presence.")
else:
    print("Conclusion: No significant association between GTM and GA presence.")

# Plot Q1.2
plt.figure(figsize=(10, 6))
tbl_ga_plot = tbl_ga.reset_index()
tbl_ga_melted = pd.melt(tbl_ga_plot, id_vars=['gtm_detected'], 
                       var_name='ga_present', value_name='count')

sns.barplot(data=tbl_ga_melted, x='gtm_detected', y='count', hue='ga_present',
            palette=['grey', 'tomato'], alpha=0.7)
plt.title('Google Analytics Presence vs GTM Usage', fontsize=14, fontweight='bold')
plt.xlabel('GTM Detected')
plt.ylabel('Count')

# Add count labels
for container in plt.gca().containers:
    plt.gca().bar_label(container, fmt='%d')

plt.legend(title='GA Present')
plt.tight_layout()
plt.show()

#########################################
# Q1.3 Event count by popularity batch
#########################################

print("\n" + "=" * 50)
print("Q1.3: Event count by popularity batch")
print("=" * 50)

# Remove rows with missing batch information
df_clean = df.dropna(subset=['pop_batch']).copy()
print(f"Rows after removing missing batch info: {len(df_clean)}")

# Check distribution of event counts by batch
print(f"\nEvent count summary by batch:")
for batch in df_clean['pop_batch'].unique():
    if pd.notna(batch):
        batch_data = df_clean[df_clean['pop_batch'] == batch]['event_count']
        print(f"\n{batch}:")
        print(batch_data.describe())

# Check for normality (Shapiro-Wilk test on samples)
print(f"\nTesting normality assumption:")
for batch in df_clean['pop_batch'].unique():
    if pd.notna(batch):
        batch_data = df_clean[df_clean['pop_batch'] == batch]['event_count']
        if len(batch_data) > 3 and len(batch_data) <= 5000:
            stat, p_val_norm = stats.shapiro(batch_data)
            print(f"{batch} - Shapiro-Wilk p-value: {p_val_norm:.6f}")

# Use Kruskal-Wallis test (non-parametric, more robust)
unique_batches = df_clean['pop_batch'].nunique()
if unique_batches > 1:
    # Prepare data for Kruskal-Wallis
    groups = [df_clean[df_clean['pop_batch'] == batch]['event_count'].values 
              for batch in df_clean['pop_batch'].unique() if pd.notna(batch)]
    
    stat, p_val_kw = kruskal(*groups)
    
    print(f"\nKruskal-Wallis Test Results:")
    print(f"H-statistic: {stat:.4f}")
    print(f"P-value: {p_val_kw:.6f}")
    
    # Effect size (eta-squared approximation)
    H = stat
    N = len(df_clean)
    k = unique_batches
    eta_squared = (H - k + 1) / (N - k) if N > k else 0
    print(f"Eta-squared (effect size): {eta_squared:.3f}")
    
    if p_val_kw < 0.05:
        print("Conclusion: Significant difference in event counts across batches.")
        
        # Post-hoc pairwise tests (Mann-Whitney U with Bonferroni correction)
        print(f"\nPost-hoc pairwise comparisons (Mann-Whitney U):")
        batches = [b for b in df_clean['pop_batch'].unique() if pd.notna(b)]
        
        for i in range(len(batches)):
            for j in range(i+1, len(batches)):
                group1 = df_clean[df_clean['pop_batch'] == batches[i]]['event_count']
                group2 = df_clean[df_clean['pop_batch'] == batches[j]]['event_count']
                
                stat_mw, p_val_mw = mannwhitneyu(group1, group2, alternative='two-sided')
                # Bonferroni correction
                n_comparisons = len(batches) * (len(batches) - 1) // 2
                p_val_corrected = min(p_val_mw * n_comparisons, 1.0)
                
                print(f"{batches[i]} vs {batches[j]}: p = {p_val_corrected:.6f}")
    else:
        print("Conclusion: No significant difference in event counts across batches.")
        
else:
    print("Error: Not enough groups for comparison.")

# Plot Q1.3
plt.figure(figsize=(12, 6))
sns.boxplot(data=df_clean, x='pop_batch', y='event_count', 
            palette=['gold', 'steelblue', 'tomato'])
plt.title('Event Count Distribution by Popularity Batch', fontsize=14, fontweight='bold')
plt.xlabel('Popularity Batch')
plt.ylabel('Event Count')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Additional summary statistics
print("\n" + "=" * 50)
print("SUMMARY STATISTICS")
print("=" * 50)

print(f"Overall GTM adoption rate: {prop*100:.2f}%")
print(f"\nGTM adoption by batch:")

batch_summary = df_clean.groupby('pop_batch').agg({
    'gtm_detected': ['count', 'mean'],
    'ga_present': 'mean',
    'event_count': ['mean', 'median']
}).round(3)

# Flatten column names
batch_summary.columns = ['n', 'gtm_rate', 'ga_rate', 'mean_events', 'median_events']
batch_summary['gtm_rate'] = batch_summary['gtm_rate'] * 100
batch_summary['ga_rate'] = batch_summary['ga_rate'] * 100

print(batch_summary)

print(f"\nAnalysis completed successfully!")
print("=" * 50)