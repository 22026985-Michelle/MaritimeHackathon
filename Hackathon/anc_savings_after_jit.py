import pandas as pd
import numpy as np

def safe_read_csv(file_path):
    """
    Safely read CSV file with error handling
    """
    try:
        df = pd.read_csv(file_path)
        # Convert IMO to string to ensure consistent merging
        if 'imo' in df.columns:
            df['imo'] = df['imo'].astype(str)
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {str(e)}")
        return None

def calculate_emission_savings():
    """
    Calculate CO2 emission savings after JIT is applied
    """
    try:
        # Load before and after JIT emissions data with validation
        print("Loading emission data files...")
        before_jit = safe_read_csv('anc_before_jit_cleaned.csv')
        after_jit = safe_read_csv('anc_after_jit.csv')
        
        if before_jit is None or after_jit is None:
            raise ValueError("Failed to load one or both CSV files")
            
        # Print initial data validation
        print("\nInitial Data Validation:")
        print(f"Records in before JIT: {len(before_jit)}")
        print(f"Records in after JIT: {len(after_jit)}")
        print(f"Unique IMOs before JIT: {before_jit['imo'].nunique()}")
        print(f"Unique IMOs after JIT: {after_jit['imo'].nunique()}")
        
        # Merge the two datasets on IMO
        merged = pd.merge(before_jit, after_jit, on='imo', how='outer')
        
        # Fill NaN values with 0
        merged['anc_before_jit'] = merged['anc_before_jit'].fillna(0)
        merged['anc_after_jit'] = merged['anc_after_jit'].fillna(0)
        
        # Calculate emission savings
        merged['anc_savings_after_jit'] = merged['anc_before_jit'] - merged['anc_after_jit']
        
        # Sort by IMO number for consistent display
        merged = merged.sort_values('imo')
        
        # Print summary statistics
        print("\nEmission Savings Summary:")
        print("=" * 50)
        print(f"Total unique IMOs: {len(merged)}")
        print(f"IMOs with positive savings: {(merged['anc_savings_after_jit'] > 0).sum()}")
        print(f"IMOs with no savings: {(merged['anc_savings_after_jit'] == 0).sum()}")
        print(f"IMOs with negative savings (data anomaly): {(merged['anc_savings_after_jit'] < 0).sum()}")
        
        print("\nEmission Savings Statistics:")
        print("=" * 50)
        print(f"Total savings (tonnes): {merged['anc_savings_after_jit'].sum():.2f}")
        print(f"Average savings per IMO (tonnes): {merged['anc_savings_after_jit'].mean():.2f}")
        print(f"Maximum savings (tonnes): {merged['anc_savings_after_jit'].max():.2f}")
        print(f"Minimum savings (tonnes): {merged['anc_savings_after_jit'].min():.2f}")
        print(f"Median savings (tonnes): {merged['anc_savings_after_jit'].median():.2f}")
        
        # Create ranges of savings for analysis
        merged['savings_range'] = pd.cut(
            merged['anc_savings_after_jit'],
            bins=[-np.inf, -0.001, 0.001, 1, 10, 50, 100, np.inf],
            labels=['Negative', 'Zero', '0-1', '1-10', '10-50', '50-100', '100+']
        )
        
        print("\nEmission Savings Ranges Distribution:")
        print("=" * 50)
        range_dist = merged['savings_range'].value_counts().sort_index()
        print(range_dist)
        
        # Print percentage distribution
        print("\nPercentage Distribution:")
        print((range_dist / len(merged) * 100).round(2).apply(lambda x: f"{x:.2f}%"))
        
        # Display all IMOs and their savings
        pd.set_option('display.max_rows', None)
        pd.set_option('display.float_format', lambda x: '%.6f' % x)
        
        print("\nComplete List of All IMOs and Their Savings:")
        print("=" * 80)
        print("IMO           Before JIT      After JIT       Savings")
        print("-" * 80)
        
        # Display in blocks of 50 for better readability
        for i in range(0, len(merged), 50):
            block = merged.iloc[i:i+50]
            for _, row in block.iterrows():
                print(f"{row['imo']:<14} {row['anc_before_jit']:12.6f} {row['anc_after_jit']:12.6f} {row['anc_savings_after_jit']:12.6f}")
            if i + 50 < len(merged):
                input("Press Enter to see next 50 IMOs...")
        
        # Save the results to CSV files
        # 1. Complete results
        merged.to_csv('anc_savings_after_jit.csv', index=False)
        
        # 2. Summary statistics
        summary_stats = pd.DataFrame({
            'Metric': ['Total IMOs', 'Total Savings', 'Average Savings', 'Max Savings', 'Min Savings', 'Median Savings'],
            'Value': [
                len(merged),
                merged['anc_savings_after_jit'].sum(),
                merged['anc_savings_after_jit'].mean(),
                merged['anc_savings_after_jit'].max(),
                merged['anc_savings_after_jit'].min(),
                merged['anc_savings_after_jit'].median()
            ]
        })
        summary_stats.to_csv('savings_summary_stats.csv', index=False)
        
        print("\nOutput files generated:")
        print("1. anc_savings_after_jit.csv - Complete savings data")
        print("2. savings_summary_stats.csv - Summary statistics")
        
        return merged
        
    except Exception as e:
        print(f"\nError calculating emission savings: {str(e)}")
        print("\nTroubleshooting suggestions:")
        print("1. Check if both CSV files exist in the current directory")
        print("2. Verify CSV files have the correct column names (imo, anc_before_jit, anc_after_jit)")
        print("3. Check for any formatting issues in the CSV files")
        raise

if __name__ == "__main__":
    try:
        savings_result = calculate_emission_savings()
    except Exception as e:
        print(f"\nProgram terminated with error: {str(e)}")
    else:
        print("\nAnalysis completed successfully")
