import pandas as pd
import numpy as np

def load_and_analyze_data():
    """
    Load and analyze emissions data from CSV with complete IMO listing
    """
    try:
        # Read the emissions data
        emissions_df = pd.read_csv('anc_before_jit.csv')
        
        # Sort by IMO number
        emissions_df = emissions_df.sort_values('imo')
        
        # Print summary statistics
        print("\nData Summary:")
        print(f"Total unique IMOs: {len(emissions_df)}")
        print(f"IMOs with non-zero emissions: {(emissions_df['anc_before_jit'] > 0).sum()}")
        print(f"IMOs with zero emissions: {(emissions_df['anc_before_jit'] == 0).sum()}")
        
        # Calculate emission statistics
        print("\nEmission Statistics:")
        print(f"Total emissions (tonnes): {emissions_df['anc_before_jit'].sum():.2f}")
        print(f"Average emissions per IMO (tonnes): {emissions_df['anc_before_jit'].mean():.2f}")
        print(f"Maximum emissions (tonnes): {emissions_df['anc_before_jit'].max():.2f}")
        print(f"Minimum non-zero emissions (tonnes): {emissions_df.loc[emissions_df['anc_before_jit'] > 0, 'anc_before_jit'].min():.2f}")
        
        # Create ranges of emissions for better understanding
        emissions_df['emission_range'] = pd.cut(emissions_df['anc_before_jit'], 
                                              bins=[0, 0.001, 1, 10, 50, 100, np.inf],
                                              labels=['Zero', '0-1', '1-10', '10-50', '50-100', '100+'])
        
        # Print distribution of emissions
        print("\nEmission Ranges Distribution:")
        print(emissions_df['emission_range'].value_counts().sort_index())
        
        # Set display options to show all rows
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        
        # Print complete list of IMOs and their emissions
        print("\nComplete List of All IMOs and Their Emissions:")
        print("=" * 50)
        
        # Format emissions to 6 decimal places for readability
        formatted_df = emissions_df[['imo', 'anc_before_jit']].copy()
        formatted_df['anc_before_jit'] = formatted_df['anc_before_jit'].round(6)
        
        # Print in blocks of 50 IMOs for better readability
        total_imos = len(formatted_df)
        block_size = 50
        
        for i in range(0, total_imos, block_size):
            print(f"\nIMOs {i+1} to {min(i+block_size, total_imos)}:")
            print(formatted_df.iloc[i:i+block_size].to_string(index=False))
            print("-" * 50)
            
            # Optional: Add a pause every 500 IMOs to make output more manageable
            if i > 0 and i % 500 == 0:
                input("Press Enter to continue showing more IMOs...")
        
        # Save three different CSV files for different purposes
        # 1. Detailed analysis with all columns
        detailed_output = emissions_df.sort_values('anc_before_jit', ascending=False)
        detailed_output.to_csv('emissions_detailed_analysis.csv', index=False)
        
        # 2. Simple IMO and emissions list
        formatted_df.to_csv('emissions_simple_list.csv', index=False)
        
        # 3. Summary by emission ranges
        range_summary = emissions_df.groupby('emission_range').agg({
            'imo': 'count',
            'anc_before_jit': ['sum', 'mean', 'min', 'max']
        }).round(6)
        range_summary.to_csv('emissions_range_summary.csv')
        
        print("\nOutput files generated:")
        print("1. emissions_detailed_analysis.csv - Full detailed analysis")
        print("2. emissions_simple_list.csv - Simple IMO and emissions list")
        print("3. emissions_range_summary.csv - Summary by emission ranges")
        
        return emissions_df
        
    except Exception as e:
        print(f"Error analyzing data: {str(e)}")
        raise

def display_specific_imo_range(emissions_df,start_imo=None, end_imo=None):
    """
    Display emissions for a specific range of IMOs
    """
    if start_imo and end_imo:
        mask = (emissions_df['imo'] >= start_imo) & (emissions_df['imo'] <= end_imo)
        print(f"\nDisplaying IMOs from {start_imo} to {end_imo}:")
        print(emissions_df[mask][['imo', 'anc_before_jit']].to_string(index=False))

if name == "__main__":
    emissions_data = load_and_analyze_data()
    
    # Uncomment and modify these lines to display specific IMO ranges
    # display_specific_imo_range(emissions_data, "1013315", "1013400")
    
    print("\nAnalysis complete. Check the generated CSV files for detailed information.")