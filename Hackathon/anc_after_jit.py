import pandas as pd
import numpy as np

def load_data():
    """
    Load AIS data and emission factor datasets with improved error handling
    """
    try:
        ais_data = pd.read_csv('ais_dataset.csv', dtype={'imo': str})
        aux_engine_factors = pd.read_csv('auxiliary_engine_emission_factors.csv')
        boiler_engine_factors = pd.read_csv('boiler_engine_emission_factors.csv')
        
        print(f"Loaded {len(ais_data)} AIS records")
        print(f"Found {ais_data['imo'].nunique()} unique IMOs")
        
        return ais_data, aux_engine_factors, boiler_engine_factors
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise

def calculate_duration(df):
    """
    Calculate activity duration from timestamps if available
    """
    try:
        if 'timestamp' in df.columns:
            # Sort by IMO and timestamp
            df = df.sort_values(['imo', 'timestamp'])
            
            # Calculate time difference between consecutive readings for each IMO
            df['activity_duration_hours'] = df.groupby('imo')['timestamp'].diff().dt.total_seconds() / 3600
            
            # Fill first entry for each IMO and any missing values with 1 hour
            df['activity_duration_hours'] = df['activity_duration_hours'].fillna(1)
        else:
            df['activity_duration_hours'] = 1
            
        # Cap unreasonable durations at 24 hours
        df.loc[df['activity_duration_hours'] > 24, 'activity_duration_hours'] = 24
        
        return df
    except Exception as e:
        print(f"Error calculating duration: {str(e)}")
        df['activity_duration_hours'] = 1
        return df

def preprocess_data(ais_data):
    """
    Preprocess AIS data while preserving all IMOs
    """
    # Create a copy to avoid modifying original data
    df = ais_data.copy()
    
    # Convert timestamp while preserving invalid dates
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # Create a baseline entry for each unique IMO
    all_imos = pd.DataFrame({'imo': df['imo'].unique()})
    
    # Fill missing values with appropriate defaults
    df['ael'] = df.get('ael', pd.Series(0)).fillna(0)
    df['abl'] = df.get('abl', pd.Series(0)).fillna(0)
    
    # Calculate activity duration
    df = calculate_duration(df)
    
    return df, all_imos

def merge_emission_factors(ais_data, aux_engine_factors, boiler_engine_factors):
    """
    Merge emission factors while handling missing values
    """
    merged = pd.merge(ais_data, aux_engine_factors, on='fuel_category', how='left')
    merged = pd.merge(merged, boiler_engine_factors, on='fuel_category', how='left')
    
    # Fill missing emission factors with 0
    for col in ['sfc_ae', 'sfc_ab']:
        merged[col] = merged[col].fillna(0)
    
    return merged

def calculate_emissions_after_jit(data):
    """
    Calculate emissions with JIT implementation (3-hour cap)
    """
    # Initialize emissions columns
    data['aux_emissions_after_jit'] = 0.0
    data['boiler_emissions_after_jit'] = 0.0
    
    # Cap activity duration at 3 hours for JIT
    data['jit_activity_duration_hours'] = data['activity_duration_hours'].clip(upper=3)
    
    # Calculate auxiliary engine emissions
    mask = (data['ael'].notna() & data['sfc_ae'].notna())
    data.loc[mask, 'aux_emissions_after_jit'] = (
        data.loc[mask, 'ael'] * 
        data.loc[mask, 'jit_activity_duration_hours'] * 
        data.loc[mask, 'sfc_ae'] * 
        0.867 * 
        3.667
    )
    
    # Calculate boiler emissions
    mask = (data['abl'].notna() & data['sfc_ab'].notna())
    data.loc[mask, 'boiler_emissions_after_jit'] = (
        data.loc[mask, 'abl'] * 
        data.loc[mask, 'jit_activity_duration_hours'] * 
        data.loc[mask, 'sfc_ab'] * 
        0.867 * 
        3.667
    )
    
    # Calculate total emissions
    data['total_emissions_grams_after_jit'] = (
        data['aux_emissions_after_jit'] + data['boiler_emissions_after_jit']
    )
    data['total_emissions_tonnes_after_jit'] = data['total_emissions_grams_after_jit'] / 1_000_000
    
    return data

def aggregate_and_analyze_emissions(data, all_imos):
    """
    Aggregate emissions and provide comprehensive analysis
    """
    # Aggregate emissions by IMO
    emissions_by_imo = data.groupby('imo')['total_emissions_tonnes_after_jit'].sum().reset_index()
    
    # Merge with all IMOs to ensure all are included
    final_emissions = pd.merge(all_imos, emissions_by_imo, on='imo', how='left')
    final_emissions['total_emissions_tonnes_after_jit'] = final_emissions['total_emissions_tonnes_after_jit'].fillna(0)
    final_emissions.columns = ['imo', 'anc_after_jit']
    
    # Sort by IMO
    final_emissions = final_emissions.sort_values('imo')
    
    # Print summary statistics
    print("\nAfter JIT Analysis Summary:")
    print(f"Total unique IMOs: {len(final_emissions)}")
    print(f"IMOs with non-zero emissions: {(final_emissions['anc_after_jit'] > 0).sum()}")
    print(f"IMOs with zero emissions: {(final_emissions['anc_after_jit'] == 0).sum()}")
    
    print("\nEmission Statistics (After JIT):")
    print(f"Total emissions (tonnes): {final_emissions['anc_after_jit'].sum():.2f}")
    print(f"Average emissions per IMO (tonnes): {final_emissions['anc_after_jit'].mean():.2f}")
    print(f"Maximum emissions (tonnes): {final_emissions['anc_after_jit'].max():.2f}")
    print(f"Minimum non-zero emissions (tonnes): {final_emissions.loc[final_emissions['anc_after_jit'] > 0, 'anc_after_jit'].min():.2f}")
    
    # Create emission ranges
    final_emissions['emission_range'] = pd.cut(
        final_emissions['anc_after_jit'],
        bins=[0, 0.001, 1, 10, 50, 100, np.inf],
        labels=['Zero', '0-1', '1-10', '10-50', '50-100', '100+']
    )
    
    print("\nEmission Ranges Distribution (After JIT):")
    print(final_emissions['emission_range'].value_counts().sort_index())
    
    # Display all IMOs in blocks
    print("\nComplete List of IMOs and Their After-JIT Emissions:")
    print("=" * 50)
    
    # Set display options
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    
    # Display in blocks of 50
    total_imos = len(final_emissions)
    block_size = 50
    
    for i in range(0, total_imos, block_size):
        print(f"\nIMOs {i+1} to {min(i+block_size, total_imos)}:")
        print(final_emissions.iloc[i:i+block_size][['imo', 'anc_after_jit']].round(6).to_string(index=False))
        print("-" * 50)
        
        if i > 0 and i % 500 == 0:
            input("Press Enter to continue showing more IMOs...")
    
    # Save results
    final_emissions.to_csv('anc_after_jit.csv', index=False)
    final_emissions.to_csv('emissions_after_jit_detailed.csv', index=False)
    
    return final_emissions

def main():
    # Load data
    ais_data, aux_engine_factors, boiler_engine_factors = load_data()
    
    # Preprocess data and get all IMOs
    processed_data, all_imos = preprocess_data(ais_data)
    
    # Merge emission factors
    merged_data = merge_emission_factors(processed_data, aux_engine_factors, boiler_engine_factors)
    
    # Calculate emissions after JIT
    emissions_data = calculate_emissions_after_jit(merged_data)
    
    # Aggregate and analyze emissions
    final_emissions = aggregate_and_analyze_emissions(emissions_data, all_imos)
    
    print("\nAnalysis complete. Results saved to 'anc_after_jit.csv' and 'emissions_after_jit_detailed.csv'")
    
    return final_emissions

if name == "__main__":
    result = main()
