import pandas as pd
from datetime import datetime
import re

class DisruptionCleaner:
    """Pandas-based data cleaning"""
    
    @staticmethod
    def clean_disruptions(raw_df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw JSON to structured data"""
        
        df = raw_df.copy()
        
        # Extract nested fields
        df['type'] = df['type'].str.lower()
        df['title'] = df['title'].str.strip()
        
        # Parse timestamps (NS uses ISO 8601)
        df['start_time'] = pd.to_datetime(df['start'], errors='coerce')
        df['end_time'] = pd.to_datetime(df['end'], errors='coerce')
        
        # Calculate duration in minutes
        df['duration_minutes'] = (
            (df['end_time'] - df['start_time']).dt.total_seconds() / 60
        )
        
        # Handle missing end times (ongoing disruptions)
        df['is_ongoing'] = df['end_time'].isna()
        df['end_time'] = df['end_time'].fillna(datetime.now())
        
        # Classify impact severity based on affected routes
        df['impact_level'] = df.apply(
            lambda row: DisruptionCleaner._calculate_impact(row),
            axis=1
        )
        
        # Extract station codes from nested structures
        df['affected_stations'] = df['routes'].apply(
            lambda x: ','.join([s['code'] for s in x]) if x else None
        )
        
        # Remove test/invalid entries
        df = df[df['title'].str.len() > 5]
        df = df[df['duration_minutes'] >= 0]
        
        return df
    
    @staticmethod
    def _calculate_impact(row) -> int:
        """Business logic: 1=minor, 5=severe"""
        if row['type'] == 'cancellation':
            return 5
        elif row['duration_minutes'] > 120:
            return 4
        elif row['duration_minutes'] > 60:
            return 3
        else:
            return 2