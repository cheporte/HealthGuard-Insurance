"""
Evidently HTML report generation for extended obesity prediction data.
Generates comprehensive reports for data analysis and model monitoring.
"""

import pandas as pd
import sqlite3
import os
from datetime import datetime
from pathlib import Path

from evidently import Report
from evidently.metrics import *
from evidently.presets import *

BASE_DIR = Path(__file__).parent.parent.parent
DB_EXTENDED_PATH = os.path.join(BASE_DIR, 'data/db/extended_data.db')
REPORTS_DIR = os.path.join(BASE_DIR, 'reports')

# Create reports directory if it doesn't exist
os.makedirs(REPORTS_DIR, exist_ok=True)


def load_extended_data(limit: int = None) -> pd.DataFrame:
    """
    Load extended data from database.
    
    Args:
        limit: Maximum number of records to load. None for all records.
        
    Returns:
        DataFrame with extended data
    """
    try:
        conn = sqlite3.connect(DB_EXTENDED_PATH)
        if limit:
            df = pd.read_sql_query(f"SELECT * FROM extended_data LIMIT {limit}", conn)
            df_original = pd.read_sql_query(f"SELECT * FROM original_data LIMIT {limit}", conn)
        else:
            df = pd.read_sql_query("SELECT * FROM extended_data", conn)
            df_original = pd.read_sql_query("SELECT * FROM original_data", conn)
            
        conn.close()
        return df, df_original
    except Exception as e:
        print(f"Error loading extended data: {e}")
        return pd.DataFrame()


def generate_data_summary_report() -> str:
    """
    Generate comprehensive data summary report.
    
    Returns:
        Path to the generated HTML report
    """
    print("Generating data summary report...")
    
    df = load_extended_data()
    
    if df.empty:
        print("No data available for report generation")
        return None
    
    try:
        report = Report(metrics=[
            DataSummaryPreset()
        ])
        
        result = report.run(df, None)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(REPORTS_DIR, f"data_summary_{timestamp}.html")
        
        result.save_html(output_path)
        print(f"✓ Data summary report saved to: {output_path}")
        
        return output_path
        
    except Exception as e:
        print(f"Error generating data summary report: {e}")
        import traceback
        traceback.print_exc()
        return None


def generate_feature_distribution_report() -> str:
    """
    Generate feature distribution report for all columns.
    
    Returns:
        Path to the generated HTML report
    """
    print("Generating feature distribution report...")
    
    df, df_original = load_extended_data()
    
    if df.empty:
        print("No data available for report generation")
        return None
    
    try:
        report = Report(metrics=[
            DataDriftPreset(),
        ])
        
        result = report.run(df, df_original)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(REPORTS_DIR, f"feature_distribution_{timestamp}.html")
        
        result.save_html(output_path)
        print(f"✓ Feature distribution report saved to: {output_path}")
        
        return output_path
        
    except Exception as e:
        print(f"Error generating feature distribution report: {e}")
        import traceback
        traceback.print_exc()
        return None


def generate_obesity_analysis_report() -> str:
    """
    Generate detailed obesity category analysis report.
    
    Returns:
        Path to the generated HTML report
    """
    print("Generating obesity analysis report...")
    
    df = load_extended_data()
    
    if df.empty:
        print("No data available for report generation")
        return None
    
    try:
        # Filter to focus on obesity-related columns
        obesity_cols = [col for col in df.columns if col not in ['timestamp']]
        
        report = Report(metrics=[
            ColumnCount(),
            ValueDrift(),
        ])
        
        report.run(reference_data=df[obesity_cols])
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(REPORTS_DIR, f"obesity_analysis_{timestamp}.html")
        
        report.save_html(output_path)
        print(f"✓ Obesity analysis report saved to: {output_path}")
        
        return output_path
        
    except Exception as e:
        print(f"Error generating obesity analysis report: {e}")
        import traceback
        traceback.print_exc()
        return None


def generate_all_reports() -> dict:
    """
    Generate all available reports.
    
    Returns:
        Dictionary with paths to generated reports
    """
    print("\n" + "="*70)
    print("Generating all Evidently HTML reports...")
    print("="*70 + "\n")
    
    results = {
        "data_summary": generate_data_summary_report(),
        "feature_distribution": generate_feature_distribution_report(),
        "obesity_analysis": generate_obesity_analysis_report(),
    }
    
    print("\n" + "="*70)
    print("Report generation complete!")
    print("="*70)
    print(f"\nReports saved to: {REPORTS_DIR}")
    print(f"Total reports generated: {len([r for r in results.values() if r])}")
    
    return results


def get_reports_summary() -> dict:
    """
    Get summary of generated reports.
    
    Returns:
        Dictionary with reports summary
    """
    try:
        df = load_extended_data()
        
        summary = {
            "total_records": len(df),
            "reports_directory": REPORTS_DIR,
            "reports_count": len([f for f in os.listdir(REPORTS_DIR) if f.endswith('.html')]) if os.path.exists(REPORTS_DIR) else 0,
        }
        
        if not df.empty and 'NObeyesdad' in df.columns:
            summary["obesity_distribution"] = df['NObeyesdad'].value_counts().to_dict()
            summary["date_range"] = {
                "start": str(df['timestamp'].min()) if 'timestamp' in df.columns else "N/A",
                "end": str(df['timestamp'].max()) if 'timestamp' in df.columns else "N/A"
            }
        
        return summary
        
    except Exception as e:
        print(f"Error getting reports summary: {e}")
        return {}


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "all":
            results = generate_all_reports()
            summary = get_reports_summary()
            
            print("\nGenerated Reports:")
            for name, path in results.items():
                status = "✓" if path else "✗"
                print(f"  {status} {name}: {path}")
            
            print("\nSummary:")
            for key, value in summary.items():
                if key not in ['reports_directory']:
                    print(f"  {key}: {value}")
        
        elif command == "summary":
            summary = get_reports_summary()
            print("\nReports Summary:")
            print("="*50)
            for key, value in summary.items():
                print(f"  {key}: {value}")
            print("="*50)
        
        elif command == "data-summary":
            generate_data_summary_report()
        
        elif command == "distribution":
            generate_feature_distribution_report()
        
        elif command == "obesity":
            generate_obesity_analysis_report()
        
        else:
            print("Usage: python app/services/evidently_reports.py [all|summary|data-summary|distribution|obesity]")
            print("  all           - Generate all reports")
            print("  summary       - Show reports summary")
            print("  data-summary  - Generate data summary report")
            print("  distribution  - Generate feature distribution report")
            print("  obesity       - Generate obesity analysis report")
    
    else:
        # Default: generate all reports
        print("Generating all reports by default...")
        generate_all_reports()
