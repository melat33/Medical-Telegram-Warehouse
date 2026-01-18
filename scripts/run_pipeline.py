#!/usr/bin/env python3
"""
Enhanced Pipeline Runner with CSV Formatting
"""

import os
import sys
import argparse
import subprocess
import time
import pandas as pd
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def log(message: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def format_csv_files():
    """Format all CSV files for better readability."""
    log("Formatting CSV files...")
    
    raw_data_dir = PROJECT_ROOT / "data" / "raw"
    
    if not raw_data_dir.exists():
        log("No data directory found")
        return
    
    # Find all CSV files
    csv_files = list(raw_data_dir.rglob("*.csv"))
    
    if not csv_files:
        log("No CSV files found")
        return
    
    for csv_file in csv_files:
        try:
            # Read CSV
            df = pd.read_csv(csv_file)
            
            # Create a summary
            summary = f"""
            CSV File: {csv_file.name}
            {'='*50}
            Total Rows: {len(df):,}
            Total Columns: {len(df.columns)}
            Channels: {df['channel_name'].nunique() if 'channel_name' in df.columns else 'N/A'}
            Date Range: {df['message_date'].min() if 'message_date' in df.columns else 'N/A'} to {df['message_date'].max() if 'message_date' in df.columns else 'N/A'}
            Messages with Images: {df['has_media'].sum() if 'has_media' in df.columns else 'N/A'}
            {'='*50}
            """
            
            log(summary)
            
            # Display table preview
            log("Preview of data:")
            print(df.head().to_string())
            print("\n" + "="*80 + "\n")
            
        except Exception as e:
            log(f"❌ Error formatting {csv_file}: {e}")


def create_data_summary():
    """Create a comprehensive data summary."""
    log("Creating data summary...")
    
    raw_data_dir = PROJECT_ROOT / "data" / "raw" / "csv"
    
    if not raw_data_dir.exists():
        log("No CSV directory found")
        return
    
    # Find today's CSV files
    today = datetime.now().strftime("%Y-%m-%d")
    today_dir = raw_data_dir / today
    
    if not today_dir.exists():
        log(f"No data for today ({today})")
        return
    
    csv_files = list(today_dir.glob("*.csv"))
    
    summary_data = []
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            
            summary = {
                "File": csv_file.name,
                "Rows": len(df),
                "Columns": len(df.columns),
                "Channels": df['channel_name'].unique().tolist() if 'channel_name' in df.columns else [],
                "Date_Range": f"{df['message_date'].min() if 'message_date' in df.columns else ''} to {df['message_date'].max() if 'message_date' in df.columns else ''}",
                "With_Images": df['has_media'].sum() if 'has_media' in df.columns else 0,
                "Total_Views": df['views'].sum() if 'views' in df.columns else 0
            }
            
            summary_data.append(summary)
            
        except Exception as e:
            log(f"Error reading {csv_file}: {e}")
    
    # Create summary DataFrame
    if summary_data:
        summary_df = pd.DataFrame(summary_data)
        
        # Save summary
        summary_file = today_dir / "data_summary.csv"
        summary_df.to_csv(summary_file, index=False)
        
        log(f"✓ Data summary saved: {summary_file}")
        
        # Display summary as table
        log("Data Summary Table:")
        print(summary_df.to_string(index=False))
        
        return summary_df
    else:
        log("No data to summarize")
        return None


def run_scraper(args):
    """Run Telegram scraper."""
    log("=" * 60)
    log("STAGE 1: SCRAPING TELEGRAM DATA")
    log("=" * 60)
    
    cmd = [sys.executable, "src/scraper.py", "--limit", "150"]  # Always use 150
    
    if args.test:
        cmd.append("--test")
    
    log(f"Running: {' '.join(cmd)}")
    
    try:
        # Run scraper
        result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
        
        if result.returncode == 0:
            log("✅ Scraping completed successfully")
            
            # Format CSV files
            format_csv_files()
            
            # Create data summary
            create_data_summary()
            
            return True
        else:
            log(f"❌ Scraping failed: {result.stderr}")
            return False
            
    except Exception as e:
        log(f"❌ Error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Enhanced Pipeline Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py              # Run scraper with limit 150
  python run_pipeline.py --test       # Test mode (limit 5)
  python run_pipeline.py --summary    # Create data summary only
        """
    )
    
    parser.add_argument('--scrape', action='store_true', help='Run scraper (default)')
    parser.add_argument('--test', action='store_true', help='Test mode (limit 5)')
    parser.add_argument('--summary', action='store_true', help='Create data summary only')
    parser.add_argument('--format', action='store_true', help='Format CSV files only')
    
    args = parser.parse_args()
    
    # Default to scrape if no specific action
    if not any([args.scrape, args.summary, args.format]):
        args.scrape = True
    
    if args.scrape:
        success = run_scraper(args)
        sys.exit(0 if success else 1)
    
    elif args.summary:
        create_data_summary()
    
    elif args.format:
        format_csv_files()


if __name__ == "__main__":
    main()