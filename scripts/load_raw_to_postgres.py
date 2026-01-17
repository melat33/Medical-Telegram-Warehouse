#!/usr/bin/env python3
"""
Load Raw Telegram Data to PostgreSQL
=====================================
Loads JSON files from data lake into PostgreSQL raw schema.
FIXED VERSION - Handles ISO 8601 timestamps with timezone properly
"""

import os
import json
import psycopg2
from pathlib import Path
from datetime import datetime
import sys
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DataLoader:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'medical_warehouse'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres123')
        }
        
        self.data_dir = Path("data/raw/telegram_messages")
        self.connection = None
        
    def connect(self):
        """Connect to PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            print(f"✅ Connected to PostgreSQL: {self.db_config['database']}")
            return True
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            print(f"   Config: {self.db_config}")
            return False
    
    def create_raw_table(self):
        """Create raw telegram messages table if it doesn't exist."""
        create_table_sql = """
        CREATE SCHEMA IF NOT EXISTS raw;
        
        CREATE TABLE IF NOT EXISTS raw.telegram_messages (
            id SERIAL PRIMARY KEY,
            message_id INTEGER,
            channel_name VARCHAR(255),
            channel_title VARCHAR(255),
            message_date TIMESTAMP,
            message_text TEXT,
            has_media BOOLEAN,
            image_path TEXT,
            views INTEGER,
            forwards INTEGER,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            raw_data JSONB,
            UNIQUE(message_id, channel_name)
        );
        
        CREATE INDEX IF NOT EXISTS idx_telegram_channel ON raw.telegram_messages(channel_name);
        CREATE INDEX IF NOT EXISTS idx_telegram_date ON raw.telegram_messages(message_date);
        CREATE INDEX IF NOT EXISTS idx_telegram_views ON raw.telegram_messages(views DESC);
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                self.connection.commit()
                print("✅ Raw table created/verified")
        except Exception as e:
            print(f"❌ Error creating table: {e}")
    
    def clear_existing_data(self, date_folder: str = None):
        """Clear existing data for specified date or all data."""
        try:
            with self.connection.cursor() as cursor:
                if date_folder:
                    # Delete only data from specific date folder
                    cursor.execute("""
                        DELETE FROM raw.telegram_messages 
                        WHERE channel_name IN (
                            SELECT DISTINCT channel_name 
                            FROM raw.telegram_messages 
                            WHERE raw_data->>'extraction_date' = %s
                        )
                    """, (date_folder,))
                    print(f"✅ Cleared existing data for date: {date_folder}")
                else:
                    # Delete all data
                    cursor.execute("DELETE FROM raw.telegram_messages")
                    print("✅ Cleared all existing data")
                
                self.connection.commit()
        except Exception as e:
            print(f"❌ Error clearing data: {e}")
    
    def parse_date(self, date_str):
        """
        Parse date string in various formats.
        Handles: ISO 8601 with timezone, ISO without timezone, other common formats
        """
        if not date_str or date_str == '':
            return None
        
        try:
            # FIXED: Handle ISO 8601 format with timezone (e.g., "2026-01-16T14:00:11+00:00")
            # Remove timezone part for simplicity (we'll store in UTC)
            if '+' in date_str:
                # Extract date and time part before timezone
                date_part = date_str.split('+')[0]
                return datetime.fromisoformat(date_part)
            elif 'T' in date_str:
                # ISO format without timezone
                return datetime.fromisoformat(date_str)
            else:
                # Try other common formats
                formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d %H:%M',
                    '%Y-%m-%d',
                    '%d/%m/%Y %H:%M:%S',
                    '%d/%m/%Y'
                ]
                
                for fmt in formats:
                    try:
                        return datetime.strptime(date_str, fmt)
                    except ValueError:
                        continue
                
                print(f"   ⚠️  Could not parse date: {date_str}")
                return None
                
        except Exception as e:
            print(f"   ⚠️  Date parsing error for {date_str}: {e}")
            return None
    
    def load_json_file(self, json_file: Path):
        """Load a single JSON file into database."""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            metadata = data.get('metadata', {})
            messages = data.get('data', []) or data.get('messages', [])
            
            channel_name = metadata.get('channel', json_file.stem)
            extraction_date = metadata.get('extraction_date', json_file.parent.name)
            
            print(f"📁 Loading {json_file.name}: {len(messages)} messages")
            
            inserted_count = 0
            with self.connection.cursor() as cursor:
                for msg in messages:
                    try:
                        # Prepare data for insertion
                        insert_sql = """
                        INSERT INTO raw.telegram_messages 
                        (message_id, channel_name, channel_title, message_date, 
                         message_text, has_media, image_path, views, forwards, raw_data)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (message_id, channel_name) DO NOTHING
                        """
                        
                        # Parse message date using the fixed method
                        message_date = self.parse_date(msg.get('message_date'))
                        
                        # Prepare values with validation
                        message_id = msg.get('message_id')
                        if message_id is None:
                            print(f"   ⚠️  Skipping message without ID: {msg}")
                            continue
                        
                        # Get channel title or use channel name as fallback
                        channel_title = msg.get('channel_title', channel_name)
                        
                        # Clean message text
                        message_text = str(msg.get('message_text', ''))
                        if not message_text or message_text.strip() == '':
                            message_text = '[No text content]'
                        
                        # Ensure views and forwards are non-negative integers
                        views = max(int(msg.get('views', 0)), 0)
                        forwards = max(int(msg.get('forwards', 0)), 0)
                        
                        # Check for image
                        image_path = msg.get('image_path')
                        has_media = bool(image_path and image_path.strip() != '')
                        
                        values = (
                            message_id,
                            channel_name,
                            channel_title,
                            message_date,
                            message_text[:10000],  # Limit text length for safety
                            has_media,
                            image_path,
                            views,
                            forwards,
                            json.dumps(msg)  # Store raw data as JSONB
                        )
                        
                        cursor.execute(insert_sql, values)
                        inserted_count += 1
                        
                    except Exception as e:
                        print(f"   ⚠️  Skipping message {msg.get('message_id')}: {e}")
                        continue
            
            self.connection.commit()
            print(f"   ✅ Inserted {inserted_count} messages")
            return inserted_count
            
        except Exception as e:
            print(f"❌ Error loading {json_file}: {e}")
            return 0
    
    def load_all_data(self, specific_date: str = None):
        """Load all JSON files from data lake."""
        total_messages = 0
        
        if specific_date:
            # Load specific date folder
            date_folder = self.data_dir / specific_date
            if date_folder.exists():
                print(f"📅 Loading data for date: {specific_date}")
                for json_file in date_folder.glob("*.json"):
                    if json_file.name != "_manifest.json":
                        total_messages += self.load_json_file(json_file)
            else:
                print(f"❌ Date folder not found: {specific_date}")
                return 0
        else:
            # Load all date folders
            date_folders = sorted([d for d in self.data_dir.iterdir() if d.is_dir()])
            
            if not date_folders:
                print(f"⚠️  No data folders found in {self.data_dir}")
                print(f"   Looking for folders like: 2026-01-16, 2026-01-18, etc.")
                return 0
            
            for date_folder in date_folders:
                print(f"\n📅 Loading data from: {date_folder.name}")
                
                # Count JSON files in this folder
                json_files = list(date_folder.glob("*.json"))
                json_files = [f for f in json_files if f.name != "_manifest.json"]
                
                if not json_files:
                    print(f"   ⚠️  No JSON files found in {date_folder.name}/")
                    continue
                
                for json_file in json_files:
                    total_messages += self.load_json_file(json_file)
        
        return total_messages
    
    def verify_data(self):
        """Verify loaded data with summary statistics."""
        try:
            with self.connection.cursor() as cursor:
                # Count total messages
                cursor.execute("SELECT COUNT(*) FROM raw.telegram_messages")
                total_count = cursor.fetchone()[0]
                
                # Count by channel
                cursor.execute("""
                    SELECT 
                        channel_name, 
                        COUNT(*) as message_count,
                        ROUND(AVG(views)::numeric, 1) as avg_views,
                        ROUND(AVG(forwards)::numeric, 1) as avg_forwards,
                        MIN(message_date) as first_post,
                        MAX(message_date) as last_post
                    FROM raw.telegram_messages
                    GROUP BY channel_name
                    ORDER BY message_count DESC
                """)
                channel_stats = cursor.fetchall()
                
                # Date range
                cursor.execute("""
                    SELECT 
                        MIN(message_date), 
                        MAX(message_date),
                        COUNT(DISTINCT DATE(message_date)) as unique_days
                    FROM raw.telegram_messages
                    WHERE message_date IS NOT NULL
                """)
                date_range = cursor.fetchone()
                
                # Data quality check
                cursor.execute("""
                    SELECT 
                        COUNT(*) as null_dates,
                        COUNT(*) FILTER (WHERE views < 0) as negative_views,
                        COUNT(*) FILTER (WHERE forwards < 0) as negative_forwards
                    FROM raw.telegram_messages
                """)
                quality_metrics = cursor.fetchone()
                
                print("\n" + "=" * 80)
                print("📊 DATA LOAD VERIFICATION")
                print("=" * 80)
                print(f"📈 Total messages loaded: {total_count:,}")
                
                if date_range[0] and date_range[1]:
                    print(f"📅 Date range: {date_range[0]} to {date_range[1]}")
                    print(f"📅 Unique days with data: {date_range[2]}")
                
                print("\n🔍 Data Quality Check:")
                print("-" * 40)
                print(f"   Messages with null dates: {quality_metrics[0]:,}")
                print(f"   Messages with negative views: {quality_metrics[1]:,}")
                print(f"   Messages with negative forwards: {quality_metrics[2]:,}")
                
                print("\n📢 Channel Statistics:")
                print("-" * 40)
                for channel, count, avg_views, avg_forwards, first_post, last_post in channel_stats:
                    print(f"  🔹 {channel}:")
                    print(f"     📝 Messages: {count:,}")
                    print(f"     👁️  Avg views: {avg_views or 0}")
                    print(f"     🔄 Avg forwards: {avg_forwards or 0}")
                    if first_post:
                        print(f"     🕐 First post: {first_post}")
                    if last_post:
                        print(f"     🕐 Last post: {last_post}")
                    print()
                
                print("=" * 80)
                
                return total_count
                
        except Exception as e:
            print(f"❌ Error verifying data: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def create_schemas(self):
        """Create necessary schemas for the warehouse."""
        try:
            with self.connection.cursor() as cursor:
                schemas = ['raw', 'staging', 'analytics']
                for schema in schemas:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                    print(f"✅ Schema '{schema}' created/verified")
                self.connection.commit()
        except Exception as e:
            print(f"❌ Error creating schemas: {e}")
    
    def run(self, clear_existing: bool = False, specific_date: str = None):
        """Run complete data loading process."""
        print("=" * 80)
        print("📥 LOADING RAW TELEGRAM DATA TO POSTGRESQL")
        print("=" * 80)
        print(f"📁 Data directory: {self.data_dir.absolute()}")
        
        # Check if data directory exists
        if not self.data_dir.exists():
            print(f"❌ Data directory not found: {self.data_dir}")
            print(f"   Create data structure with: data/raw/telegram_messages/YYYY-MM-DD/*.json")
            return
        
        # Connect to database
        if not self.connect():
            return
        
        # Create schemas
        self.create_schemas()
        
        # Create table
        self.create_raw_table()
        
        # Clear existing data if requested
        if clear_existing:
            self.clear_existing_data(specific_date)
        
        # Load data
        total_messages = self.load_all_data(specific_date)
        
        # Verify and show summary
        if total_messages > 0:
            self.verify_data()
            print(f"\n✅ Successfully loaded {total_messages:,} messages")
        else:
            print("\n⚠️  No messages were loaded")
            print("   Check if you have data in: data/raw/telegram_messages/")
            print("   Folder structure should be:")
            print("   ├── data/raw/telegram_messages/")
            print("   │   ├── 2026-01-16/")
            print("   │   │   ├── channel1.json")
            print("   │   │   └── channel2.json")
            print("   │   └── 2026-01-18/")
            print("   │       ├── channel1.json")
            print("   │       └── channel2.json")
        
        # Close connection
        if self.connection:
            self.connection.close()
            print("🔌 Database connection closed")
        
        print("=" * 80)


def main():
    """Command line interface."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Load raw Telegram data to PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Load all data
  %(prog)s --clear            # Clear all data and reload
  %(prog)s --date 2026-01-16  # Load specific date only
  %(prog)s --clear --date 2026-01-18  # Clear and load specific date
        """
    )
    
    parser.add_argument(
        '--clear', 
        action='store_true',
        help='Clear existing data before loading'
    )
    
    parser.add_argument(
        '--date',
        type=str,
        help='Load specific date folder (YYYY-MM-DD)'
    )
    
    args = parser.parse_args()
    
    # Validate date format if provided
    if args.date:
        try:
            datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError:
            print(f"❌ Invalid date format: {args.date}")
            print("   Use format: YYYY-MM-DD (e.g., 2026-01-16)")
            return
    
    loader = DataLoader()
    loader.run(
        clear_existing=args.clear,
        specific_date=args.date
    )


if __name__ == "__main__":
    main()