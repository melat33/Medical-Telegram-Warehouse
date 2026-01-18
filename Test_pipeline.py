#!/usr/bin/env python3
"""
Test the complete pipeline with mock data
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import random

def create_mock_data():
    """Create mock Telegram data for testing."""
    print("🧪 Creating mock data for testing...")
    
    data_dir = Path("data/raw/telegram_messages")
    today = datetime.now().strftime("%Y-%m-%d")
    today_dir = data_dir / today
    today_dir.mkdir(parents=True, exist_ok=True)
    
    # Sample channels
    channels = [
        {"name": "tikvahpharma", "type": "pharmaceuticals", "title": "Tikvah Pharma"},
        {"name": "cheMed123", "type": "medical", "title": "CheMed Medical Products"},
        {"name": "lobelia4cosmetics", "type": "cosmetics", "title": "Lobelia Cosmetics"},
        {"name": "tenamereja", "type": "general", "title": "Tenamereja Health"}
    ]
    
    # Medical products
    products = [
        "Paracetamol 500mg", "Amoxicillin 250mg", "Vitamin C 1000mg",
        "Ibuprofen 400mg", "Metformin 500mg", "Losartan 50mg",
        "Skin Cream 50ml", "Hand Sanitizer 500ml", "Face Mask Pack of 50",
        "Multivitamin Tablets", "Protein Powder", "Omega-3 Capsules"
    ]
    
    total_messages = 0
    
    for channel in channels:
        messages = []
        num_messages = random.randint(15, 30)
        
        for i in range(num_messages):
            days_ago = random.randint(0, 30)
            message_date = datetime.now() - timedelta(days=days_ago, hours=random.randint(0, 23))
            
            product = random.choice(products)
            price = random.randint(50, 500)
            
            message = {
                "message_id": i + 1,
                "channel_name": channel["name"],
                "channel_title": channel["title"],
                "message_date": message_date.isoformat(),
                "message_text": f"{product} available at {price} ETB. Call 0911-XX-XX-XX for orders.",
                "has_media": random.choice([True, False]),
                "image_path": f"data/raw/images/{channel['name']}/{i+1}.jpg" if random.random() > 0.7 else None,
                "views": random.randint(100, 5000),
                "forwards": random.randint(0, 100),
                "extracted_at": datetime.now().isoformat()
            }
            messages.append(message)
        
        # Save channel data
        json_file = today_dir / f"{channel['name']}.json"
        data = {
            "metadata": {
                "channel": channel["name"],
                "extraction_date": today,
                "processed_at": datetime.now().isoformat(),
                "message_count": len(messages),
                "schema_version": "1.0"
            },
            "data": messages
        }
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        total_messages += len(messages)
        print(f"  ✓ Created {len(messages)} messages for @{channel['name']}")
    
    # Create manifest
    manifest = {
        "manifest": {
            "date": today,
            "generated_at": datetime.now().isoformat(),
            "total_messages": total_messages,
            "channels": {ch["name"]: 25 for ch in channels},
            "storage_structure": {
                "messages": f"data/raw/telegram_messages/{today}/*.json",
                "images": "data/raw/images/{channel_name}/{message_id}.jpg"
            }
        }
    }
    
    manifest_file = today_dir / "_manifest.json"
    with open(manifest_file, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2)
    
    print(f"\n✅ Created {total_messages} mock messages in {today_dir}")
    return total_messages

def test_database_connection():
    """Test PostgreSQL connection."""
    print("\n🔗 Testing database connection...")
    
    try:
        import psycopg2
        from dotenv import load_dotenv
        load_dotenv()
        
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'medical_warehouse'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'postgres123')
        )
        
        print("✅ PostgreSQL connection successful")
        
        # Create raw schema and table
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            cursor.execute("""
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
                    raw_data JSONB
                )
            """)
            conn.commit()
            print("✅ Created raw.telegram_messages table")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def test_dbt():
    """Test dbt installation and configuration."""
    print("\n⚙️  Testing dbt...")
    
    try:
        import subprocess
        
        # Check dbt installation
        result = subprocess.run(["dbt", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ dbt is installed")
        else:
            print("❌ dbt not found. Installing...")
            subprocess.run(["pip", "install", "dbt-postgres"], check=True)
            print("✅ dbt installed")
        
        # Initialize dbt project if not exists
        dbt_dir = Path("medical_warehouse")
        if not dbt_dir.exists():
            print("📁 Creating dbt project...")
            subprocess.run(["dbt", "init", "medical_warehouse"], check=True)
            print("✅ dbt project created")
        else:
            print("✅ dbt project exists")
        
        # Configure dbt profiles
        profiles_dir = Path.home() / ".dbt"
        profiles_dir.mkdir(exist_ok=True)
        
        profiles_content = """medical_warehouse:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "localhost"
      user: "postgres"
      pass: "postgres123"
      port: 5432
      dbname: "medical_warehouse"
      schema: "analytics"
      threads: 4
"""
        
        profiles_file = profiles_dir / "profiles.yml"
        profiles_file.write_text(profiles_content)
        print(f"✅ dbt profiles configured at: {profiles_file}")
        
        # Test dbt debug
        print("\n🔧 Testing dbt debug...")
        result = subprocess.run(["dbt", "debug"], cwd="medical_warehouse", capture_output=True, text=True)
        if "All checks passed" in result.stdout:
            print("✅ dbt debug passed")
        else:
            print(f"⚠️  dbt debug issues: {result.stderr}")
        
        return True
        
    except Exception as e:
        print(f"❌ dbt test failed: {e}")
        return False

def main():
    """Run complete pipeline test."""
    print("=" * 80)
    print("🧪 MEDICAL TELEGRAM PIPELINE - COMPLETE TEST")
    print("=" * 80)
    
    # Step 1: Create mock data
    create_mock_data()
    
    # Step 2: Test database
    if not test_database_connection():
        return
    
    # Step 3: Test dbt
    if not test_dbt():
        return
    
    # Step 4: Load data to PostgreSQL
    print("\n📥 Loading data to PostgreSQL...")
    from scripts.load_raw_to_postgres import DataLoader
    loader = DataLoader()
    loader.run(clear_existing=True)
    
    print("\n" + "=" * 80)
    print("✅ TEST COMPLETE!")
    print("=" * 80)
    print("\nNext steps:")
    print("1. Check PostgreSQL: docker exec -it medical_warehouse_db psql -U postgres -d medical_warehouse")
    print("2. Run dbt: cd medical_warehouse && dbt run")
    print("3. Access PGAdmin: http://localhost:5050 (admin@medical.com / admin123)")
    print("=" * 80)

if __name__ == "__main__":
    main()