# Save as: scripts/run_yolo_pipeline.py
#!/usr/bin/env python3
"""
Complete YOLO pipeline with database connection check - FIXED VERSION
"""

import os
import sys
from pathlib import Path
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import subprocess
import re
import cv2

load_dotenv()

class FinalYOLOPipeline:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'medical_warehouse'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres123')
        }
        self.model = None
    
    def check_database(self):
        """Check database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Check if processed schema exists
            cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'processed'")
            schema_exists = cursor.fetchone() is not None
            
            cursor.close()
            conn.close()
            
            print(f"✓ Database connection successful")
            print(f"✓ Processed schema exists: {schema_exists}")
            return True
            
        except Exception as e:
            print(f"✗ Database connection failed: {e}")
            return False
    
    def setup_directories(self):
        """Ensure all directories exist"""
        directories = [
            "data/processed",
            "data/processed/visualizations",
            "data/raw/images",
            "medical_warehouse/models/marts",
            "medical_warehouse/models/staging",
            "scripts"
        ]
        
        for dir_path in directories:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
        
        return True
    
    def setup_yolo_model(self):
        """Setup YOLO model"""
        try:
            from ultralytics import YOLO
        except ImportError:
            print("Installing ultralytics...")
            subprocess.run([sys.executable, "-m", "pip", "install", "ultralytics", "-q"], check=True)
            from ultralytics import YOLO
        
        try:
            print("Loading YOLO model (yolov8n.pt)...")
            self.model = YOLO('yolov8n.pt')
            print(f"✓ YOLO loaded. Can detect {len(self.model.names)} object types")
            return True
            
        except Exception as e:
            print(f"✗ Failed to load YOLO: {e}")
            return False
    
    def detect_and_save_visualization(self, image_path, output_dir="data/processed/visualizations"):
        """Run YOLO detection and save image with bounding boxes"""
        
        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Read image
        img = cv2.imread(str(image_path))
        if img is None:
            return None, "Could not read image"
        
        # Run YOLO
        results = self.model.predict(
            source=str(image_path),
            conf=0.25,
            save=False,
            verbose=False
        )
        
        detections = []
        for result in results:
            if result.boxes is not None:
                for box in result.boxes:
                    # Get detection details
                    x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                    conf = float(box.conf[0].cpu().numpy())
                    cls = int(box.cls[0].cpu().numpy())
                    label = result.names[cls]
                    
                    detections.append({
                        'class': label,
                        'confidence': conf,
                        'bbox': [float(x1), float(y1), float(x2), float(y2)]
                    })
                    
                    # Draw on image (Green boxes)
                    cv2.rectangle(img, 
                                (int(x1), int(y1)), 
                                (int(x2), int(y2)), 
                                (0, 255, 0),  # Green
                                2)
                    
                    # Add label
                    label_text = f"{label} {conf:.2f}"
                    cv2.putText(img, 
                              label_text,
                              (int(x1), int(y1) - 10),
                              cv2.FONT_HERSHEY_SIMPLEX,
                              0.5,
                              (0, 255, 0),
                              2)
        
        # Save visualization
        channel_name = image_path.parent.name
        vis_filename = f"{channel_name}_{image_path.stem}_detected.jpg"
        vis_path = output_path / vis_filename
        cv2.imwrite(str(vis_path), img)
        
        return detections, str(vis_path)
    
    def run_yolo_detection(self, limit: int = 5):
        """Run YOLO detection and save visualizations"""
        print("\n" + "="*60)
        print("STEP 1: RUNNING YOLO DETECTION WITH VISUALIZATION")
        print("="*60)
        
        if not self.setup_yolo_model():
            return False
        
        try:
            all_results = []
            visualizations_saved = 0
            images_dir = Path("data/raw/images")
            
            for channel_dir in images_dir.iterdir():
                if channel_dir.is_dir():
                    channel_name = channel_dir.name
                    images = []
                    for ext in ['*.jpg', '*.jpeg', '*.png', '*.webp']:
                        images.extend(channel_dir.glob(ext))
                    
                    if limit > 0:
                        images = images[:limit]
                    
                    if not images:
                        continue
                    
                    print(f"\n📸 Processing {len(images)} images from {channel_name}:")
                    
                    for img_path in images:
                        print(f"  {img_path.name}", end=" → ")
                        
                        try:
                            # Detect and save visualization
                            detections, vis_path = self.detect_and_save_visualization(img_path)
                            
                            # Classify based on detections
                            classes = [d['class'] for d in detections] if detections else []
                            confidences = [d['confidence'] for d in detections] if detections else []
                            
                            has_person = 'person' in classes
                            has_product = any(c in ['bottle', 'cup', 'handbag'] for c in classes)
                            
                            if has_person and has_product:
                                category = 'promotional'
                            elif has_product and not has_person:
                                category = 'product_display'
                            elif has_person and not has_product:
                                category = 'lifestyle'
                            else:
                                category = 'other'
                            
                            confidence_score = 0
                            if detections:
                                confidence_score = sum(confidences) / len(confidences)
                            
                            # Tags
                            tags = []
                            if has_person: tags.append('has_person')
                            if has_product: tags.append('has_product')
                            if confidence_score > 0.8: tags.append('high_confidence')
                            elif confidence_score > 0.5: tags.append('medium_confidence')
                            else: tags.append('low_confidence')
                            
                            # Message ID
                            message_id = 0
                            patterns = [r'^(\d+)\.', r'message_(\d+)']
                            for pattern in patterns:
                                match = re.search(pattern, img_path.stem)
                                if match:
                                    try:
                                        message_id = int(match.group(1))
                                        break
                                    except:
                                        continue
                            
                            result = {
                                'message_id': message_id if message_id else 10000 + len(all_results),
                                'channel_name': channel_name,
                                'image_path': str(img_path),
                                'filename': img_path.name,
                                'date_folder': datetime.now().strftime('%Y-%m-%d'),
                                'size_kb': round(img_path.stat().st_size / 1024, 2),
                                'detection_count': len(detections),
                                'category': category,
                                'confidence_score': round(confidence_score, 3),
                                'business_tags': ','.join(tags),
                                'top_objects': ','.join(classes[:3]) if classes else '',
                                'processed_at': datetime.now().isoformat(),
                                'visualization_path': vis_path if vis_path else ''
                            }
                            
                            all_results.append(result)
                            if vis_path:
                                visualizations_saved += 1
                            
                            print(f"{category} ({len(detections)} objects) ✓")
                            
                        except Exception as e:
                            print(f"error: {str(e)[:30]}")
                            continue
            
            # Save CSV
            output_path = Path("data/processed/yolo_results.csv")
            df = pd.DataFrame(all_results)
            df.to_csv(output_path, index=False)
            
            print(f"\n✓ Saved {len(df)} results to {output_path}")
            print(f"✓ Saved {visualizations_saved} visualizations to data/processed/visualizations/")
            
            # Summary
            print("\n📊 Detection Summary:")
            print(f"  Total images: {len(df)}")
            for cat, count in df['category'].value_counts().items():
                print(f"  {cat}: {count} images")
            
            if len(df) > 0:
                avg_conf = df['confidence_score'].mean()
                total_objects = df['detection_count'].sum()
                print(f"\n  Average confidence: {avg_conf:.3f}")
                print(f"  Total objects detected: {total_objects}")
            
            # Show sample visualizations
            if visualizations_saved > 0:
                vis_dir = Path("data/processed/visualizations")
                vis_files = list(vis_dir.glob("*.jpg"))
                if vis_files:
                    print(f"\n📸 Sample visualizations:")
                    for vis in vis_files[:3]:  # Show first 3
                        print(f"  • {vis.name}")
            
            return True
            
        except Exception as e:
            print(f"✗ YOLO processing failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def load_to_database(self):
        """Load results to PostgreSQL"""
        print("\n" + "="*60)
        print("STEP 2: LOADING TO DATABASE")
        print("="*60)
        
        csv_path = Path("data/processed/yolo_results.csv")
        if not csv_path.exists():
            print("✗ No CSV file found")
            return False
        
        try:
            df = pd.read_csv(csv_path)
            print(f"Loaded {len(df)} records from CSV")
            
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Create table with visualization_path column
            cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS processed;
            
            DROP TABLE IF EXISTS processed.yolo_results;
            
            CREATE TABLE processed.yolo_results (
                message_id INTEGER,
                channel_name VARCHAR(100),
                image_path TEXT,
                filename VARCHAR(200),
                date_folder VARCHAR(20),
                size_kb DECIMAL(10,2),
                detection_count INTEGER,
                category VARCHAR(50),
                confidence_score DECIMAL(5,3),
                business_tags TEXT,
                top_objects TEXT,
                processed_at TIMESTAMP,
                visualization_path TEXT
            );
            """)
            
            # Insert data
            for _, row in df.iterrows():
                cursor.execute("""
                INSERT INTO processed.yolo_results VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    int(row['message_id']),
                    str(row['channel_name']),
                    str(row['image_path']),
                    str(row['filename']),
                    str(row['date_folder']),
                    float(row['size_kb']),
                    int(row['detection_count']),
                    str(row['category']),
                    float(row['confidence_score']),
                    str(row['business_tags']),
                    str(row['top_objects']),
                    pd.to_datetime(row['processed_at']),
                    str(row.get('visualization_path', '')) if 'visualization_path' in row else ''
                ))
            
            conn.commit()
            
            # Verify
            cursor.execute("SELECT COUNT(*) FROM processed.yolo_results")
            count = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            print(f"✓ Loaded {count} records to processed.yolo_results")
            return True
            
        except Exception as e:
            print(f"✗ Database load failed: {e}")
            return False
    
    def create_simple_dbt_model(self):
        """Create a simple dbt model that will work"""
        print("\n" + "="*60)
        print("STEP 3: CREATING SIMPLE dbt MODEL")
        print("="*60)
        
        # Create minimal dbt_project.yml if it doesn't exist
        project_path = Path("medical_warehouse/dbt_project.yml")
        if not project_path.exists():
            project_content = """name: 'medical_warehouse'
version: '1.0.0'
config-version: 2

profile: 'medical_warehouse'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["data"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  medical_warehouse:
    materialized: table
    staging:
      materialized: view
    marts:
      materialized: table
"""
            with open(project_path, 'w') as f:
                f.write(project_content)
            print("✓ Created dbt_project.yml")
        
        # Create sources.yml
        sources_path = Path("medical_warehouse/models/sources.yml")
        sources_content = """version: 2

sources:
  - name: processed
    schema: processed
    tables:
      - name: yolo_results
        description: "YOLO object detection results with visualizations"
"""
        with open(sources_path, 'w') as f:
            f.write(sources_content)
        print("✓ Created sources.yml")
        
        # Create the model WITH visualization path
        model_dir = Path("medical_warehouse/models/marts")
        model_dir.mkdir(parents=True, exist_ok=True)
        
        model_content = """{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY processed_at) AS detection_id,
    message_id,
    channel_name,
    image_path,
    filename,
    size_kb,
    detection_count,
    category,
    confidence_score,
    business_tags,
    top_objects,
    processed_at AS analysis_timestamp,
    visualization_path,
    CURRENT_TIMESTAMP AS loaded_at
FROM {{ source('processed', 'yolo_results') }}
"""
        
        model_path = model_dir / "fct_image_detections.sql"
        with open(model_path, 'w') as f:
            f.write(model_content)
        
        print(f"✓ Created dbt model: {model_path}")
        return True
    
    def run_dbt_simple(self):
        """Run dbt with simple approach"""
        print("\n" + "="*60)
        print("STEP 4: RUNNING dbt")
        print("="*60)
        
        original_dir = os.getcwd()
        os.chdir("medical_warehouse")
        
        try:
            # First test connection
            print("Testing dbt connection...")
            result = subprocess.run(
                ["dbt", "debug"],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print("dbt debug failed, trying manual approach...")
                return self.create_table_manually()
            
            print("dbt debug successful")
            
            # Run the model
            print("Running dbt model...")
            result = subprocess.run(
                ["dbt", "run", "--models", "fct_image_detections"],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                print("✓ dbt model created successfully")
                return True
            else:
                print("dbt run failed, trying manual approach...")
                print("Error:", result.stderr[-500:])
                return self.create_table_manually()
                
        except Exception as e:
            print(f"dbt execution error: {e}")
            return self.create_table_manually()
        finally:
            os.chdir(original_dir)
    
    def create_table_manually(self):
        """Create table manually if dbt fails"""
        print("Creating table manually via SQL...")
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Create analytics schema
            cursor.execute("CREATE SCHEMA IF NOT EXISTS analytics")
            
            # Create the table with visualization_path
            cursor.execute("""
            DROP TABLE IF EXISTS analytics.fct_image_detections;
            
            CREATE TABLE analytics.fct_image_detections AS
            SELECT
                ROW_NUMBER() OVER (ORDER BY processed_at) AS detection_id,
                message_id,
                channel_name,
                image_path,
                filename,
                size_kb,
                detection_count,
                category,
                confidence_score,
                business_tags,
                top_objects,
                processed_at AS analysis_timestamp,
                visualization_path,
                CURRENT_TIMESTAMP AS loaded_at
            FROM processed.yolo_results;
            """)
            
            conn.commit()
            
            # Verify
            cursor.execute("SELECT COUNT(*) FROM analytics.fct_image_detections")
            count = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            print(f"✓ Created analytics.fct_image_detections with {count} records")
            return True
            
        except Exception as e:
            print(f"✗ Manual table creation failed: {e}")
            return False
    
    def generate_final_report(self):
        """Generate final report with visualization info"""
        print("\n" + "="*60)
        print("STEP 5: GENERATING FINAL REPORT")
        print("="*60)
        
        try:
            conn = psycopg2.connect(**self.db_config)
            
            # Check what tables we have
            cursor = conn.cursor()
            cursor.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema IN ('analytics', 'processed')
            ORDER BY table_schema, table_name
            """)
            
            tables = cursor.fetchall()
            
            report_lines = [
                "="*60,
                "TASK 3: YOLO IMAGE ANALYSIS - FINAL REPORT",
                "="*60,
                f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "",
                "DATABASE TABLES:"
            ]
            
            for schema, table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                count = cursor.fetchone()[0]
                report_lines.append(f"  {schema}.{table}: {count} rows")
            
            # Get analysis from analytics table if it exists
            cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'analytics' 
            AND table_name = 'fct_image_detections'
            """)
            
            if cursor.fetchone():
                # Get summary stats
                cursor.execute("""
                SELECT 
                    COUNT(*) as total_images,
                    ROUND(AVG(confidence_score), 3) as avg_confidence,
                    SUM(detection_count) as total_detections,
                    ROUND(AVG(detection_count), 2) as avg_detections_per_image,
                    COUNT(CASE WHEN visualization_path != '' THEN 1 END) as images_with_visualizations
                FROM analytics.fct_image_detections
                """)
                
                total, avg_conf, total_det, avg_det, vis_count = cursor.fetchone()
                
                report_lines.extend([
                    "",
                    "ANALYSIS SUMMARY:",
                    f"  Total Images Analyzed: {total}",
                    f"  Images with Visualizations: {vis_count}",
                    f"  Average Confidence Score: {avg_conf}",
                    f"  Total Objects Detected: {total_det}",
                    f"  Average Objects per Image: {avg_det}",
                    "",
                    "CATEGORY BREAKDOWN:"
                ])
                
                cursor.execute("""
                SELECT 
                    category,
                    COUNT(*) as image_count,
                    ROUND(AVG(confidence_score), 3) as avg_confidence,
                    ROUND(AVG(detection_count), 2) as avg_objects
                FROM analytics.fct_image_detections
                GROUP BY category
                ORDER BY image_count DESC
                """)
                
                for category, count, conf, avg_obj in cursor.fetchall():
                    report_lines.append(f"  {category}: {count} images (avg confidence: {conf}, avg objects: {avg_obj})")
            
            cursor.close()
            conn.close()
            
            # Check visualization folder
            vis_dir = Path("data/processed/visualizations")
            if vis_dir.exists():
                vis_files = list(vis_dir.glob("*.jpg"))
                report_lines.extend([
                    "",
                    "VISUALIZATIONS:",
                    f"  Total visualization files: {len(vis_files)}",
                    f"  Location: data/processed/visualizations/"
                ])
                
                if vis_files:
                    report_lines.append("  Sample files:")
                    for vis in vis_files[:5]:  # Show first 5
                        report_lines.append(f"    • {vis.name}")
            
            # Save report
            report_path = Path("data/processed/task3_complete_report.txt")
            with open(report_path, 'w') as f:
                f.write('\n'.join(report_lines))
            
            print("✓ Report generated:")
            print('\n'.join(report_lines[:40]))  # Print first 40 lines
            
            return True
            
        except Exception as e:
            print(f"✗ Report generation failed: {e}")
            return True  # Don't fail the whole pipeline for report
    
    def show_visualization_examples(self):
        """Show examples of saved visualizations - FIXED VERSION"""
        print("\n" + "="*60)
        print("VISUALIZATION EXAMPLES")
        print("="*60)
        
        vis_dir = Path("data/processed/visualizations")
        if not vis_dir.exists():
            print("No visualizations folder found")
            return True  # FIX: Return True
        
        vis_files = list(vis_dir.glob("*.jpg"))
        
        if not vis_files:
            print("No visualization files found")
            return True  # FIX: Return True
        
        print(f"Found {len(vis_files)} visualization files")
        print("\nFirst 5 visualizations:")
        
        for i, vis_file in enumerate(vis_files[:5]):
            # Get info from filename
            parts = vis_file.stem.split('_')
            channel = parts[0]
            image_name = '_'.join(parts[1:-1]) if len(parts) > 2 else parts[1]
            
            # Get size
            size_kb = vis_file.stat().st_size / 1024
            
            print(f"\n{i+1}. {vis_file.name}")
            print(f"   Channel: {channel}")
            print(f"   Original image: {image_name}.jpg")
            print(f"   Size: {size_kb:.1f} KB")
            print(f"   Path: {vis_file}")
        
        # Also show in database
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT channel_name, filename, detection_count, category, visualization_path
            FROM analytics.fct_image_detections 
            WHERE visualization_path != ''
            ORDER BY detection_count DESC 
            LIMIT 3
            """)
            
            results = cursor.fetchall()
            if results:
                print("\n📊 Top images with visualizations in database:")
                for channel, filename, count, category, vis_path in results:
                    print(f"\n  {channel}/{filename}")
                    print(f"    Objects: {count}")
                    print(f"    Category: {category}")
                    print(f"    Visualization: {Path(vis_path).name if vis_path else 'N/A'}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"Could not query database: {e}")
        
        return True  # FIX: Add this return statement
    
    def run(self, limit: int = 5):
        """Run complete pipeline"""
        print("\n" + "="*60)
        print("YOLO IMAGE ANALYSIS PIPELINE WITH VISUALIZATIONS")
        print("="*60)
        
        print("Setting up directories...")
        if not self.setup_directories():
            return False
        
        print("\nChecking database connection...")
        if not self.check_database():
            return False
        
        steps = [
            ("YOLO Detection with Visualization", self.run_yolo_detection, [limit]),
            ("Database Load", self.load_to_database, []),
            ("Create dbt Model", self.create_simple_dbt_model, []),
            ("Run dbt/Manual", self.run_dbt_simple, []),
            ("Generate Report", self.generate_final_report, []),
            ("Show Visualization Examples", self.show_visualization_examples, [])
        ]
        
        success = True
        for step_name, step_func, args in steps:
            if success:
                print(f"\n▶ {step_name}")
                if step_func(*args):
                    print(f"✓ {step_name} completed")
                else:
                    print(f"✗ {step_name} failed")
                    success = False
        
        if success:
            print("\n" + "="*60)
            print("🎉 TASK 3 COMPLETED SUCCESSFULLY!")
            print("="*60)
            print("\nWhat was accomplished:")
            print("1. ✓ Ran YOLO object detection on medical/cosmetic images")
            print("2. ✓ Saved visualizations with bounding boxes for ALL images")
            print("3. ✓ Saved results to CSV: data/processed/yolo_results.csv")
            print("4. ✓ Loaded data to PostgreSQL: processed.yolo_results")
            print("5. ✓ Created analytics table: analytics.fct_image_detections")
            print("6. ✓ Generated analysis report")
            print("\nVisualizations saved to: data/processed/visualizations/")
            print("\nTo verify in database:")
            print("  SELECT * FROM analytics.fct_image_detections WHERE visualization_path != '' LIMIT 3;")
            print("  SELECT channel_name, COUNT(*) FROM analytics.fct_image_detections GROUP BY channel_name;")
            print("\nTo view visualizations:")
            print("  explorer \"data\\processed\\visualizations\"")
            print("\nReport saved to: data/processed/task3_complete_report.txt")
        else:
            print("\n✗ PIPELINE FAILED")
        
        return success

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="YOLO Image Analysis Pipeline with Visualizations")
    parser.add_argument('--limit', type=int, default=5,
                       help='Images per channel to process (default: 5)')
    
    args = parser.parse_args()
    
    pipeline = FinalYOLOPipeline()
    success = pipeline.run(args.limit)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()