import os
import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import argparse
from tqdm import tqdm
import re

# Try to import YOLO
try:
    from ultralytics import YOLO
    YOLO_AVAILABLE = True
except ImportError:
    YOLO_AVAILABLE = False
    print("WARNING: ultralytics not installed. Install with: pip install ultralytics")

class YOLODetectionPipeline:
    """Complete YOLO detection pipeline for medical Telegram images"""
    
    def __init__(self, model_name: str = 'yolov8n.pt'):
        self.model_name = model_name
        self.model = None
        self.results = []
        
    def setup(self):
        """Setup YOLO model"""
        print(f"[INFO] Setting up YOLO model: {self.model_name}")
        
        if not YOLO_AVAILABLE:
            print("[ERROR] ultralytics not installed. Run: pip install ultralytics")
            return False
        
        try:
            # Load YOLO model (will download if not exists)
            self.model = YOLO(self.model_name)
            print("[SUCCESS] Model loaded successfully")
            return True
            
        except Exception as e:
            print(f"[ERROR] Failed to setup YOLO model: {e}")
            return False
    
    def find_telegram_images(self, base_path: str = "data/raw/telegram_messages") -> List[Dict]:
        """
        Find all Telegram images in the data structure
        """
        
        print(f"[INFO] Searching for Telegram images in: {base_path}")
        
        images_info = []
        base_path = Path(base_path)
        
        if not base_path.exists():
            print(f"[ERROR] Base path does not exist: {base_path}")
            return images_info
        
        # Find all date folders
        date_folders = [d for d in base_path.iterdir() if d.is_dir()]
        
        for date_folder in date_folders:
            # Find all channel folders
            for item in date_folder.iterdir():
                if item.is_dir() and not item.name.endswith('.json'):
                    channel_name = item.name
                    
                    # Look for images folder
                    images_folder = item / "images"
                    if images_folder.exists() and images_folder.is_dir():
                        for image_file in images_folder.glob("*"):
                            if image_file.suffix.lower() in ['.jpg', '.jpeg', '.png', '.webp']:
                                
                                message_id = self._extract_message_id(image_file.name, date_folder, channel_name)
                                
                                images_info.append({
                                    'image_path': str(image_file.absolute()),
                                    'date_folder': date_folder.name,
                                    'channel_name': channel_name,
                                    'message_id': message_id,
                                    'filename': image_file.name,
                                    'size_kb': round(image_file.stat().st_size / 1024, 2)
                                })
        
        print(f"[INFO] Found {len(images_info)} images")
        return images_info
    
    def _extract_message_id(self, filename: str, date_folder: Path, channel_name: str) -> Optional[int]:
        """Extract message ID from filename"""
        
        patterns = [
            r'message_(\d+)',    # message_12345
            r'msg_(\d+)',        # msg_12345  
            r'^(\d+)\.',         # 12345.jpg
            r'_(\d+)\.',         # anything_12345.jpg
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                try:
                    return int(match.group(1))
                except:
                    continue
        
        # If no pattern matches, return None
        return None
    
    def detect_objects(self, image_path: str) -> Dict:
        """Run YOLO object detection on a single image"""
        
        if not self.model:
            raise ValueError("Model not initialized")
        
        try:
            # Run inference
            results = self.model.predict(
                source=image_path,
                conf=0.25,      # Confidence threshold
                verbose=False   # Don't print progress
            )
            
            detections = []
            
            for result in results:
                if result.boxes is not None:
                    for box in result.boxes:
                        class_id = int(box.cls[0])
                        class_name = result.names[class_id]
                        confidence = float(box.conf[0])
                        
                        # Only keep relevant objects
                        relevant_classes = ['person', 'bottle', 'cup', 'handbag', 'cell phone', 
                                          'book', 'chair', 'dining table', 'laptop', 'tv',
                                          'banana', 'apple', 'orange', 'broccoli', 'carrot',
                                          'dog', 'cat', 'bird', 'bed', 'toilet']
                        
                        if class_name in relevant_classes:
                            detections.append({
                                'class': class_name,
                                'confidence': confidence
                            })
            
            return {
                'image_path': image_path,
                'detections': detections,
                'detection_count': len(detections),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"[ERROR] Error detecting objects in {image_path}: {e}")
            return {
                'image_path': image_path,
                'detections': [],
                'detection_count': 0,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def classify_image(self, detections: List[Dict]) -> Dict:
        """Classify image based on detections"""
        
        if not detections:
            return {
                'category': 'unknown',
                'confidence_score': 0.0,
                'business_tags': 'no_detections,low_confidence',
                'top_objects': '',
                'business_insights': json.dumps({
                    'category': 'unknown',
                    'confidence': 0.0,
                    'tags': ['no_detections'],
                    'business_implications': ['No objects detected']
                })
            }
        
        # Extract classes and confidences
        classes = [d['class'] for d in detections]
        confidences = [d['confidence'] for d in detections]
        
        has_person = 'person' in classes
        has_bottle = 'bottle' in classes
        has_product = any(c in ['bottle', 'cup', 'handbag', 'wine glass'] for c in classes)
        has_food = any(c in ['banana', 'apple', 'orange', 'broccoli', 'carrot'] for c in classes)
        
        # Determine category
        if has_person and has_product:
            category = 'promotional'
        elif has_product and not has_person:
            category = 'product_display'
        elif has_person and not has_product:
            category = 'lifestyle'
        elif has_food:
            category = 'ingredient_showcase'
        else:
            category = 'other'
        
        # Calculate confidence score
        confidence_score = sum(confidences) / len(confidences) if confidences else 0
        
        # Create business tags
        business_tags = []
        if has_person:
            business_tags.append('has_person')
        if has_product:
            business_tags.append('has_product')
        if has_food:
            business_tags.append('has_ingredients')
        
        if confidence_score > 0.8:
            business_tags.append('high_confidence')
            confidence_level = 'high'
        elif confidence_score > 0.5:
            business_tags.append('medium_confidence')
            confidence_level = 'medium'
        else:
            business_tags.append('low_confidence')
            confidence_level = 'low'
        
        # Generate business insights
        business_implications = []
        if category == 'promotional':
            business_implications = ["Good for brand storytelling", "Likely marketing content"]
        elif category == 'product_display':
            business_implications = ["Pure product showcase", "Good for catalog/details"]
        elif category == 'lifestyle':
            business_implications = ["Focus on user experience", "Builds brand connection"]
        
        return {
            'category': category,
            'confidence_score': round(confidence_score, 3),
            'business_tags': ','.join(business_tags),
            'top_objects': ','.join(classes[:3]) if classes else '',
            'business_insights': json.dumps({
                'category': category,
                'confidence': confidence_score,
                'confidence_level': confidence_level,
                'tags': business_tags,
                'detection_summary': {
                    'total_objects': len(detections),
                    'top_objects': classes[:3] if classes else []
                },
                'business_implications': business_implications
            })
        }
    
    def process_images(self, images_info: List[Dict], limit: int = 0) -> List[Dict]:
        """Process multiple images"""
        
        if limit > 0 and limit < len(images_info):
            images_info = images_info[:limit]
            print(f"[INFO] Limiting to {limit} images")
        
        print(f"[INFO] Processing {len(images_info)} images...")
        
        all_results = []
        
        for img_info in tqdm(images_info, desc="Processing images"):
            # Run detection
            detection_result = self.detect_objects(img_info['image_path'])
            
            # Classify image
            classification = self.classify_image(detection_result['detections'])
            
            # Combine all information
            result = {
                **img_info,
                'detections': detection_result['detections'],
                'detection_count': detection_result['detection_count'],
                'processed_at': datetime.now().isoformat(),
                **classification
            }
            
            # Add error if exists
            if 'error' in detection_result:
                result['error'] = detection_result['error']
            
            all_results.append(result)
        
        self.results = all_results
        return all_results
    
    def save_results(self, output_path: str = "data/processed/yolo_results.csv"):
        """Save results to CSV file"""
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if not self.results:
            print("[WARNING] No results to save")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(self.results)
        
        # Remove complex columns for CSV export
        if 'detections' in df.columns:
            df = df.drop(columns=['detections'])
        
        # Save to CSV
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"[INFO] Results saved to: {output_path} ({len(df)} records)")
        
        # Print summary
        self.print_summary(df)
        
        return output_path
    
    def print_summary(self, df: pd.DataFrame):
        """Print analysis summary"""
        
        print("\n" + "="*60)
        print("YOLO DETECTION SUMMARY")
        print("="*60)
        
        if df.empty:
            print("No data to summarize")
            return
        
        total_images = len(df)
        images_with_detections = len(df[df['detection_count'] > 0])
        
        print(f"Total Images Processed: {total_images}")
        print(f"Images with Detections: {images_with_detections} ({images_with_detections/total_images*100:.1f}%)")
        
        # Category distribution
        if 'category' in df.columns:
            print("\nCategory Distribution:")
            category_counts = df['category'].value_counts()
            for category, count in category_counts.items():
                percentage = count / total_images * 100
                print(f"   {category:20s}: {count:4d} images ({percentage:5.1f}%)")
        
        # Average confidence
        if 'confidence_score' in df.columns:
            avg_confidence = df['confidence_score'].mean()
            print(f"\nAverage Confidence: {avg_confidence:.3f}")
        
        print("="*60)

def main():
    """Main function"""
    
    parser = argparse.ArgumentParser(description="YOLO Object Detection for Telegram Images")
    parser.add_argument('--input', default='data/raw/telegram_messages', 
                       help='Input directory with Telegram data')
    parser.add_argument('--output', default='data/processed/yolo_results.csv',
                       help='Output CSV file path')
    parser.add_argument('--model', default='yolov8n.pt',
                       help='YOLO model to use')
    parser.add_argument('--limit', type=int, default=0,
                       help='Limit number of images to process (0 for all)')
    
    args = parser.parse_args()
    
    print("STARTING YOLO OBJECT DETECTION PIPELINE")
    print("="*60)
    
    # Initialize pipeline
    pipeline = YOLODetectionPipeline(model_name=args.model)
    
    # Setup YOLO
    if not pipeline.setup():
        sys.exit(1)
    
    # Find images
    images_info = pipeline.find_telegram_images(args.input)
    
    if not images_info:
        print("ERROR: No images found.")
        sys.exit(1)
    
    # Process images
    results = pipeline.process_images(images_info, args.limit)
    
    # Save results
    pipeline.save_results(args.output)
    
    print("\nPIPELINE COMPLETED")

if __name__ == "__main__":
    main()
    