"""
ADVANCED IMAGE CLASSIFICATION RULES FOR MEDICAL/COSMETIC CONTENT
Detects business-relevant patterns from YOLO results
"""

import json
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum

class ImageCategory(Enum):
    """Business-relevant image categories"""
    PROMOTIONAL = "promotional"           # Person showing product
    PRODUCT_DISPLAY = "product_display"   # Product only, clean
    LIFESTYLE = "lifestyle"              # Person in context
    INGREDIENT_SHOWCASE = "ingredient_showcase"  # Ingredients/food
    MEDICAL_CONTEXT = "medical_context"   # Medical setting
    BEFORE_AFTER = "before_after"        # Comparison shots
    TEXT_HEAVY = "text_heavy"            # Text/price lists
    UNKNOWN = "unknown"                  # Can't classify

@dataclass
class DetectionResult:
    """Single object detection result"""
    class_name: str
    confidence: float
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'DetectionResult':
        """Create from dictionary"""
        return cls(
            class_name=data.get('class', ''),
            confidence=data.get('confidence', 0.0)
        )

@dataclass
class ImageAnalysis:
    """Complete image analysis"""
    image_path: str
    message_id: int
    detections: List[DetectionResult]
    category: str
    confidence_score: float
    business_tags: List[str]
    business_insights: Dict[str, Any]
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'image_path': self.image_path,
            'message_id': self.message_id,
            'detection_count': len(self.detections),
            'category': self.category,
            'confidence_score': self.confidence_score,
            'business_tags': ','.join(self.business_tags),
            'business_insights': json.dumps(self.business_insights),
            'metadata': json.dumps(self.metadata),
            'top_objects': ','.join([d.class_name for d in self.detections[:3]]) if self.detections else ''
        }

class MedicalImageClassifier:
    """Advanced classifier for medical/cosmetic images"""
    
    # RELEVANT YOLO CLASSES FOR OUR DOMAIN
    MEDICAL_RELEVANT_CLASSES = {
        # People
        'person': ['person', 'human'],
        
        # Products/Containers
        'bottle': ['product', 'container', 'bottle'],
        'cup': ['product', 'container', 'cup'], 
        'wine glass': ['product', 'container', 'glass'],
        'handbag': ['cosmetic', 'accessory', 'bag'],
        'suitcase': ['storage', 'travel'],
        
        # Medical/Pharmacy
        'book': ['documentation', 'manual'],
        'cell phone': ['communication', 'tech'],
        'remote': ['device', 'electronic'],
        
        # Setting/Context
        'chair': ['setting', 'office'],
        'dining table': ['setting', 'home'],
        'bed': ['setting', 'medical'],
        'toilet': ['setting', 'bathroom'],
        'tv': ['setting', 'home'],
        
        # Ingredients/Food
        'banana': ['ingredient', 'food', 'fruit'],
        'apple': ['ingredient', 'food', 'fruit'],
        'orange': ['ingredient', 'food', 'fruit'],
        'broccoli': ['ingredient', 'food', 'vegetable'],
        'carrot': ['ingredient', 'food', 'vegetable'],
        
        # Animals (for testing/safety)
        'dog': ['animal', 'pet'],
        'cat': ['animal', 'pet'],
        'bird': ['animal', 'wildlife'],
    }
    
    def __init__(self, confidence_threshold: float = 0.3):
        self.confidence_threshold = confidence_threshold
    
    def analyze_detections(self, detections_data: List[Dict], image_path: str = "", message_id: int = 0) -> ImageAnalysis:
        """Analyze detections and create comprehensive analysis"""
        
        # Convert to DetectionResult objects
        detections = [
            DetectionResult.from_dict(d) 
            for d in detections_data 
            if d.get('confidence', 0) >= self.confidence_threshold
            and d.get('class', '') in self.MEDICAL_RELEVANT_CLASSES
        ]
        
        if not detections:
            return self._create_unknown_analysis(detections, image_path, message_id)
        
        # Analyze detections
        category, category_confidence = self._categorize_image(detections)
        business_tags = self._extract_business_tags(detections)
        overall_confidence = self._calculate_overall_confidence(detections)
        business_insights = self._generate_business_insights(detections, category)
        
        return ImageAnalysis(
            image_path=image_path,
            message_id=message_id,
            detections=detections,
            category=category.value,
            confidence_score=overall_confidence,
            business_tags=business_tags,
            business_insights=business_insights,
            metadata={
                'detection_count': len(detections),
                'top_classes': [d.class_name for d in detections[:3]],
                'avg_confidence': sum(d.confidence for d in detections) / len(detections) if detections else 0,
                'category_confidence': category_confidence
            }
        )
    
    def _categorize_image(self, detections: List[DetectionResult]) -> Tuple[ImageCategory, float]:
        """Categorize image based on detections"""
        
        # Count detections by class
        class_counts = {}
        for det in detections:
            class_counts[det.class_name] = class_counts.get(det.class_name, 0) + 1
        
        has_person = class_counts.get('person', 0) > 0
        has_product = any(c in ['bottle', 'cup', 'handbag', 'wine glass'] for c in class_counts)
        has_food = any(c in ['banana', 'apple', 'orange', 'broccoli', 'carrot'] for c in class_counts)
        has_medical_setting = any(c in ['bed', 'chair'] for c in class_counts) and has_person
        
        # Apply categorization rules
        if has_person and has_product:
            return ImageCategory.PROMOTIONAL, 0.8
        elif has_product and not has_person:
            return ImageCategory.PRODUCT_DISPLAY, 0.7
        elif has_person and not has_product and not has_medical_setting:
            return ImageCategory.LIFESTYLE, 0.6
        elif has_food:
            return ImageCategory.INGREDIENT_SHOWCASE, 0.7
        elif has_medical_setting:
            return ImageCategory.MEDICAL_CONTEXT, 0.6
        elif class_counts.get('person', 0) >= 2:
            # Multiple people might indicate before/after
            return ImageCategory.BEFORE_AFTER, 0.5
        else:
            return ImageCategory.UNKNOWN, 0.3
    
    def _extract_business_tags(self, detections: List[DetectionResult]) -> List[str]:
        """Extract business-relevant tags from detections"""
        tags = set()
        
        for det in detections:
            if det.class_name in self.MEDICAL_RELEVANT_CLASSES:
                class_tags = self.MEDICAL_RELEVANT_CLASSES[det.class_name]
                tags.update(class_tags)
        
        # Add confidence-based tags
        if detections:
            avg_confidence = sum(d.confidence for d in detections) / len(detections)
            if avg_confidence > 0.8:
                tags.add('high_confidence')
            elif avg_confidence > 0.5:
                tags.add('medium_confidence')
            else:
                tags.add('low_confidence')
        
        return list(tags)
    
    def _calculate_overall_confidence(self, detections: List[DetectionResult]) -> float:
        """Calculate overall confidence score"""
        if not detections:
            return 0.0
        
        confidences = [d.confidence for d in detections]
        base_confidence = sum(confidences) / len(confidences)
        
        # Bonus for multiple high-confidence detections
        high_conf_count = sum(1 for c in confidences if c > 0.7)
        bonus = min(high_conf_count * 0.05, 0.2)
        
        return round(min(base_confidence + bonus, 1.0), 3)
    
    def _generate_business_insights(self, detections: List[DetectionResult], category: ImageCategory) -> Dict:
        """Generate business insights from image analysis"""
        
        insights = {
            'category': category.value,
            'detection_summary': {
                'total_objects': len(detections),
                'top_objects': [
                    {'class': d.class_name, 'confidence': d.confidence}
                    for d in sorted(detections, key=lambda x: x.confidence, reverse=True)[:3]
                ]
            },
            'business_implications': []
        }
        
        # Add business implications based on category
        if category == ImageCategory.PROMOTIONAL:
            insights['business_implications'].extend([
                "Good for brand storytelling and marketing",
                "Shows product in use context",
                "Effective for social media engagement"
            ])
        elif category == ImageCategory.PRODUCT_DISPLAY:
            insights['business_implications'].extend([
                "Pure product showcase - good for catalogs",
                "Focus on product features and details",
                "Useful for technical documentation"
            ])
        elif category == ImageCategory.LIFESTYLE:
            insights['business_implications'].extend([
                "Builds emotional brand connection",
                "Focus on user experience and benefits",
                "Good for long-term brand building"
            ])
        elif category == ImageCategory.INGREDIENT_SHOWCASE:
            insights['business_implications'].extend([
                "Highlights natural/organic ingredients",
                "Supports health and wellness claims",
                "Appeals to health-conscious consumers"
            ])
        elif category == ImageCategory.MEDICAL_CONTEXT:
            insights['business_implications'].extend([
                "Establishes professional/medical authority",
                "Builds trust through clinical context",
                "Appeals to health professionals"
            ])
        
        return insights
    
    def _create_unknown_analysis(self, detections: List[DetectionResult], image_path: str, message_id: int) -> ImageAnalysis:
        """Create analysis for unknown/empty detections"""
        return ImageAnalysis(
            image_path=image_path,
            message_id=message_id,
            detections=detections,
            category=ImageCategory.UNKNOWN.value,
            confidence_score=0.0,
            business_tags=['no_detections', 'low_confidence'],
            business_insights={
                'category': 'unknown',
                'business_implications': ['No relevant objects detected'],
                'detection_summary': {'total_objects': 0, 'top_objects': []}
            },
            metadata={'detection_count': 0}
        )

# Utility functions
def load_and_classify_results(csv_path: str, output_path: str = None) -> pd.DataFrame:
    """Load YOLO results CSV and apply advanced classification"""
    import pandas as pd
    
    df = pd.read_csv(csv_path)
    classifier = MedicalImageClassifier()
    
    enhanced_results = []
    
    for _, row in df.iterrows():
        # Parse detections if they exist
        detections = []
        if 'detections' in row and pd.notna(row['detections']):
            try:
                detections_data = json.loads(row['detections'])
                detections = [DetectionResult.from_dict(d) for d in detections_data]
            except:
                detections = []
        
        # Analyze
        analysis = classifier.analyze_detections(
            [asdict(d) for d in detections],
            row.get('image_path', ''),
            row.get('message_id', 0)
        )
        
        # Convert to dict and merge with original row
        result_dict = analysis.to_dict()
        result_dict.update({k: v for k, v in row.items() if k not in result_dict})
        enhanced_results.append(result_dict)
    
    enhanced_df = pd.DataFrame(enhanced_results)
    
    if output_path:
        enhanced_df.to_csv(output_path, index=False)
        print(f"Enhanced results saved to: {output_path}")
    
    return enhanced_df