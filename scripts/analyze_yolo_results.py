# Create file: scripts/analyze_yolo_results.py
import pandas as pd
import matplotlib.pyplot as plt

# Simulate your results (you have actual data)
results = {
    'image_category': ['product_display', 'product_display', 'product_display', 
                      'lifestyle', 'lifestyle', 'lifestyle'] + ['other']*14,
    'confidence': [0.42, 0.41, 0.43, 0.38, 0.37, 0.39] + [0.26]*14,
    'channel': ['tikvahpharma', 'tikvahpharma', 'lobelia4cosmetics',
               'lobelia4cosmetics', 'lobelia4cosmetics', 'tikvahpharma'] + ['mixed']*14
}

df = pd.DataFrame(results)

# Generate insights
insights = {
    "total_images_analyzed": 20,
    "category_distribution": df['image_category'].value_counts().to_dict(),
    "average_confidence_by_category": df.groupby('image_category')['confidence'].mean().to_dict(),
    "best_detection": df.loc[df['confidence'].idxmax()].to_dict(),
    "channel_analysis": df[df['channel'] != 'mixed']['channel'].value_counts().to_dict()
}

print("YOLO Analysis Insights:")
for key, value in insights.items():
    print(f"{key}: {value}")

# Save to file
import json
with open('data/yolo_analysis.json', 'w') as f:
    json.dump(insights, f, indent=2)