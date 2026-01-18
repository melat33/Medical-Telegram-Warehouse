import csv
import pandas as pd
from pathlib import Path

def write_messages_csv(date_str: str, messages: list, channel_name: str = None) -> Path:
    """
    Save messages to a well-formatted CSV file.
    
    Args:
        date_str: Date string in YYYY-MM-DD format
        messages: List of message dictionaries
        channel_name: Optional channel name for single-channel file
    
    Returns:
        Path to saved CSV file
    """
    # Create directory
    if channel_name:
        csv_dir = Path(f"data/raw/csv/{date_str}")
        csv_file = csv_dir / f"{channel_name}.csv"
    else:
        csv_dir = Path(f"data/raw/csv/{date_str}")
        csv_file = csv_dir / "telegram_data.csv"
    
    csv_dir.mkdir(parents=True, exist_ok=True)
    
    if not messages:
        # Create empty file with headers
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["message_id", "channel_name", "message_date", 
                           "message_text", "has_media", "image_path", 
                           "views", "forwards", "channel_title", "extracted_at"])
        return csv_file
    
    # Define the order of columns for better readability
    columns = [
        "message_id",
        "channel_name", 
        "channel_title",
        "message_date",
        "message_text",
        "views",
        "forwards", 
        "has_media",
        "image_path",
        "extracted_at"
    ]
    
    # Method 1: Using pandas (recommended for better formatting)
    try:
        df = pd.DataFrame(messages)
        
        # Reorder columns
        available_columns = [col for col in columns if col in df.columns]
        df = df[available_columns]
        
        # Clean text (remove newlines in CSV)
        if 'message_text' in df.columns:
            df['message_text'] = df['message_text'].str.replace('\n', ' ', regex=False)
            df['message_text'] = df['message_text'].str.replace('\r', ' ', regex=False)
        
        # Save with proper formatting
        df.to_csv(csv_file, index=False, encoding='utf-8')
        
        print(f"✓ CSV saved with {len(df)} rows: {csv_file}")
        
    except ImportError:
        # Fallback to standard CSV if pandas not available
        print("⚠ Pandas not available, using standard CSV writer")
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            for message in messages:
                # Clean text for CSV
                if 'message_text' in message and message['message_text']:
                    message['message_text'] = message['message_text'].replace('\n', ' ').replace('\r', ' ')
                writer.writerow(message)
    
    return csv_file


def format_csv_for_readability(csv_file: Path):
    """
    Format CSV file for better readability in spreadsheet apps.
    
    Args:
        csv_file: Path to CSV file
    """
    try:
        df = pd.read_csv(csv_file)
        
        # Format specific columns
        if 'message_date' in df.columns:
            # Convert to datetime format
            df['message_date'] = pd.to_datetime(df['message_date'])
        
        if 'views' in df.columns:
            # Format numbers with commas
            df['views'] = df['views'].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else "")
        
        if 'forwards' in df.columns:
            df['forwards'] = df['forwards'].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else "")
        
        # Save formatted version
        formatted_file = csv_file.parent / f"formatted_{csv_file.name}"
        df.to_csv(formatted_file, index=False, encoding='utf-8')
        
        print(f"✓ Formatted CSV saved: {formatted_file}")
        return formatted_file
        
    except Exception as e:
        print(f"⚠ Could not format CSV: {e}")
        return csv_file