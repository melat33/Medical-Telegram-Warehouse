"""
Telegram Scraper for Ethiopian Medical Channels - BOT VERSION
===============================================================
Uses bot token for authentication (no phone required).
"""

import asyncio
import argparse
import json
import csv
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import time

# Fix Windows encoding issues
if sys.platform == 'win32':
    # Set console to UTF-8
    os.system('chcp 65001 > nul')
    # Use ASCII-safe symbols
    CHECK = "[OK]"
    CROSS = "[FAIL]"
    WARN = "[WARN]"
    ARROW = "->"
else:
    CHECK = "✓"
    CROSS = "✗"
    WARN = "⚠"
    ARROW = "→"

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from telethon import TelegramClient
    from telethon.errors import FloodWaitError, ChannelPrivateError, ChannelInvalidError
    from telethon.tl.types import MessageMediaPhoto, Channel, InputPeerChannel
except ImportError:
    print(f"{CROSS} ERROR: Telethon library not installed. Run: pip install telethon")
    sys.exit(1)


class SimpleDataLake:
    """Simple data storage manager."""
    
    def __init__(self, base_path: str = "data"):
        self.base_path = Path(base_path)
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create required directories."""
        directories = [
            self.base_path / "raw" / "telegram_messages",
            self.base_path / "raw" / "images",
            self.base_path / "raw" / "csv",
            self.base_path / "logs",
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    def get_today_path(self, date_str: str) -> Path:
        """Get today's date partition path."""
        return self.base_path / "raw" / "telegram_messages" / date_str
    
    def get_image_dir(self, channel_name: str) -> Path:
        """Get image directory for a channel."""
        path = self.base_path / "raw" / "images" / channel_name
        path.mkdir(parents=True, exist_ok=True)
        return path
    
    def write_json(self, date_str: str, channel_name: str, messages: List[Dict]) -> Path:
        """Write messages to JSON file."""
        partition_dir = self.get_today_path(date_str)
        partition_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = partition_dir / f"{channel_name}.json"
        
        data = {
            "metadata": {
                "channel": channel_name,
                "date": date_str,
                "message_count": len(messages),
                "generated_at": datetime.now().isoformat()
            },
            "messages": messages
        }
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"  JSON saved: {file_path}")
        return file_path
    
    def write_csv(self, date_str: str, channel_name: str, messages: List[Dict]) -> Path:
        """Write messages to CSV file."""
        csv_dir = self.base_path / "raw" / "csv" / date_str
        csv_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = csv_dir / f"{channel_name}.csv"
        
        if not messages:
            return file_path
        
        # Define column order for better readability
        fieldnames = [
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
        
        # Clean message text for CSV (remove newlines)
        cleaned_messages = []
        for msg in messages:
            cleaned_msg = msg.copy()
            if 'message_text' in cleaned_msg and cleaned_msg['message_text']:
                cleaned_msg['message_text'] = cleaned_msg['message_text'].replace('\n', ' ').replace('\r', ' ')
            cleaned_messages.append(cleaned_msg)
        
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cleaned_messages)
        
        print(f"  CSV saved: {file_path}")
        return file_path
    
    def write_combined_csv(self, date_str: str, all_messages: List[Dict]) -> Path:
        """Write all messages to a combined CSV file."""
        csv_dir = self.base_path / "raw" / "csv" / date_str
        csv_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = csv_dir / "telegram_data.csv"
        
        if not all_messages:
            return file_path
        
        # Define column order
        fieldnames = [
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
        
        # Clean message text
        cleaned_messages = []
        for msg in all_messages:
            cleaned_msg = msg.copy()
            if 'message_text' in cleaned_msg and cleaned_msg['message_text']:
                cleaned_msg['message_text'] = cleaned_msg['message_text'].replace('\n', ' ').replace('\r', ' ')
            cleaned_messages.append(cleaned_msg)
        
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cleaned_messages)
        
        return file_path


class TelegramBotScraper:
    """Telegram scraper using bot authentication."""
    
    def __init__(self, api_id: int, api_hash: str, bot_token: str, data_path: str = "data"):
        self.api_id = api_id
        self.api_hash = api_hash
        self.bot_token = bot_token
        self.datalake = SimpleDataLake(data_path)
        self.today = datetime.today().strftime("%Y-%m-%d")
        
        # Create client with bot token
        self.client = TelegramClient(
            session=f"bot_session_{api_id}",
            api_id=api_id,
            api_hash=api_hash
        )
        
        print("=" * 80)
        print("MEDICAL TELEGRAM SCRAPER")
        print("=" * 80)
        print(f"API ID: {api_id}")
        print(f"Data Path: {data_path}")
        print(f"Date: {self.today}")
        print("=" * 80)
    
    async def start_bot(self):
        """Start bot session."""
        try:
            if self.bot_token:
                await self.client.start(bot_token=self.bot_token)
                print(f"{CHECK} Bot authenticated successfully")
            else:
                await self.client.start()
                print(f"{CHECK} User authenticated successfully")
            return True
        except Exception as e:
            print(f"{CROSS} Failed to authenticate: {e}")
            if not self.bot_token:
                print("Note: Without bot token, you need to enter your phone number")
            return False
    
    async def scrape_channel(self, channel: str, limit: int = 150) -> tuple:
        """Scrape a single channel with limit 150."""
        channel_name = channel.strip('@')
        print(f"\nScraping: {channel}")
        
        try:
            # Try different methods to get channel entity
            try:
                entity = await self.client.get_entity(channel)
            except ValueError:
                try:
                    entity = await self.client.get_entity(f"https://t.me/{channel_name}")
                except Exception:
                    print(f"  {CROSS} Could not find channel: {channel}")
                    return [], 0
            
            if not isinstance(entity, (Channel, InputPeerChannel)):
                print(f"  {CROSS} {channel} is not a public channel")
                return [], 0
            
            channel_title = getattr(entity, 'title', 'Unknown')
            print(f"  Channel: {channel_title}")
            
            # Create image directory
            image_dir = self.datalake.get_image_dir(channel_name)
            
            messages = []
            count = 0
            image_count = 0
            
            # Scrape messages with rate limiting
            async for message in self.client.iter_messages(entity, limit=limit):
                try:
                    # Check for image
                    image_path = None
                    has_media = message.media is not None
                    
                    if has_media and isinstance(message.media, MessageMediaPhoto):
                        try:
                            filename = f"{message.id}.jpg"
                            image_path = str(image_dir / filename)
                            await self.client.download_media(message.media, file=image_path)
                            image_count += 1
                        except Exception:
                            image_path = None
                    
                    # Create message dict
                    message_dict = {
                        "message_id": message.id,
                        "channel_name": channel_name,
                        "channel_title": channel_title,
                        "message_date": message.date.isoformat() if message.date else "",
                        "message_text": message.message or "",
                        "has_media": has_media,
                        "image_path": image_path,
                        "views": message.views or 0,
                        "forwards": message.forwards or 0,
                        "extracted_at": datetime.now().isoformat()
                    }
                    
                    messages.append(message_dict)
                    count += 1
                    
                    # Show progress every 30 messages
                    if count % 30 == 0:
                        print(f"    Scraped {count} messages...")
                    
                    # Small delay to avoid rate limiting
                    if count % 50 == 0:
                        await asyncio.sleep(0.5)
                        
                except Exception as e:
                    # print(f"    Error processing message: {e}")
                    continue
            
            print(f"  {CHECK} Successfully scraped {count} messages ({image_count} images)")
            return messages, count
            
        except ChannelPrivateError:
            print(f"  {CROSS} Channel {channel} is private or inaccessible")
            return [], 0
            
        except FloodWaitError as e:
            wait_time = e.seconds
            print(f"  {WARN} Rate limited. Waiting {wait_time} seconds...")
            await asyncio.sleep(wait_time)
            return [], 0
            
        except Exception as e:
            print(f"  {CROSS} Failed to scrape {channel}: {e}")
            return [], 0
    
    async def run(self, channels: List[str], limit: int = 150):
        """Run the scraper with limit 150."""
        print("\n" + "=" * 80)
        print("STARTING SCRAPING SESSION")
        print(f"Channels: {len(channels)}")
        print(f"Limit per channel: {limit}")
        print("=" * 80)
        
        # Start bot
        if not await self.start_bot():
            return
        
        stats = {}
        all_messages = []
        total_images = 0
        
        # Scrape each channel
        for i, channel in enumerate(channels, 1):
            print(f"\n[{i}/{len(channels)}] Processing: {channel}")
            
            messages, count = await self.scrape_channel(channel, limit)
            
            if messages:
                # Count images
                batch_images = sum(1 for msg in messages if msg.get("image_path"))
                total_images += batch_images
                
                # Save data
                channel_name = channel.strip('@')
                self.datalake.write_json(self.today, channel_name, messages)
                self.datalake.write_csv(self.today, channel_name, messages)
                
                all_messages.extend(messages)
            
            stats[channel] = count
            
            # Delay between channels
            if i < len(channels):
                print(f"  Waiting 2 seconds before next channel...")
                await asyncio.sleep(2)
        
        # Save combined data
        if all_messages:
            combined_csv = self.datalake.write_combined_csv(self.today, all_messages)
            print(f"\n{CHECK} Combined CSV saved: {combined_csv}")
            
            # Print summary table
            self._print_summary_table(stats, total_images, all_messages)
        
        # Disconnect
        await self.client.disconnect()
        print(f"\n{CHECK} Scraper shutdown complete")
    
    def _print_summary_table(self, stats: dict, total_images: int, all_messages: List[Dict]):
        """Print a formatted summary table."""
        print("\n" + "=" * 80)
        print("SCRAPING SUMMARY")
        print("=" * 80)
        
        total_messages = sum(stats.values())
        successful_channels = sum(1 for count in stats.values() if count > 0)
        
        # Summary stats
        print(f"Successful Channels: {successful_channels}/{len(stats)}")
        print(f"Total Messages: {total_messages}")
        print(f"Total Images: {total_images}")
        print(f"Data Location: {self.datalake.base_path}/raw/")
        
        # Channel details table
        print("\n" + "-" * 50)
        print(f"{'Channel':<25} {'Messages':<10} {'Status':<10}")
        print("-" * 50)
        
        for channel, count in sorted(stats.items(), key=lambda x: x[1], reverse=True):
            status = CHECK if count > 0 else CROSS
            print(f"{channel:<25} {count:<10} {status:<10}")
        
        # Message statistics
        if all_messages:
            print("\n" + "-" * 50)
            print("MESSAGE STATISTICS:")
            print("-" * 50)
            
            # Count messages with media
            messages_with_media = sum(1 for msg in all_messages if msg.get('has_media'))
            messages_with_images = sum(1 for msg in all_messages if msg.get('image_path'))
            
            print(f"Messages with media: {messages_with_media} ({messages_with_media/total_messages*100:.1f}%)")
            print(f"Messages with images: {messages_with_images} ({messages_with_images/total_messages*100:.1f}%)")
            
            # Average views and forwards
            if total_messages > 0:
                avg_views = sum(msg.get('views', 0) for msg in all_messages) / total_messages
                avg_forwards = sum(msg.get('forwards', 0) for msg in all_messages) / total_messages
                print(f"Average views: {avg_views:.1f}")
                print(f"Average forwards: {avg_forwards:.1f}")
        
        print("=" * 80)


def get_config():
    """Get configuration from environment or defaults."""
    from dotenv import load_dotenv
    load_dotenv()
    
    # API credentials
    api_id = int(os.getenv("Tg_API_ID", "34878880"))
    api_hash = os.getenv("Tg_API_HASH", "0a883f04d3dc7f6f79e10c33190bdf9b")
    bot_token = os.getenv("Tg_BOT_TOKEN", "")
    
    # If no bot token, warn user
    if not bot_token:
        print(f"{WARN} No bot token found. Will use regular authentication.")
        print(f"{WARN} Create a bot token by talking to @BotFather on Telegram")
        print(f"{WARN} Add Tg_BOT_TOKEN=your_bot_token to your .env file")
    
    # REQUIRED CHANNELS FROM PROJECT SPECIFICATION
    channels = [
        "@cheMed123",           # CheMed Telegram Channel - Medical products
        "@lobelia4cosmetics",   # Lobelia Cosmetics - Cosmetics and health products
        "@tikvahpharma",        # Tikvah Pharma - Pharmaceuticals
        
        # Additional channels (optional)
        "@tenamereja",          # Medical Information
        "@ethio_pharmacy",      # Ethiopian Pharmacy
    ]
    
    # Get channels from environment if specified
    env_channels = os.getenv("TELEGRAM_CHANNELS")
    if env_channels:
        channels = [ch.strip() for ch in env_channels.split(",")]
    
    return api_id, api_hash, bot_token, channels


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Telegram Scraper for Ethiopian Medical Channels",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  python {sys.argv[0]}                     # Default (limit 150)
  python {sys.argv[0]} --test              # Test mode (5 messages)
  python {sys.argv[0]} --limit 300         # Scrape 300 messages per channel
  python {sys.argv[0]} --path my_data      # Save to custom directory

REQUIRED CHANNELS:
  1. @cheMed123           - Medical products
  2. @lobelia4cosmetics   - Cosmetics and health products  
  3. @tikvahpharma        - Pharmaceuticals
        """
    )
    
    parser.add_argument('--limit', type=int, default=150, help='Messages per channel (default: 150)')
    parser.add_argument('--test', action='store_true', help='Test mode (5 messages)')
    parser.add_argument('--path', type=str, default='data', help='Data directory')
    parser.add_argument('--channels', nargs='+', help='Custom channels (overrides defaults)')
    parser.add_argument('--no-delay', action='store_true', help='Disable rate limiting delays')
    
    args = parser.parse_args()
    
    # Test mode
    if args.test:
        args.limit = 5
        print(f"{WARN} TEST MODE: Scraping only 5 messages per channel")
    
    # Get configuration
    api_id, api_hash, bot_token, default_channels = get_config()
    
    # Use custom channels if provided
    channels = args.channels if args.channels else default_channels
    
    # Filter out invalid channels
    valid_channels = []
    for channel in channels:
        if channel.startswith('@'):
            valid_channels.append(channel)
        else:
            print(f"{WARN} Channel '{channel}' should start with @, adding @")
            valid_channels.append(f"@{channel}")
    
    if not valid_channels:
        print(f"{CROSS} ERROR: No valid channels specified")
        return
    
    # Print banner
    print("=" * 80)
    print("MEDICAL TELEGRAM DATA PIPELINE - SCRAPER")
    print("=" * 80)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Target Channels: {len(valid_channels)}")
    print(f"Messages per channel: {args.limit}")
    print("=" * 80)
    
    # Show channels
    print("CHANNELS TO SCRAPE:")
    for i, channel in enumerate(valid_channels, 1):
        channel_name = channel.strip('@')
        print(f"  {i}. {channel} - https://t.me/{channel_name}")
    print("=" * 80)
    
    # Create and run scraper
    scraper = TelegramBotScraper(api_id, api_hash, bot_token, args.path)
    await scraper.run(valid_channels, args.limit)


if __name__ == "__main__":
    # Run async main
    asyncio.run(main())