"""
Configuration and environment management for Telegram Scraper.
"""

import os
import sys
from pathlib import Path
from typing import Optional, List
from dataclasses import dataclass, field
from dotenv import load_dotenv

# FIX: Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@dataclass
class ScraperConfig:
    """Centralized configuration for the Telegram scraper."""
    
    # FIXED: Fields WITHOUT defaults FIRST
    project_root: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    base_data_path: str = field(default_factory=lambda: "data")
    
    # Fields WITH defaults AFTER non-default fields
    # API Credentials (your provided values)
    api_id: int = 34878880
    api_hash: str = "0a883f04d3dc7f6f79e10c33190bdf9b"
    
    # Rate limiting
    channel_delay: float = 3.0
    message_delay: float = 1.0
    
    # Limits
    default_message_limit: int = 150
    max_retries: int = 3
    
    # Session file
    session_file: str = "telegram_scraper.session"
    
    # REQUIRED channels from the project specification
    default_channels: List[str] = field(default_factory=lambda: [
        # Primary channels from the project spec
        "@cheMed123",           # CheMed Telegram Channel - Medical products
        "@lobelia4cosmetics",   # Lobelia Cosmetics - Cosmetics and health products
        "@tikvahpharma",        # Tikvah Pharma - Pharmaceuticals
        
        # Additional channels from et.tgstat.com/medicine
        "@tenamereja",          # Additional medical channel
        "@ethio_pharmacy",      # Ethiopian Pharmacy
        "@meds_ethiopia",       # Meds Ethiopia
        "@healthcare_et",       # Healthcare Ethiopia
        "@pharma_eth"           # Pharma Ethiopia
    ])
    
    @classmethod
    def from_env(cls, base_path: str = "data") -> 'ScraperConfig':
        """Load configuration from environment variables."""
        load_dotenv()
        
        # Get project root
        project_root = Path(__file__).resolve().parent.parent
        
        # Channel list as specified in the project requirements
        channels = [
            # Primary channels from the project spec
            "@cheMed123",           # CheMed Telegram Channel - Medical products
            "@lobelia4cosmetics",   # Lobelia Cosmetics - Cosmetics and health products
            "@tikvahpharma",        # Tikvah Pharma - Pharmaceuticals
            
            # Additional channels from et.tgstat.com/medicine
            "@tenamereja",          # Additional medical channel
            "@ethio_pharmacy",      # Ethiopian Pharmacy
            "@meds_ethiopia",       # Meds Ethiopia
            "@healthcare_et",       # Healthcare Ethiopia
            "@pharma_eth"           # Pharma Ethiopia
        ]
        
        # Allow environment variable override for testing
        env_channels = os.getenv("TELEGRAM_CHANNELS")
        if env_channels:
            channels = [ch.strip() for ch in env_channels.split(",")]
        
        return cls(
            project_root=project_root,
            base_data_path=base_path,
            api_id=cls.api_id,  # Using the hardcoded value from project spec
            api_hash=cls.api_hash,  # Using the hardcoded value from project spec
            session_file="telegram_scraper.session",
            channel_delay=3.0,
            message_delay=1.0,
            default_message_limit=100,
            max_retries=3,
            default_channels=channels
        )
    
    def validate(self) -> bool:
        """Validate all configuration parameters."""
        if not self.api_id or not self.api_hash:
            print("ERROR: Telegram API credentials are required")
            return False
        
        if not self.default_channels:
            print("WARNING: No channels configured for scraping")
        
        if self.channel_delay < 0 or self.message_delay < 0:
            print("ERROR: Delays cannot be negative")
            return False
        
        if self.max_retries <= 0:
            print("ERROR: Max retries must be positive")
            return False
        
        return True
    
    def get_channel_info(self) -> List[dict]:
        """Get channel information with categorization."""
        channel_info = []
        
        # Categorize channels based on names/content
        for channel in self.default_channels:
            channel_name = channel.strip('@')
            category = self._categorize_channel(channel_name)
            channel_info.append({
                "username": channel,
                "name": channel_name,
                "category": category,
                "url": f"https://t.me/{channel_name}"
            })
        
        return channel_info
    
    def _categorize_channel(self, channel_name: str) -> str:
        """Categorize channel based on name/content."""
        channel_lower = channel_name.lower()
        
        if "chemed" in channel_lower:
            return "medical_products"
        elif "lobelia" in channel_lower:
            return "cosmetics"
        elif "tikvah" in channel_lower:
            return "pharmaceuticals"
        elif "pharm" in channel_lower:
            return "pharmaceuticals"
        elif "med" in channel_lower:
            return "medical_products"
        elif "health" in channel_lower:
            return "healthcare"
        else:
            return "general"


def setup_project_path() -> None:
    """Add project root to Python path for module imports."""
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))