import os
import sys
from enum import Enum
import re

import mistralai
import dotenv
import rich

# Actual imports
from mistralai import Mistral
from dotenv import load_dotenv
from typing import List, Dict, Any
import time
import random
import json
from bot.prompts import DealPrompts
import asyncio
from functools import partial
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn, BarColumn
from rich.panel import Panel
from rich.text import Text
from bot.handlers.progress_handler import ProgressHandler

# Logging configuration
import logging
logger = logging.getLogger(__name__)

# Load environment variables
try:
    load_dotenv()
except Exception as e:
    logger.error(f"Error loading .env file: {e}")

# Initialize console
console = Console()

class ProgressStages(Enum):
    INIT = "Initializing"
    COMPLETE = "Complete"

class FieldValidator:
    LANGUAGE_MAPPING = {
        # English
        'en': 'English',
        'eng': 'English',
        'english': 'English',
        
        # French
        'fr': 'French',
        'fre': 'French',
        'french': 'French',
        
        # Italian
        'it': 'Italian',
        'ita': 'Italian',
        'italian': 'Italian',
        
        # Spanish
        'es': 'Spanish',
        'esp': 'Spanish',
        'spanish': 'Spanish',
        
        # German
        'de': 'German',
        'ger': 'German',
        'german': 'German',
        
        # Dutch
        'nl': 'Dutch',
        'dut': 'Dutch',
        'dutch': 'Dutch',
        
        # Portuguese
        'pt': 'Portuguese',
        'por': 'Portuguese',
        'portuguese': 'Portuguese',
        
        # Russian
        'ru': 'Russian',
        'rus': 'Russian',
        'russian': 'Russian',
        
        # Swedish
        'se': 'Swedish',
        'swe': 'Swedish',
        'swedish': 'Swedish',
        
        # Danish
        'dk': 'Danish',
        'dan': 'Danish',
        'danish': 'Danish',
        
        # Norwegian
        'no': 'Norwegian',
        'nor': 'Norwegian',
        'norwegian': 'Norwegian',
        
        # Finnish
        'fi': 'Finnish',
        'fin': 'Finnish',
        'finnish': 'Finnish',
        
        # Native/Local
        'nat': 'Native',
        'native': 'Native',
        'local': 'Native'
    }
    
    SOURCE_MAPPING = {
        # Facebook
        'fb': 'Facebook',
        'FB': 'Facebook',
        'facebook': 'Facebook',
        
        # Google and variations
        'gg': 'Google',
        'GG': 'Google',
        'google': 'Google',
        'google display': 'Google Display',
        'google seo': 'Google SEO',
        'dv360': 'Google DV360',
        'dv': 'Google DV360',
        'google dv360': 'Google DV360',
        'google dv 360': 'Google DV360',
        'dv 360': 'Google DV360',
        'display': 'Display',
        
        # SEO
        'seo': 'SEO',
        'SEO': 'SEO',
        
        # Taboola
        'taboola': 'Taboola',
        'TABOOLA': 'Taboola',
        
        # Bing
        'bing': 'Bing',
        'BING': 'Bing',
        
        # Others
        'native': 'Native',
        'tiktok': 'TikTok',
        'push': 'Push',
        'email': 'Email'
    }
    
    @classmethod
    def clean_language(cls, language: str) -> str:
        """Normalize language codes to full names"""
        if not language:
            return 'Native'
            
        # Split on comma and clean each language
        languages = []
        if ',' in str(language):
            language_list = [lang.strip() for lang in str(language).split(',')]
        else:
            language_list = [str(language)]
            
        for lang in language_list:
            lang = lang.lower()
            cleaned = cls.LANGUAGE_MAPPING.get(lang, lang.capitalize())
            languages.append(cleaned)
            
        return ','.join(languages)
    
    @classmethod
    def clean_source(cls, source: str) -> str:
        """Normalize source names"""
        if not source:
            return source
            
        # Handle multiple sources
        if '|' in source or '+' in source:
            sources = re.split(r'[|+]', source)
            cleaned = [cls.SOURCE_MAPPING.get(s.strip(), s.strip()) for s in sources]
            return '|'.join(filter(None, cleaned))
            
        return cls.SOURCE_MAPPING.get(source.strip(), source.strip())
    
    @classmethod
    def clean_geo(cls, geo: str) -> str:
        """Clean geo codes - extract only country code"""
        if not geo:
            return geo
            
        # Remove emojis and flags
        geo = re.sub(r'[\U0001F1E6-\U0001F1FF]', '', str(geo))
        
        # Extract country code from remaining text
        matches = re.findall(r'([A-Za-z]{2})', geo)
        if matches:
            return matches[0].upper()
        
        return geo.strip().split()[0].upper() if geo.strip() else ''

    @classmethod
    def clean_value(cls, value: Any, field_type: str = 'text') -> Any:
        """Clean and format field values consistently"""
        try:
            if not value:  # Handle None or empty values
                return ''
                
            if field_type in ['cr', 'crg']:
                # Handle percentage values for CR and CRG
                if isinstance(value, (int, float)):
                    float_value = float(value)
                else:
                    # Remove % and handle ranges (take the average)
                    value = str(value).replace('%', '')
                    if '-' in value:
                        # Handle range format (e.g., "10-12")
                        try:
                            low, high = map(float, value.split('-'))
                            float_value = (low + high) / 2
                        except ValueError:
                            # If range parsing fails, try to extract first valid number
                            float_value = float(re.search(r'\d+(?:\.\d+)?', value).group())
                    else:
                        float_value = float(value)
                
                # If value is greater than 1, assume it's a percentage
                result = float_value / 100 if float_value > 1 else float_value
                # Round to 4 decimal places to avoid floating point artifacts
                return round(result, 4)
                
            value = str(value).strip()
            
            if field_type == 'language':
                return cls.clean_language(value)
            elif field_type == 'sources':
                # Clean sources: remove spaces around separators, convert separators to commas
                value = (value.replace(' + ', ',')
                             .replace('+', ',')
                             .replace('|', ',')
                             .replace(' ,', ',')
                             .replace(', ', ',')
                             .strip())
                return ','.join(filter(None, value.split(',')))
            elif field_type == 'list':
                # Clean lists: remove brackets, quotes, extra spaces
                value = (value.replace('[', '')
                             .replace(']', '')
                             .replace("'", '')
                             .replace('"', '')
                             .strip())
                return ','.join(filter(None, [item.strip() for item in value.split(',')]))
            elif field_type == 'geo':
                return cls.clean_geo(value)
            else:  # Default text cleaning
                return value.strip()
                
        except Exception as e:
            logger.warning(f"Error cleaning field ({field_type}): {str(e)}")
            return value  # Return original value if cleaning fails

class DealParser:
    def __init__(self, message=None):
        self._validate_api_key()
        self.client = Mistral(api_key=os.getenv("MISTRAL_API_KEY"))
        self.message = message
        self.progress = None  # Will hold ProgressHandler instance
        self.max_retries = 3
        self.base_delay = 1.0
        self.model = "mistral-large-latest"

    def _validate_api_key(self):
        """Validate API key exists"""
        try:
            if not os.getenv("MISTRAL_API_KEY"):
                logger.error("MISTRAL_API_KEY environment variable not set")
                raise ValueError(
                    "MISTRAL_API_KEY environment variable not set. "
                    "Please set it in your .env file."
                )
        except Exception as e:
            logger.error(f"API key validation error: {str(e)}")
            raise

    async def parse_deals(self, text: str) -> List[Dict]:
        try:
            start_time = time.time()
            
            # Initialize progress handler if we have a message
            if self.message:
                self.progress = ProgressHandler(self.message)
            
            # Use progress handler for updates
            if self.progress:
                await self.progress.update_progress("init", {
                    "message": "🔄 Starting Deal Parser Bot..."
                })
            
            # Structure analysis
            if self.progress:
                await self.progress.update_progress("structure_start", {
                    "message": "📊 Analyzing Deal Structure..."
                })
            
            structure = await self._analyze_structure(text)
            
            if self.progress:
                await self.progress.update_progress("structure_complete", {
                    "message": "✅ Structure Analysis Complete"
                })
            
            total_deals = self.get_total_deals(structure)
            results = []
            
            # Process deals
            for section_idx, section in enumerate(structure.get("sections", [])):
                shared_fields = section.get("shared_fields", {})
                
                for deal_idx, deal_block in enumerate(section.get("deal_blocks", [])):
                    if self.progress:
                        await self.progress.update_progress("progress", {
                            "current": deal_idx + 1,
                            "total": total_deals,
                            "message": f"🔄 Processing deal {deal_idx + 1} of {total_deals}"
                        })
                    
                    context = {
                        "shared_fields": shared_fields,
                        "deal_text": deal_block["text"]
                    }
                    
                    parsed_deal = await self._parse_deal(deal_block["text"], context)
                    results.append(parsed_deal)
            
            # Complete
            elapsed_time = time.time() - start_time
            if self.progress:
                await self.progress.update_progress("complete", {
                    "elapsed_time": elapsed_time,
                    "total_deals": total_deals,
                    "message": "✨ Processing Complete!"
                })
            
            return results
            
        except Exception as e:
            if self.progress:
                await self.progress.update_progress("error", {
                    "message": f"❌ Error: {str(e)}"
                })
            raise

    async def _show_completion_message(self, start_time: float, total_deals: int):
        """Show completion message without blocking main process"""
        await asyncio.sleep(0.2)  # Small delay for visual purposes only
        total_time = time.time() - start_time
        self.console.print()
        self.console.print(Panel(
            Text.assemble(
                ("✨ Deal Processing Complete!\n\n", "bold green"),
                (f"Total Time: {total_time:.2f} seconds\n", "blue"),
                (f"Deals Processed: {total_deals}", "blue")
            ),
            title="Summary",
            border_style="green"
        ))

    async def _analyze_structure(self, text: str) -> Dict:
        """Analyze text structure and identify shared fields and deal blocks"""
        try:
            response = await self._call_mistral(
                DealPrompts.create_structure_prompt(text)
            )
            structure = json.loads(response)
            
            # Validate structure format
            if "sections" not in structure:
                structure = {
                    "sections": [{
                        "shared_fields": {},
                        "deal_blocks": []
                    }]
                }
                
            return structure
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse structure response: {e}")
            raise

    async def _parse_deal(self, deal_text: str, context: Dict) -> Dict:
        """Parse individual deal with shared context"""
        try:
            response = await self._call_mistral(
                DealPrompts.create_parsing_prompt(deal_text, context)
            )
            parsed = json.loads(response)
            
            if "parsed_data" in parsed:
                data = parsed["parsed_data"]
                
                # Clean fields using FieldValidator
                data["language"] = FieldValidator.clean_language(data.get("language"))
                data["source"] = FieldValidator.clean_source(data.get("source"))
                data["geo"] = FieldValidator.clean_geo(data.get("geo"))
                data["cr"] = FieldValidator.clean_value(data.get("cr"), "cr")
                data["crg"] = FieldValidator.clean_value(data.get("crg"), "crg")
                
                # Determine pricing model based on values
                if data.get("crg"):
                    data["pricing_model"] = "CPA/CRG"
                elif data.get("cpa"):
                    data["pricing_model"] = "CPA"
                elif data.get("cpl"):
                    data["pricing_model"] = "CPL"
                
                # Ensure funnels is always a list
                if not isinstance(data.get("funnels"), list):
                    data["funnels"] = []
                    
            return parsed
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse deal response: {e}")
            return self._create_error_response(str(e))

    def _create_error_response(self, error_message: str) -> Dict:
        """Create standardized error response"""
        return {
            "raw_text": "",
            "parsed_data": {
                "partner": "&",
                "region": "TIER3",
                "geo": "&",
                "language": "Native",
                "source": "&",
                "pricing_model": "CPA",
                "cpa": None,
                "crg": None,
                "cpl": None,
                "funnels": [],
                "cr": None,
                "deduction_limit": None
            },
            "metadata": {
                "error": error_message
            }
        }

    async def _call_mistral(self, messages: List[Dict]) -> str:
        """Make API call to Mistral with proper async handling"""
        for attempt in range(self.max_retries):
            try:
                response = await self.client.chat.complete_async(
                    model=self.model,
                    messages=messages,
                    temperature=0.0,
                    response_format={"type": "json_object"}
                )
                
                # Log response for debugging
                content = response.choices[0].message.content
                logger.debug(f"Mistral response: {content}")
                return content
                
            except Exception as e:
                if "429" in str(e) and attempt < self.max_retries - 1:
                    delay = (self.base_delay * (2 ** attempt) + 
                            random.uniform(0, 0.1 * (2 ** attempt)))
                    logger.warning(f"Rate limit hit. Retrying in {delay:.2f} seconds...")
                    await asyncio.sleep(delay)
                    continue
                logger.error(f"Error calling Mistral API: {str(e)}")
                if attempt == self.max_retries - 1:
                    raise
                continue

    def get_total_deals(self, structure):
        try:
            return sum(len(section["deal_blocks"]) for section in structure["sections"])
        except (KeyError, TypeError) as e:
            logger.error(f"Error parsing structure: {e}")
            return 0

    def _extract_funnels(self, text: str) -> List[str]:
        """Extract funnels from text"""
        funnels = []
        funnel_indicators = ['funnels:', 'landing page:', 'funnel:']
        
        for line in text.lower().split('\n'):
            for indicator in funnel_indicators:
                if indicator in line:
                    # Extract everything after the indicator
                    funnel_text = line.split(indicator)[1].strip()
                    # Split on common separators and clean
                    funnels.extend([f.strip() for f in re.split(r'[,|/]', funnel_text) if f.strip()])
                    
        return funnels

if __name__ == "__main__":
    import asyncio
    
    async def main():
        parser = DealParser()
        # Add a sample text or method to test the parser
        sample_text = "Your sample deal text here"
        deals = await parser.parse_deals(sample_text)
        print(deals)

    asyncio.run(main())
