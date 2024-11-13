import os
import logging
from typing import List, Dict, Any
from dataclasses import dataclass
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import asyncio
import dotenv
from datetime import datetime
import time
from notion_service import NotionDealsClient as NotionService
import json

# Load environment variables
dotenv.load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class Deal:
    region: str
    partner: str
    geo: str
    language: str
    source: str
    pricing_model: str
    cpa: float = None
    crg: float = None
    cpl: float = None
    funnels: List[str] = None
    cr: str = None
    deduction_limit: float = None
    
    @property
    def is_valid(self) -> bool:
        """Check if deal has all required fields based on pricing model"""
        # Log validation details for debugging
        logger.debug(f"Validating deal: {self.__dict__}")
        
        # Basic required fields
        if not all([self.region, self.partner, self.geo, self.language]):
            logger.debug("Missing basic required fields")
            return False
            
        if not self.source or self.source == "&":
            logger.debug("Missing or invalid source")
            return False
            
        # Make funnels validation more flexible
        if self.funnels:
            if isinstance(self.funnels, list) and (not self.funnels or self.funnels == ["&"]):
                logger.debug("Empty or invalid funnels list")
                return False
            elif isinstance(self.funnels, str) and (not self.funnels or self.funnels == "&"):
                logger.debug("Empty or invalid funnels string")
                return False
        else:
            logger.debug("Missing funnels")
            return False
            
        # Validate based on pricing model
        if self.pricing_model == "cpa_crg":
            valid = bool(self.cpa and self.crg)
            if not valid:
                logger.debug("Missing CPA or CRG for cpa_crg model")
            return valid
        elif self.pricing_model == "cpa":
            valid = bool(self.cpa)
            if not valid:
                logger.debug("Missing CPA for cpa model")
            return valid
        elif self.pricing_model == "cpl":
            valid = bool(self.cpl)
            if not valid:
                logger.debug("Missing CPL for cpl model")
            return valid
            
        logger.debug("Invalid pricing model")
        return False

class DealBot:
    def __init__(self, debug=False):
        self.debug = debug
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
            
        self._validate_env()
        self.notion_client = NotionService(
            notion_token=os.getenv("NOTION_TOKEN"),
            database_id=os.getenv("OFFERS_DATABASE_ID"),
            kitchen_database_id=os.getenv("ADVERTISERS_DATABASE_ID")
        )
        # Add rate limiting
        self.last_request_time = 0
        self.min_request_interval = 0.5  # seconds
        
    def _validate_env(self):
        """Validate required environment variables exist"""
        # Update to match your .env file
        required_vars = ["TELEGRAM_BOT_TOKEN", "NOTION_TOKEN", "OFFERS_DATABASE_ID", "ADVERTISERS_DATABASE_ID"]
        
        # Add debug print
        print("\nEnvironment variables:")
        for var in required_vars:
            value = os.getenv(var)
            print(f"{var}: {value}")
            if value:
                print(f"Length: {len(value)}")
        
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Send a message when the command /start is issued."""
        welcome_message = (
            "👋 Hi! I'm the Deal Parser Bot.\n\n"
            "Send me deal strings in this format:\n"
            "REGION-PARTNER-GEO-LANGUAGE-SOURCE-MODEL-CPA-CRG-CPL-FUNNELS-CR-DEDUCTIONLIMIT\n\n"
            "Example:\n"
            "TIER1-FTD Company-UK|IE|NL-Native-Facebook|Google-cpa_crg-1200-0.10-&-QuantumAI-&-0.05"
        )
        await update.message.reply_text(welcome_message)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Send a message when the command /help is issued."""
        help_text = (
            "📝 Required Fields:\n"
            "- Region (TIER1, LATAM, etc)\n"
            "- Partner name\n"
            "- GEO (country codes)\n"
            "- Language\n"
            "- Source\n"
            "- Pricing model (cpa_crg, cpa, cpl)\n"
            "- Funnels\n\n"
            "Plus relevant pricing fields based on model:\n"
            "- CPA/CRG: Both CPA and CRG required\n"
            "- CPA only: CPA required\n"
            "- CPL only: CPL required"
        )
        await update.message.reply_text(help_text)

    def parse_deal_string(self, deal_string: str) -> tuple[Deal, str]:
        """Parse a single deal string into a Deal object and return error message if any"""
        try:
            # Validate basic string format
            if not deal_string or not isinstance(deal_string, str):
                return None, "Invalid deal string format"
            
            # Split the string on hyphens
            fields = [f.strip() for f in deal_string.split("-")]
            if len(fields) != 12:
                return None, f"Expected 12 fields, got {len(fields)}"
            
            # Convert CPA/CPL to float if not "&"
            try:
                cpa = float(fields[6]) if fields[6] != "&" else None
                cpl = float(fields[8]) if fields[8] != "&" else None
            except ValueError:
                return None, f"Invalid CPA '{fields[6]}' or CPL '{fields[8]}' value. Must be a number or '&'"
            
            # Convert CRG to float and handle percentage
            crg = fields[7]
            if crg != "&":
                try:
                    crg = float(crg)
                    if crg > 1:  # Convert percentage to decimal
                        crg = crg / 100
                except ValueError:
                    return None, f"Invalid CRG value '{fields[7]}'. Must be a number or '&'"
            else:
                crg = None
                
            # Handle funnels list
            funnels = fields[9].split("|") if fields[9] != "&" else None
            
            # Convert deduction limit
            deduction = fields[11]
            if deduction != "&":
                try:
                    deduction = float(deduction)
                    if deduction > 1:  # Convert percentage to decimal
                        deduction = deduction / 100
                except ValueError:
                    return None, f"Invalid deduction limit '{fields[11]}'. Must be a number or '&'"
            else:
                deduction = None
                
            deal = Deal(
                region=fields[0],
                partner=fields[1],
                geo=fields[2],
                language=fields[3],
                source=fields[4],
                pricing_model=fields[5],
                cpa=cpa,
                crg=crg,
                cpl=cpl,
                funnels=funnels,
                cr=fields[10],
                deduction_limit=deduction
            )
            
            if not deal.is_valid:
                missing = []
                if not all([deal.region, deal.partner, deal.geo, deal.language]):
                    missing.extend([
                        "region" if not deal.region else None,
                        "partner" if not deal.partner else None,
                        "geo" if not deal.geo else None,
                        "language" if not deal.language else None
                    ])
                if not deal.source or deal.source == "&":
                    missing.append("source")
                if not deal.funnels or deal.funnels == ["&"]:
                    missing.append("funnels")
                    
                missing = [m for m in missing if m]  # Remove None values
                return None, f"Missing required fields: {', '.join(missing)}"
            
            return deal, None
            
        except Exception as e:
            logger.error(f"Error parsing deal string: {str(e)}")
            logger.error(f"Problematic string: {deal_string}")
            return None, f"Error parsing deal: {str(e)}"

    def _prepare_notion_data(self, deals: List[Deal]) -> List[Dict]:
        notion_deals = []
        for deal in deals:
            try:
                notion_deal = {
                    "company_name": deal.partner,
                    "geo": deal.geo,
                    "language": deal.language,
                    "source": deal.source,
                    "funnels": deal.funnels if isinstance(deal.funnels, list) else [deal.funnels] if deal.funnels else [],
                    "cpa": deal.cpa,
                    "crg": deal.crg,
                    "cpl": deal.cpl,
                    "deduction": deal.deduction_limit
                }
                notion_deals.append(notion_deal)
            except Exception as e:
                logger.error(f"Error preparing Notion data for deal: {str(e)}")
                continue
        return notion_deals

    async def _rate_limit(self):
        """Simple rate limiting for API calls"""
        current_time = time.time()
        if current_time - self.last_request_time < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval)
        self.last_request_time = current_time

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle incoming messages containing deal strings."""
        try:
            # Add message size limit
            MAX_DEALS = 50
            MAX_MESSAGE_LENGTH = 10000
            
            if len(update.message.text) > MAX_MESSAGE_LENGTH:
                await update.message.reply_text(
                    "❌ Message too long. Please split into smaller batches."
                )
                return
            
            deal_strings = [line.strip() for line in update.message.text.split("\n") if line.strip()]
            
            if len(deal_strings) > MAX_DEALS:
                await update.message.reply_text(
                    f"❌ Too many deals ({len(deal_strings)}). Maximum is {MAX_DEALS}."
                )
                return

            # Skip old messages (>30 seconds)
            if (datetime.now().timestamp() - update.message.date.timestamp()) > 30:
                return

            # Send initial processing message
            processing_msg = await update.message.reply_text("🔄 Starting deal analysis...\nPlease wait while I process your deals.")

            valid_deals = []
            invalid_deals = []
            error_messages = []
            
            # Process each deal string
            total_deals = len(deal_strings)
            for idx, deal_string in enumerate(deal_strings, 1):
                # Add debug logging
                logger.debug(f"Processing deal {idx}/{total_deals}: {deal_string}")
                
                deal, error = self.parse_deal_string(deal_string)
                if deal:
                    logger.debug(f"Successfully parsed deal: {deal.__dict__}")
                    valid_deals.append(deal)
                else:
                    logger.error(f"Failed to parse deal: {error}")
                    invalid_deals.append(deal_string)
                    error_messages.append(
                        f"Deal #{idx}:\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"📝 Input:\n{deal_string}\n\n"
                        f"❌ Error:\n{error}\n"
                    )

            if not valid_deals:
                error_summary = "❌ No valid deals found.\n\n"
                error_summary += "Issues found:\n\n"
                error_summary += "\n".join(error_messages)
                error_summary += "\n\nPlease fix the issues and try again."
                
                # Split message if too long
                if len(error_summary) > 4096:  # Telegram message length limit
                    chunks = [error_summary[i:i+4096] for i in range(0, len(error_summary), 4096)]
                    for chunk in chunks:
                        await processing_msg.reply_text(chunk)
                    await processing_msg.delete()
                else:
                    await processing_msg.edit_text(error_summary)
                return

            # If we have both valid and invalid deals, show warning
            if invalid_deals:
                warning = f"⚠️ Found {len(invalid_deals)} invalid deals:\n\n"
                warning += "\n".join(error_messages)
                warning += f"\n\nProceeding with {len(valid_deals)} valid deals..."
                
                # Split warning if too long
                if len(warning) > 4096:
                    chunks = [warning[i:i+4096] for i in range(0, len(warning), 4096)]
                    for chunk in chunks:
                        await processing_msg.reply_text(chunk)
                    await asyncio.sleep(3)  # Give users time to read the warning
                else:
                    await processing_msg.edit_text(warning)
                    await asyncio.sleep(3)  # Give users time to read the warning

            # Update processing message for Notion submission
            await processing_msg.edit_text(
                "🔄 Processing Submission...\n\n"
                "1️⃣ Collecting approved deals..."
            )

            # Prepare and submit deals to Notion
            notion_data = self._prepare_notion_data(valid_deals)
            
            await processing_msg.edit_text(
                "🔄 Processing Submission...\n\n"
                "1️⃣ Approved deals collected\n"
                "2️⃣ Initializing Notion connection..."
            )

            # Submit to Notion
            await processing_msg.edit_text(
                "🔄 Processing Submission...\n\n"
                "1️⃣ Approved deals collected\n"
                "2️⃣ Notion connection established\n"
                "3️⃣ Submitting deals..."
            )

            # Add timing
            start_time = time.time()
            results = self.notion_client.submit_deals(notion_data)
            completion_time = time.time() - start_time

            # Create summary message
            summary = "✅ Submission Complete!\n\n"
            summary += f"⏱️ Completed in: {completion_time:.1f}s\n"
            summary += "━━━━━━━━━━━━━━━━\n\n"
            summary += "📋 Submitted Deals:\n\n"
            
            for i, deal in enumerate(valid_deals, 1):
                summary += f"Deal #{i}: {deal.partner}\n"
                summary += "━━━━━━━━━━━━━━━\n"
                summary += f"🌍 GEO: {deal.geo}\n"
                summary += f"🗣 Language: {deal.language}\n"
                summary += f"📱 Source: {deal.source}\n"
                
                if deal.cpa:
                    summary += f"💰 CPA: ${deal.cpa}\n"
                if deal.crg:
                    summary += f" CRG: {float(deal.crg)*100:.0f}%\n"
                if deal.cpl:
                    summary += f"🎯 CPL: ${deal.cpl}\n"
                
                if deal.funnels:
                    summary += f"🔄 Funnels: {', '.join(deal.funnels)}\n"
                
                summary += "✅ Successfully submitted\n\n"

            # Add final statistics
            summary += f"📊 Final Results:\n"
            summary += f" {len(valid_deals)} deals submitted successfully"

            await processing_msg.edit_text(summary)

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}", exc_info=True)
            await update.message.reply_text(
                "❌ An error occurred while processing your deals.\n"
                f"Error details: {str(e)}\n"
                "Please check the format and try again."
            )

    def run(self):
        """Start the bot."""
        # Create application and add handlers
        application = Application.builder().token(os.getenv("TELEGRAM_BOT_TOKEN")).build()

        # Add command handlers
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("help", self.help_command))

        # Add message handler
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

        # Start the bot
        application.run_polling()

if __name__ == '__main__':
    try:
        bot = DealBot()
        print("🤖 Bot is running...")
        bot.run()
    except Exception as e:
        logger.error(f"Failed to start bot: {str(e)}", exc_info=True)
