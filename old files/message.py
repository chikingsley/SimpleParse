from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from telegram.ext import ContextTypes, CallbackContext
from bot.client import DealParser, FieldValidator
import logging
import time
from typing import Any
from .notion_client import NotionDealsClient
import os
import re
import asyncio
import traceback

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class MessageHandler:
    def __init__(self):
        self.deal_parser = DealParser(message=None)
        self.current_deals = {}
        self.deal_statuses = {}  # Track status of each deal
        self.user_states = {}  # Track user states
        self.session_timeout = 3600  # 1 hour
        self.editing_state = {}  # Track who's editing what

    def _cleanup_old_sessions(self):
        """Remove expired sessions"""
        current_time = time.time()
        expired = [
            user_id for user_id, data in self.user_states.items()
            if current_time - data['last_activity'] > self.session_timeout
        ]
        for user_id in expired:
            del self.user_states[user_id]
            del self.current_deals[user_id]

    async def _format_deal_message(self, deal, index: int, total: int, user_id: int) -> str:
        """Format deal with status emoji and raw text"""
        # Get deal status
        status = self.deal_statuses.get(user_id, {}).get(index-1)
        
        # Choose status emoji
        status_emoji = "📋"  # Default
        if status == 'approved':
            status_emoji = "✅"
        elif status == 'rejected':
            status_emoji = "❌"
            
        # Get parsed data from correct location
        parsed_data = deal.get('parsed_data', deal)
        
        # Set default language ONLY if it's None, empty string, or not present
        if not parsed_data.get('language'):
            geo = parsed_data.get('geo', '').lower()
            if any(eng_geo in geo for eng_geo in ['uk', 'us', 'gb', 'au', 'ca']):
                parsed_data['language'] = 'English'
            else:
                parsed_data['language'] = 'Native'
        
        # Ensure funnels is always a list
        funnels = parsed_data.get('funnels', [])
        if isinstance(funnels, str):
            funnels = [funnels]
        elif not isinstance(funnels, list):
            funnels = []
            
        raw_text = deal.get('raw_text', '')
            
        return (
            f"{status_emoji} Deal {index} of {total}\n\n"
            f"📝 Original Text:\n{raw_text}\n\n"
            f"📊 Deal Details:\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🤝 Partner: {parsed_data.get('partner', 'N/A')}\n"
            f"🌍 Region: {parsed_data.get('region', 'N/A')}\n"
            f"🗺 GEO: {parsed_data.get('geo', 'N/A')}\n"
            f"🗣 Language: {parsed_data.get('language', 'Native')}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📱 Source: {parsed_data.get('source', 'N/A')}\n"
            f"💰 Pricing Model: {parsed_data.get('pricing_model', 'N/A')}\n"
            f"💵 CPA: {parsed_data.get('cpa', 'N/A')}\n"
            f"📈 CRG: {f'{round(parsed_data.get('crg')*100, 2):.0f}%' if parsed_data.get('crg') else 'N/A'}\n"
            f"🎯 CPL: {parsed_data.get('cpl', 'N/A')}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🔄 Funnels: {', '.join(funnels) if funnels else 'N/A'}\n"
            f"📊 CR: {f'{round(parsed_data.get('cr')*100, 2):.0f}%' if parsed_data.get('cr') else 'N/A'}\n"
            f"📉 Deduction Limit: {f'{round(parsed_data.get('deduction_limit')*100, 2):.0f}%' if parsed_data.get('deduction_limit') else 'N/A'}\n"
            f"━━━━━━━━━━━━━━━"
        )

    async def _create_keyboard(self, current_index: int, total_deals: int, statuses: dict) -> InlineKeyboardMarkup:
        keyboard = []
        
        # Navigation buttons
        if total_deals > 1:
            nav_row = []
            if current_index > 0:
                nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"prev_{current_index}"))
            if current_index < total_deals - 1:
                nav_row.append(InlineKeyboardButton("➡️ Next", callback_data=f"next_{current_index}"))
            if nav_row:
                keyboard.append(nav_row)

        # Action buttons with status
        status = statuses.get(current_index)
        approve_text = "✅ Approved" if status == 'approved' else "Approve ?"
        reject_text = "❌ Rejected" if status == 'rejected' else "Reject ?"
        
        keyboard.extend([
            [
                InlineKeyboardButton(approve_text, callback_data=f"approve_{current_index}"),
                InlineKeyboardButton(reject_text, callback_data=f"reject_{current_index}")
            ],
            [InlineKeyboardButton("✏️ Edit", callback_data=f"edit_{current_index}")]
        ])

        return InlineKeyboardMarkup(keyboard)

    def _clean_field(self, value, field_type='text'):
        """Clean and format field values consistently"""
        return FieldValidator.clean_value(value, field_type)

    async def _update_field_value(self, field: str, value: str) -> Any:
        """Validate and convert field values"""
        try:
            if field in ['crg', 'cr']:
                return FieldValidator.clean_value(value, field)
            elif field in ['cpa', 'cpl']:
                # Convert to float
                return float(value)
            elif field == 'pricing_model':
                # Validate pricing model
                valid_models = ['CPA/CRG', 'CPA', 'CPL']
                if value.upper() not in valid_models:
                    raise ValueError(f"Must be one of: {', '.join(valid_models)}")
                return value.upper()
            elif field == 'deduction_limit':
                # Remove % if present and convert to decimal                
                value = value.replace('%', '')
                return float(value) / 100
            else:
                # Use FieldValidator for other fields
                return FieldValidator.clean_value(value, field)
        except ValueError as e:
            raise ValueError(f"Invalid value for {field}: {str(e)}")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle incoming messages and callback queries"""
        try:
            # Handle callback queries (button clicks) - NO timestamp check for callbacks
            if update.callback_query:
                await self.handle_callback(update, context)
                return

            # Check message age ONLY for new messages
            if update.message and (time.time() - update.message.date.timestamp() > 30):
                logger.info("Skipping old message")
                return

            # Handle text messages
            if not update.message or not update.message.text:
                logger.warning("Received update without message text")
                return

            user_id = update.effective_user.id
            message_text = update.message.text
            
            # Check if user is in editing state FIRST
            if user_id in self.editing_state:
                await self._handle_edit_input(update, context)
                return
            
            # If not editing, then check if it's a deal message
            # Require at least two strong indicators to consider it a deal
            strong_indicators = [
                # Price patterns
                r"\d+\s*\+\s*\d+%",          # 1000+10%
                r"\$?\s*\d+\s*\+\s*\d+%",    # $1000+10%
                r"(?:Price|CPA|CPL)\s*:?\s*[\d.]+", # Price: 1000, CPA: 1000, CPL: 15
                
                # Deal headers
                r"(?:Partner|Company)\s*:",   # Partner: or Company:
                r"(?:GEO|Country)\s*:",       # GEO: or Country:
            ]
            
            supporting_indicators = [
                # Deal elements
                r"Source\s*:",
                r"Funnel(?:s)?\s*:",
                r"Landing Page\s*:",
                r"model\s*:",
                
                # Country codes
                r"[A-Z]{2}\s*(?:native|eng|fr|es|de)",  # e.g., "UK eng" or "FR native"
                
                # Traffic sources
                r"(?:FB|Facebook|Google|SEO|Taboola|Native)\s+[Tt]raffic",
            ]
            
            # Count strong and supporting indicators
            strong_matches = sum(1 for pattern in strong_indicators 
                               if re.search(pattern, message_text, re.IGNORECASE))
            supporting_matches = sum(1 for pattern in supporting_indicators 
                                  if re.search(pattern, message_text, re.IGNORECASE))
            
            # Require at least one strong indicator and one supporting indicator
            is_deal = strong_matches >= 1 and supporting_matches >= 1
            
            # Also check it's not just a progress message
            if "Deal Parsing Progress" in message_text or "Processing deal" in message_text:
                is_deal = False
            
            if not is_deal:
                # Regular conversation flow
                response = ("I can help you submit deals! Just share the deal details including:\n"
                           "• Partner/Company name\n"
                           "• GEO/Country\n"
                           "• Price (CPA/CPL)\n"
                           "• Traffic source\n"
                           "• Funnels")
                await update.message.reply_text(response)
                return

            # Create initial message
            self.processing_message = await update.message.reply_text(
                "🔄 Starting deal analysis...\n"
                "Please wait while I process your deals."
            )
            
            # Update DealParser with the message
            self.deal_parser.message = self.processing_message
            
            # Parse deals
            formatted_deals = await self.deal_parser.parse_deals(message_text)
            
            # Store deals for this user
            self.current_deals[user_id] = {
                'deals': formatted_deals,
                'current_index': 0,
                'last_activity': time.time()
            }
            
            # Initialize status tracking
            self.deal_statuses[user_id] = {}
            
            # Display the first deal
            await self._display_current_deal(update, self.processing_message, user_id)
            
        except Exception as e:
            error_message = (
                "❌ Error processing your message.\n"
                "Please check the format and try again."
            )
            logger.error(f"Error details: {str(e)}")
            if hasattr(self, 'processing_message'):
                try:
                    await self.processing_message.edit_text(error_message)
                except Exception as edit_error:
                    logger.error(f"Error updating error message: {str(edit_error)}")
                    if update.message:
                        await update.message.reply_text(error_message)
            elif update.message:
                await update.message.reply_text(error_message)

    async def _display_current_deal(self, update: Update, message, user_id: int):
        """Display current deal with navigation"""
        user_data = self.current_deals.get(user_id)
        if not user_data or not user_data.get('deals'):
            logger.error(f"No deals found for user {user_id}")
            await message.edit_text("❌ No deals to display. Please submit some deals first.")
            return

        current_index = user_data.get('current_index', 0)
        total_deals = len(user_data['deals'])
        
        # Validate index
        if current_index >= total_deals:
            logger.error(f"Invalid index {current_index} for {total_deals} deals")
            current_index = 0
            user_data['current_index'] = 0
        
        try:
            deal = user_data['deals'][current_index]
            
            # Pass user_id to _format_deal_message
            deal_text = await self._format_deal_message(
                deal, 
                current_index + 1, 
                total_deals,
                user_id
            )
            
            # Create keyboard
            reply_markup = await self._create_keyboard(
                current_index, 
                total_deals, 
                self.deal_statuses.get(user_id, {})
            )

            if message:
                await message.edit_text(deal_text, reply_markup=reply_markup)
            else:
                await update.message.reply_text(deal_text, reply_markup=reply_markup)
                
        except Exception as e:
            logger.error(f"Error displaying deal: {str(e)}")
            error_message = (
                "❌ Error displaying deal.\n"
                "Please try submitting your deals again."
            )
            if message:
                await message.edit_text(error_message)
            else:
                await update.message.reply_text(error_message)

    async def _submit_to_notion(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Submit approved deals to Notion"""
        try:
            query = update.callback_query
            user_id = update.effective_user.id
            start_time = time.time()
            
            logger.info("Starting Notion submission process...")
            
            # Initial status update
            await query.edit_message_text(
                "🔄 Processing Submission...\n\n"
                "1️⃣ Collecting approved deals..."
            )
            
            # Get approved deals
            approved_deals = []
            for idx, deal in enumerate(self.current_deals[user_id]['deals']):
                if self.deal_statuses.get(user_id, {}).get(idx) == 'approved':
                    parsed_data = deal.get('parsed_data', deal)
                    approved_deal = {
                        'company_name': self._clean_field(parsed_data.get('partner')),
                        'region': self._clean_field(parsed_data.get('region'), 'geo'),
                        'geo': self._clean_field(parsed_data.get('geo'), 'geo'),
                        'language': self._clean_field(parsed_data.get('language'), 'language'),
                        'sources': self._clean_field(parsed_data.get('source'), 'sources'),
                        'cpa_buying': parsed_data.get('cpa', ''),
                        'crg_buying': parsed_data.get('crg', ''),
                        'cpl_buying': parsed_data.get('cpl', ''),
                        'funnels': self._clean_field(
                            parsed_data.get('funnels', []) if isinstance(parsed_data.get('funnels'), list)
                            else parsed_data.get('funnels', ''),
                            'list'
                        ),
                        'cr': parsed_data.get('cr', ''),
                        'deduction': parsed_data.get('deduction_limit', '')
                    }
                    if any([approved_deal['cpa_buying'], 
                           approved_deal['crg_buying'], 
                           approved_deal['cpl_buying']]):
                        approved_deals.append(approved_deal)

            if not approved_deals:
                await query.edit_message_text(
                    "❌ No approved deals found to submit.\n"
                    "Please approve at least one deal before submitting to Notion."
                )
                return

            logger.info(f"Found {len(approved_deals)} approved deals to submit")

            # Update status - Initializing Notion
            await query.edit_message_text(
                "🔄 Processing Submission...\n\n"
                "1️⃣ Approved deals collected\n"
                "2️⃣ Initializing Notion connection..."
            )

            # Initialize Notion client
            notion_token = os.getenv('NOTION_TOKEN')
            offers_db_id = os.getenv('OFFERS_DATABASE_ID')
            kitchen_db_id = os.getenv('ADVERTISERS_DATABASE_ID')
            
            if not all([notion_token, offers_db_id, kitchen_db_id]):
                raise ValueError("Missing required Notion environment variables")
            
            notion_client = NotionDealsClient(
                notion_token=notion_token,
                database_id=offers_db_id,
                kitchen_database_id=kitchen_db_id
            )

            # Update status - Submitting
            await query.edit_message_text(
                "🔄 Processing Submission...\n\n"
                "1️⃣ Approved deals collected\n"
                "2️⃣ Notion connection established\n"
                "3️⃣ Submitting deals..."
            )
            logger.info("Submitting deals to Notion...")

            # Submit deals to Notion
            results = notion_client.submit_deals(approved_deals)

            # Calculate completion time
            completion_time = time.time() - start_time
            
            logger.info(f"Notion submission completed in {completion_time:.2f} seconds")

            # Create pretty summary
            summary = "✅ Submission Complete!\n\n"
            summary += f"⏱️ Completed in: {completion_time:.1f}s\n"
            summary += "━━━━━━━━━━━━━━━\n\n"
            summary += "📋 Submitted Deals:\n\n"
            
            for i, deal in enumerate(approved_deals, 1):
                summary += f"Deal #{i}: {deal['company_name']}\n"
                summary += "━━━━━━━━━━━━━━━\n"
                summary += f"🌍 GEO: {deal['geo']}\n"
                summary += f"🗣 Language: {deal['language']}\n"
                summary += f"📱 Source: {deal['sources']}\n"
                
                if deal['cpa_buying']:
                    summary += f"💰 CPA: ${deal['cpa_buying']}\n"
                if deal['crg_buying']:
                    summary += f" CRG: {float(deal['crg_buying'])*100:.0f}%\n"
                if deal['cpl_buying']:
                    summary += f"🎯 CPL: ${deal['cpl_buying']}\n"
                
                if deal['funnels']:
                    # Handle both string and list cases
                    if isinstance(deal['funnels'], str):
                        summary += f"🔄 Funnels: {deal['funnels']}\n"
                    else:
                        summary += f"🔄 Funnels: {', '.join(deal['funnels'])}\n"
                
                summary += "✅ Successfully submitted\n\n"

            # Add final statistics
            summary += f"📊 Final Results:\n"
            summary += f"✅ {len(approved_deals)} deals submitted successfully"

            # Update message with pretty summary and new keyboard
            keyboard = [[
                InlineKeyboardButton("🆕 Process New Deals", callback_data="final_discard")
            ]]
            
            await query.edit_message_text(
                text=summary,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
            # Clear processed deals
            del self.current_deals[user_id]
            del self.deal_statuses[user_id]

        except Exception as e:
            error_details = traceback.format_exc()
            logger.error(f"Error submitting to Notion: {str(e)}\n{error_details}")
            error_msg = f"❌ Error submitting to Notion: {str(e)}"
            await query.edit_message_text(error_msg)

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle button callbacks"""
        query = update.callback_query
        user_id = update.effective_user.id
        
        try:
            await query.answer()
            
            # Handle Notion submission
            if query.data == "submit_notion":
                logger.info("Notion submission button clicked")
                await self._submit_to_notion(update, context)
                return
                
            # Handle final action buttons
            if query.data.startswith('final_'):
                action = query.data.split('_')[1]
                
                if action == 'discard':
                    # Clear user data
                    if user_id in self.current_deals:
                        del self.current_deals[user_id]
                    if user_id in self.deal_statuses:
                        del self.deal_statuses[user_id]
                        
                    await query.edit_message_text(
                        "🗑️ Deals Discarded Successfully\n\n"
                        "Your deals have been cleared from the system. "
                        "Thank you for using the Deal Parser."
                    )
                    return
                    
                elif action == 'reprocess':
                    # Reset statuses but keep deals
                    if user_id in self.deal_statuses:
                        del self.deal_statuses[user_id]
                    
                    # Reset to first deal and replace the current message entirely
                    if user_id in self.current_deals:
                        self.current_deals[user_id]['current_index'] = 0
                        await query.edit_message_text(
                            text=await self._format_deal_message(
                                self.current_deals[user_id]['deals'][0],
                                1,
                                len(self.current_deals[user_id]['deals']),
                                user_id
                            ),
                            reply_markup=await self._create_keyboard(
                                0,
                                len(self.current_deals[user_id]['deals']),
                                {}  # Reset statuses
                            )
                        )
                    return

            # Split callback data
            parts = query.data.split('_')
            action = parts[0]
            index = int(parts[-1])  # Last part is always the index
            
            user_data = self.current_deals.get(user_id)
            if not user_data:
                return
                
            total_deals = len(user_data['deals'])
            current_deal = user_data['deals'][index]
            
            if action == 'edit':
                # Show edit options keyboard
                keyboard = [
                    [
                        InlineKeyboardButton("Partner", callback_data=f"editfield_partner_{index}"),
                        InlineKeyboardButton("GEO", callback_data=f"editfield_geo_{index}")
                    ],
                    [
                        InlineKeyboardButton("CPA", callback_data=f"editfield_cpa_{index}"),
                        InlineKeyboardButton("CRG", callback_data=f"editfield_crg_{index}")
                    ],
                    [
                        InlineKeyboardButton("CPL", callback_data=f"editfield_cpl_{index}"),
                        InlineKeyboardButton("CR", callback_data=f"editfield_cr_{index}")
                    ],
                    [
                        InlineKeyboardButton("Source", callback_data=f"editfield_source_{index}"),
                        InlineKeyboardButton("Funnels", callback_data=f"editfield_funnels_{index}")
                    ],
                    [
                        InlineKeyboardButton("Language", callback_data=f"editfield_language_{index}"),
                        InlineKeyboardButton("Pricing Model", callback_data=f"editmodel_{index}")
                    ],
                    [
                        InlineKeyboardButton("Deduction Limit", callback_data=f"editfield_deduction_limit_{index}")
                    ],
                    [InlineKeyboardButton("🔙 Back", callback_data=f"back_{index}")]
                ]
                await query.edit_message_reply_markup(InlineKeyboardMarkup(keyboard))
                
            elif action == 'editmodel':
                # Show pricing model options
                keyboard = [
                    [InlineKeyboardButton("CPA/CRG", callback_data=f"setmodel_CPA/CRG_{index}")],
                    [InlineKeyboardButton("CPA", callback_data=f"setmodel_CPA_{index}")],
                    [InlineKeyboardButton("CPL", callback_data=f"setmodel_CPL_{index}")],
                    [InlineKeyboardButton("🔙 Back", callback_data=f"edit_{index}")]
                ]
                await query.edit_message_reply_markup(InlineKeyboardMarkup(keyboard))
                
            elif action == 'setmodel':
                # Update pricing model
                model = parts[1]
                deal = self.current_deals[user_id]['deals'][index]
                if 'parsed_data' in deal:
                    deal['parsed_data']['pricing_model'] = model
                else:
                    deal['pricing_model'] = model
                    
                # Show updated deal
                await query.edit_message_text(
                    await self._format_deal_message(deal, index + 1, total_deals, user_id),
                    reply_markup=await self._create_keyboard(index, total_deals, self.deal_statuses.get(user_id, {}))
                )
                
            elif action == 'editfield':
                # Store editing state with original message
                self.editing_state[user_id] = {
                    'field': parts[1],
                    'deal_index': index,
                    'message': query.message  # Store the original message
                }
                
                # Show edit prompt
                await query.edit_message_text(
                    f"Please enter new value for {parts[1]}:\n\n" +
                    (await self._format_deal_message(current_deal, index + 1, total_deals, user_id)) +
                    "\n\nType your new value or click Back to cancel.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Back", callback_data=f"back_{index}")
                    ]])
                )
                
            elif action == 'back':
                # Return to main deal view
                await self._display_current_deal(update, query.message, user_id)
                
            elif action == 'approve':
                try:
                    # Update status in our local storage
                    if user_id not in self.deal_statuses:
                        self.deal_statuses[user_id] = {}
                    self.deal_statuses[user_id][index] = 'approved'
                    
                    # If there's a next deal, show it in the same window
                    if index < total_deals - 1:
                        user_data['current_index'] = index + 1
                        next_deal = user_data['deals'][index + 1]
                        await query.edit_message_text(
                            text=await self._format_deal_message(next_deal, index + 2, total_deals, user_id),
                            reply_markup=await self._create_keyboard(index + 1, total_deals, self.deal_statuses[user_id])
                        )
                    else:
                        # If this was the last deal, show summary
                        await self._show_summary(update, user_id)
                        
                except Exception as e:
                    logger.error(f"Error in approve action: {str(e)}", exc_info=True)
                    await query.answer("Error processing approval")
                    
            elif action == 'reject':
                # Update status
                if user_id not in self.deal_statuses:
                    self.deal_statuses[user_id] = {}
                self.deal_statuses[user_id][index] = 'rejected'
                
                # If there's a next deal, show it in the same window
                if index < total_deals - 1:
                    user_data['current_index'] = index + 1
                    next_deal = user_data['deals'][index + 1]
                    await query.edit_message_text(
                        text=await self._format_deal_message(next_deal, index + 2, total_deals, user_id),
                        reply_markup=await self._create_keyboard(index + 1, total_deals, self.deal_statuses[user_id])
                    )
                else:
                    # If this was the last deal, show summary
                    await self._show_summary(update, user_id)
                    
            elif action == 'next':
                if index < total_deals - 1:
                    user_data['current_index'] = index + 1
                    await self._display_current_deal(update, query.message, user_id)
                    
            elif action == 'prev':
                if index > 0:
                    user_data['current_index'] = index - 1
                    await self._display_current_deal(update, query.message, user_id)

        except Exception as e:
            error_details = traceback.format_exc()
            logger.error(f"Error handling callback: {str(e)}\n{error_details}")
            await query.answer("Error processing button click")

    async def _show_summary(self, update: Update, user_id: int):
        """Show summary of all deals"""
        try:
            deals = self.current_deals[user_id]['deals']
            statuses = self.deal_statuses[user_id]
            
            # Count statuses
            approved = sum(1 for i in range(len(deals)) if statuses.get(i) == 'approved')
            rejected = sum(1 for i in range(len(deals)) if statuses.get(i) == 'rejected')
            pending = len(deals) - approved - rejected
            
            summary = (
                "📊 Deal Review Summary\n\n"
                f"✅ Approved: {approved}\n"
                f"❌ Rejected: {rejected}\n"
                f"⏳ Pending: {pending}\n\n"
            )
            
            # Create final action buttons
            keyboard = [
                [
                    InlineKeyboardButton("⚪️ Submit to Notion", callback_data="submit_notion"),
                    InlineKeyboardButton("♺ Reprocess", callback_data="final_reprocess")
                ],
                [InlineKeyboardButton("🗑️ Discard All", callback_data="final_discard")]
            ]
            
            # Edit the existing message instead of creating a new one
            if isinstance(update.callback_query, CallbackQuery):
                await update.callback_query.edit_message_text(
                    text=summary,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            else:
                await update.effective_chat.send_message(
                    text=summary,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            
        except Exception as e:
            error_details = traceback.format_exc()
            logger.error(f"Error showing summary: {str(e)}\n{error_details}")

    async def _handle_progress(self, stage: str, data: dict):
        """Handle progress updates from client"""
        if not hasattr(self, 'processing_message'):
            return
            
        try:
            # Get message from data
            message = data.get('message', '')
            
            # Add progress bar for deal processing
            if stage == 'progress' and 'current' in data and 'total' in data:
                current = data['current']
                total = data['total']
                progress = current / total
                bar_length = 20
                filled = int(bar_length * progress)
                bar = '█' * filled + '░' * (bar_length - filled)
                
                message = (
                    f"{message}\n\n"
                    f"[{bar}] {current}/{total}"
                )
            
            # Only update if message has changed
            if self.processing_message.text != message:
                await self.processing_message.edit_text(message)
                
        except Exception as e:
            logger.error(f"Error updating progress: {str(e)}")

    async def _handle_edit_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text input after edit button click"""
        try:
            user_id = update.effective_user.id
            edit_state = self.editing_state[user_id]
            
            # Get the field and deal being edited
            field = edit_state['field']
            deal_index = edit_state['deal_index']
            original_message = edit_state['message']
            
            # Get the new value and validate/convert it
            new_value = update.message.text.strip()
            try:
                converted_value = await self._update_field_value(field, new_value)
            except ValueError as e:
                await update.message.reply_text(
                    f"❌ Invalid value: {str(e)}\n"
                    "Please try again or click Back to cancel."
                )
                return

            # Update the deal with new value
            deal = self.current_deals[user_id]['deals'][deal_index]
            if 'parsed_data' in deal:
                deal['parsed_data'][field] = converted_value
            else:
                deal[field] = converted_value

            # Delete the edit prompt message and user's input message
            await original_message.delete()
            await update.message.delete()

            # Show updated deal
            await self._display_current_deal(update, None, user_id)

            # Clear editing state
            del self.editing_state[user_id]

        except Exception as e:
            logger.error(f"Error handling edit input: {str(e)}")
            await update.message.reply_text(
                "❌ Error updating value. Please try again."
            )
            # Clean up editing state on error
            if user_id in self.editing_state:
                del self.editing_state[user_id]
