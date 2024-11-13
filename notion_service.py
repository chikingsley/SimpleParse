import dotenv
from notion_client import Client as NotionClient  # Fix: Import from notion_client instead of notion_god
from typing import List, Dict, Any
from dotenv import load_dotenv
import logging
import os
import traceback
import json

# Logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load environment variables
try:
    load_dotenv()
except Exception as e:
    logger.error(f"Error loading .env file: {e}")

class NotionDealsClient:
    def __init__(self, notion_token: str, database_id: str, kitchen_database_id: str, debug: bool = False):
        logger.info("Initializing NotionDealsClient...")
        try:
            if debug:
                logger.setLevel(logging.DEBUG)
            
            self.client = NotionClient(auth=notion_token)
            self.database_id = database_id
            self.kitchen_database_id = kitchen_database_id
            
            logger.info(f"Initialized Notion client with databases:")
            logger.info(f"OFFERS_DATABASE_ID: {self.database_id}")
            logger.info(f"ADVERTISERS_DATABASE_ID: {self.kitchen_database_id}")
        except Exception as e:
            logger.error(f"Failed to initialize Notion client: {str(e)}")
            raise

    def submit_deals(self, deals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Submit multiple deals to Notion database"""
        logger.info(f"Starting submission of {len(deals)} deals")
        results = []
        for deal in deals:
            try:
                logger.info(f"Processing deal for company: {deal.get('company_name', 'Unknown')}")
                
                # Map Deal object fields to Notion properties
                company_name = deal.get('partner', deal.get('company_name'))
                company_id = self._get_or_create_company(company_name)
                
                # Handle multi-value fields
                languages = [lang.strip() for lang in str(deal.get('language', '')).split('|')]
                sources = [source.strip() for source in str(deal.get('source', deal.get('sources', ''))).split('|')]
                funnels = deal.get('funnels', [])
                if isinstance(funnels, str):
                    funnels = [f.strip() for f in funnels.split(',')]
                
                properties = {
                    "GEO-Funnel Code": {
                        "title": [{
                            "text": {
                                "content": f"{deal.get('geo')} {deal.get('language')}-{company_name}-{deal.get('source', deal.get('sources', ''))}"
                            }
                        }]
                    },
                    "Active Status`": {
                        "select": {
                            "name": "Active"
                        }
                    },
                    "Language": {
                        "multi_select": [{"name": lang} for lang in languages if lang]
                    },
                    "Sources": {
                        "multi_select": [{"name": source} for source in sources if source]
                    },
                    "Funnels": {
                        "multi_select": [{"name": funnel} for funnel in funnels if funnel]
                    },
                    "CPA | Buying": {
                        "number": float(deal.get('cpa', deal.get('cpa_buying'))) if deal.get('cpa', deal.get('cpa_buying')) else None
                    },
                    "CRG | Buying": {
                        "number": float(deal.get('crg', deal.get('crg_buying'))) if deal.get('crg', deal.get('crg_buying')) else None
                    },
                    "CPL | Buying": {
                        "number": float(deal.get('cpl', deal.get('cpl_buying'))) if deal.get('cpl', deal.get('cpl_buying')) else None
                    },
                    "CPA | Network | Selling": {
                        "number": float(deal.get('cpa', deal.get('cpa_buying'))) + 100 if deal.get('cpa', deal.get('cpa_buying')) else None
                    },
                    "CRG | Network | Selling": {
                        "number": float(deal.get('crg', deal.get('crg_buying'))) + 0.01 if deal.get('crg', deal.get('crg_buying')) else None
                    },
                    "CPL | Network | Selling": {
                        "number": float(deal.get('cpl', deal.get('cpl_buying'))) + 5 if deal.get('cpl', deal.get('cpl_buying')) else None
                    },
                    "Deduction %": {
                        "number": float(deal.get('deduction')) if deal.get('deduction') else None
                    },
                    "⚡ ALL ADVERTISERS | Kitchen": {
                        "relation": [{"id": company_id}]
                    }
                }

                # Remove None values
                properties = {k: v for k, v in properties.items() 
                            if (v.get("number") is not None or k != "number")}
                
                logger.info("Creating new page in Notion...")
                logger.debug(f"Properties for Notion: {properties}")

                # Add before creating new page
                if not self._validate_properties(properties):
                    raise ValueError("Invalid properties for Notion submission")

                # Create the new page in Individual OFFERS | Kitchen database
                new_page = self.client.pages.create(
                    parent={"database_id": self.database_id},
                    properties=properties
                )
                logger.info(f"Successfully created Notion page for {deal['company_name']}")
                results.append({"success": True, "deal": deal, "notion_page": new_page})
                
            except Exception as e:
                error_details = traceback.format_exc()
                logger.error(f"Error submitting deal to Notion: {str(e)}")
                logger.error(f"Deal data: {json.dumps(deal, indent=2)}")
                logger.error(f"Traceback: {error_details}")
                results.append({
                    "success": False, 
                    "deal": deal, 
                    "error": str(e),
                    "details": error_details
                })
            
        logger.info(f"Completed submission. Success: {sum(1 for r in results if r['success'])}, Failed: {sum(1 for r in results if not r['success'])}")
        return results

    def _get_or_create_company(self, company_name: str) -> str:
        """Search for existing company or create new one"""
        try:
            if not company_name:
                raise ValueError("Company name cannot be empty")
            
            logger.info(f"Searching for company: {company_name}")
            
            # Search for existing company
            search_results = self.client.databases.query(
                database_id=self.kitchen_database_id,
                filter={
                    "property": "title",
                    "title": {
                        "equals": company_name
                    }
                }
            )

            if search_results.get("results"):
                company_id = search_results["results"][0]["id"]
                logger.info(f"Found existing company: {company_name} (ID: {company_id})")
                return company_id
            
            # Create new company
            logger.info(f"Creating new company: {company_name}")
            new_company = self.client.pages.create(
                parent={"database_id": self.kitchen_database_id},
                properties={
                    "title": {
                        "title": [{"text": {"content": company_name}}]
                    }
                }
            )
            
            company_id = new_company["id"]
            logger.info(f"Created new company: {company_name} (ID: {company_id})")
            return company_id
            
        except Exception as e:
            logger.error(f"Error handling company {company_name}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def _validate_properties(self, properties: Dict) -> bool:
        """Validate properties before submission"""
        required_fields = ["GEO-Funnel Code", "Active Status`", "Language", "Sources"]
        
        for field in required_fields:
            if field not in properties:
                logger.error(f"Missing required field: {field}")
                return False
            
        return True
