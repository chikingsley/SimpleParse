import dotenv
from notion_client import Client
from typing import List, Dict, Any
from dotenv import load_dotenv
import logging
import os
import traceback

# Logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load environment variables
try:
    load_dotenv()
except Exception as e:
    logger.error(f"Error loading .env file: {e}")

class NotionDealsClient:
    def __init__(self, notion_token: str, database_id: str, kitchen_database_id: str):
        logger.info("Initializing NotionDealsClient...")
        try:
            self.client = Client(auth=notion_token)
            # Use passed parameters instead of re-fetching from env
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
                
                # Get or create company in ALL ADVERTISERS | Kitchen database
                company_id = self._get_or_create_company(deal["company_name"])
                logger.info(f"Got company ID: {company_id}")
                
                # Split multi-value fields
                languages = [lang.strip() for lang in str(deal["language"]).split("|")]
                sources = [source.strip() for source in str(deal["sources"]).replace(",", "|").split("|")]
                funnels = [funnel.strip() for funnel in str(deal["funnels"]).split(",")]
                
                logger.info(f"Processed fields - Languages: {languages}, Sources: {sources}, Funnels: {funnels}")
                
                # Format the properties according to exact Notion schema
                properties = {
                    "GEO-Funnel Code": {  
                        "title": [{
                            "text": {
                                "content": f"{deal['geo']} {deal['language']}-{deal['company_name']}-{deal['sources']}"
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
                        "number": float(deal["cpa_buying"]) if deal["cpa_buying"] else None
                    },
                    "CRG | Buying": {
                        "number": float(deal["crg_buying"]) if deal["crg_buying"] else None
                    },
                    "CPL | Buying": {
                        "number": float(deal["cpl_buying"]) if deal["cpl_buying"] else None
                    },
                    "CPA | Network | Selling": {
                        "number": float(deal["cpa_buying"]) + 100 if deal["cpa_buying"] else None
                    },
                    "CRG | Network | Selling": {
                        "number": float(deal["crg_buying"]) + 0.01 if deal["crg_buying"] else None
                    },
                    "CPL | Network | Selling ": {  
                        "number": float(deal["cpl_buying"]) + 5 if deal["cpl_buying"] else None
                    },
                    "Deduction %": {
                        "number": float(deal["deduction"]) if deal["deduction"] else None
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

                # Create the new page in Individual OFFERS | Kitchen database
                new_page = self.client.pages.create(
                    parent={"database_id": self.database_id},
                    properties=properties
                )
                logger.info(f"Successfully created Notion page for {deal['company_name']}")
                results.append({"success": True, "deal": deal, "notion_page": new_page})
                
            except Exception as e:
                error_details = traceback.format_exc()
                logger.error(f"Error submitting deal to Notion: {str(e)}\n{error_details}")
                results.append({
                    "success": False, 
                    "deal": deal, 
                    "error": str(e),
                    "details": error_details
                })
            
        logger.info(f"Completed submission. Success: {sum(1 for r in results if r['success'])}, Failed: {sum(1 for r in results if not r['success'])}")
        return results

    def _get_or_create_company(self, company_name: str) -> str:
        """Search for existing company or create new one in ALL ADVERTISERS | Kitchen database"""
        try:
            logger.info(f"Searching for company: {company_name}")
            
            # Search for existing company with modified filter syntax
            search_results = self.client.databases.query(
                database_id=self.kitchen_database_id,
                filter={
                    "property": "title",  
                    "title": {
                        "equals": company_name
                    }
                }
            )

            logger.debug(f"Search results: {search_results}")

            if search_results["results"]:
                logger.info(f"Found existing company: {company_name}")
                return search_results["results"][0]["id"]
            
            logger.info(f"Creating new company: {company_name}")
            # Create new company if not found
            new_company = self.client.pages.create(
                parent={"database_id": self.kitchen_database_id},
                properties={
                    "title": {  
                        "title": [{"text": {"content": company_name}}]
                    }
                }
            )
            logger.info(f"Successfully created new company: {company_name}")
            return new_company["id"]
            
        except Exception as e:
            error_details = traceback.format_exc()
            logger.error(f"Notion API Error for company {company_name}: {str(e)}\n{error_details}")
            raise Exception(f"Error handling company {company_name}: {str(e)}")
