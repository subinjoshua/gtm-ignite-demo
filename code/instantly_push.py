#!/usr/bin/env python3
"""
Instantly.ai Lead Push Pipeline
================================

Pushes enriched leads to Instantly.ai campaigns via API.

Features:
- Create/update leads in Instantly
- Assign to campaigns by persona
- Track push status in database
- Demo mode for testing

Usage:
    # Demo mode (no API calls)
    python instantly_push.py --demo
    
    # Live mode
    python instantly_push.py --input enriched_leads.csv --campaign camp_123

Environment Variables:
    INSTANTLY_API_KEY - Your Instantly.ai API key
    DATABASE_URL - PostgreSQL connection string (optional)

"""

import os
import json
import time
import argparse
import logging
import csv
from datetime import datetime
from typing import List, Dict, Optional

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

INSTANTLY_API_BASE = "https://api.instantly.ai/api/v1"
RATE_LIMIT_DELAY = 0.5  # seconds between API calls

# Campaign IDs (you would set these after creating campaigns in Instantly)
CAMPAIGNS = {
    "superintendent": "camp_tx_superintendents_q1_2026",
    "safety_director": "camp_tx_safety_directors_q1_2026",
}

# ============================================================================
# DEMO DATA
# ============================================================================

DEMO_LEADS = [
    {
        "email": "bruce.gearing@leanderisd.org",
        "first_name": "Bruce",
        "last_name": "Gearing",
        "company_name": "Leander ISD",
        "title": "Superintendent",
        "persona": "superintendent",
        "custom_variables": {
            "enrollment": 42000,
            "city": "Leander",
            "has_safety_director": True
        }
    },
    {
        "email": "sha.rogers@leanderisd.org",
        "first_name": "Shā",
        "last_name": "Rogers",
        "company_name": "Leander ISD",
        "title": "Chief of Safety & Security",
        "persona": "safety_director",
        "custom_variables": {
            "enrollment": 42000,
            "city": "Leander"
        }
    },
    {
        "email": "mike.waldrip@friscoisd.org",
        "first_name": "Mike",
        "last_name": "Waldrip",
        "company_name": "Frisco ISD",
        "title": "Superintendent",
        "persona": "superintendent",
        "custom_variables": {
            "enrollment": 67000,
            "city": "Frisco"
        }
    },
    {
        "email": "rick.westfall@kellerisd.net",
        "first_name": "Rick",
        "last_name": "Westfall",
        "company_name": "Keller ISD",
        "title": "Superintendent",
        "persona": "superintendent",
        "custom_variables": {
            "enrollment": 34000,
            "city": "Keller"
        }
    },
    {
        "email": "fred.brent@georgetownisd.org",
        "first_name": "Fred",
        "last_name": "Brent",
        "company_name": "Georgetown ISD",
        "title": "Superintendent",
        "persona": "superintendent",
        "custom_variables": {
            "enrollment": 14000,
            "city": "Georgetown"
        }
    },
    {
        "email": "hafedh_azaiez@roundrockisd.org",
        "first_name": "Hafedh",
        "last_name": "Azaiez",
        "company_name": "Round Rock ISD",
        "title": "Superintendent",
        "persona": "superintendent",
        "custom_variables": {
            "enrollment": 47000,
            "city": "Round Rock"
        }
    },
    {
        "email": "elizabeth.fagen@humbleisd.net",
        "first_name": "Elizabeth",
        "last_name": "Fagen",
        "company_name": "Humble ISD",
        "title": "Superintendent",
        "persona": "superintendent",
        "custom_variables": {
            "enrollment": 47000,
            "city": "Humble"
        }
    },
    {
        "email": "holly.ferguson@prosper-isd.net",
        "first_name": "Holly",
        "last_name": "Ferguson",
        "company_name": "Prosper ISD",
        "title": "Superintendent",
        "persona": "superintendent",
        "custom_variables": {
            "enrollment": 30000,
            "city": "Prosper"
        }
    }
]

# ============================================================================
# INSTANTLY API CLIENT
# ============================================================================

class InstantlyClient:
    """Client for Instantly.ai API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
    
    def _make_request(self, method: str, endpoint: str, data: Dict = None) -> Dict:
        """Make API request"""
        
        url = f"{INSTANTLY_API_BASE}/{endpoint}"
        params = {"api_key": self.api_key}
        
        try:
            time.sleep(RATE_LIMIT_DELAY)
            
            if method == "GET":
                response = self.session.get(url, params=params)
            elif method == "POST":
                response = self.session.post(url, params=params, json=data)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"API Error: {e}")
            return {"error": str(e)}
    
    def list_campaigns(self) -> List[Dict]:
        """List all campaigns"""
        result = self._make_request("GET", "campaign/list")
        return result if isinstance(result, list) else []
    
    def get_campaign(self, campaign_id: str) -> Dict:
        """Get campaign details"""
        return self._make_request("GET", f"campaign/get?campaign_id={campaign_id}")
    
    def add_lead(self, campaign_id: str, lead: Dict) -> Dict:
        """Add a lead to a campaign"""
        
        payload = {
            "campaign_id": campaign_id,
            "email": lead["email"],
            "first_name": lead.get("first_name", ""),
            "last_name": lead.get("last_name", ""),
            "company_name": lead.get("company_name", ""),
            "personalization": lead.get("title", ""),
            "custom_variables": lead.get("custom_variables", {})
        }
        
        return self._make_request("POST", "lead/add", payload)
    
    def add_leads_bulk(self, campaign_id: str, leads: List[Dict]) -> Dict:
        """Add multiple leads to a campaign"""
        
        formatted_leads = []
        for lead in leads:
            formatted_leads.append({
                "email": lead["email"],
                "first_name": lead.get("first_name", ""),
                "last_name": lead.get("last_name", ""),
                "company_name": lead.get("company_name", ""),
                "personalization": lead.get("title", ""),
                "custom_variables": lead.get("custom_variables", {})
            })
        
        payload = {
            "campaign_id": campaign_id,
            "leads": formatted_leads
        }
        
        return self._make_request("POST", "lead/add", payload)
    
    def get_lead_status(self, email: str) -> Dict:
        """Get lead status"""
        return self._make_request("GET", f"lead/get?email={email}")


# ============================================================================
# PUSH PIPELINE
# ============================================================================

class InstantlyPushPipeline:
    """Pipeline to push leads to Instantly.ai"""
    
    def __init__(self, api_key: str = None, demo_mode: bool = False):
        self.demo_mode = demo_mode
        self.client = None
        self.push_log = []
        
        if not demo_mode:
            if not api_key:
                raise ValueError("INSTANTLY_API_KEY required for live mode")
            if not HAS_REQUESTS:
                raise ImportError("requests library required: pip install requests")
            self.client = InstantlyClient(api_key)
    
    def push_lead(self, lead: Dict, campaign_id: str) -> Dict:
        """Push a single lead to Instantly"""
        
        if self.demo_mode:
            # Simulate successful push
            result = {
                "success": True,
                "email": lead["email"],
                "campaign_id": campaign_id,
                "instantly_lead_id": f"demo_lead_{lead['email'].split('@')[0]}",
                "status": "added"
            }
            self.push_log.append(result)
            return result
        
        # Live API call
        response = self.client.add_lead(campaign_id, lead)
        
        result = {
            "success": "error" not in response,
            "email": lead["email"],
            "campaign_id": campaign_id,
            "response": response
        }
        self.push_log.append(result)
        return result
    
    def run(self, leads: List[Dict], campaign_mapping: Dict[str, str] = None) -> Dict:
        """Run push pipeline for all leads"""
        
        if campaign_mapping is None:
            campaign_mapping = CAMPAIGNS
        
        logger.info(f"Starting push for {len(leads)} leads...")
        logger.info(f"Mode: {'DEMO' if self.demo_mode else 'LIVE'}")
        
        results = {
            "total": len(leads),
            "success": 0,
            "failed": 0,
            "by_campaign": {}
        }
        
        for i, lead in enumerate(leads):
            persona = lead.get("persona", "superintendent")
            campaign_id = campaign_mapping.get(persona)
            
            if not campaign_id:
                logger.warning(f"No campaign for persona: {persona}")
                results["failed"] += 1
                continue
            
            logger.info(f"[{i+1}/{len(leads)}] Pushing {lead['email']} to {persona} campaign...")
            
            result = self.push_lead(lead, campaign_id)
            
            if result["success"]:
                results["success"] += 1
                results["by_campaign"][campaign_id] = results["by_campaign"].get(campaign_id, 0) + 1
            else:
                results["failed"] += 1
                logger.error(f"Failed to push {lead['email']}: {result}")
        
        # Summary
        logger.info("=" * 50)
        logger.info("PUSH COMPLETE")
        logger.info("=" * 50)
        logger.info(f"Total leads: {results['total']}")
        logger.info(f"Successful: {results['success']}")
        logger.info(f"Failed: {results['failed']}")
        logger.info("By campaign:")
        for campaign, count in results["by_campaign"].items():
            logger.info(f"  - {campaign}: {count} leads")
        
        return results


# ============================================================================
# CLI HELPERS
# ============================================================================

def load_leads_from_csv(input_path: str) -> List[Dict]:
    """Load leads from enriched CSV"""
    
    leads = []
    with open(input_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip rows without email
            if not row.get("email"):
                continue
            
            leads.append({
                "email": row["email"],
                "first_name": row.get("first_name", ""),
                "last_name": row.get("last_name", ""),
                "company_name": row.get("district_name", row.get("company_name", "")),
                "title": row.get("title", ""),
                "persona": row.get("persona", "superintendent"),
                "custom_variables": {
                    "enrollment": row.get("enrollment", ""),
                    "city": row.get("city", ""),
                    "district_name": row.get("district_name", ""),
                }
            })
    
    return leads


def save_push_log(log: List[Dict], output_path: str):
    """Save push log to JSON"""
    
    with open(output_path, "w") as f:
        json.dump(log, f, indent=2)
    
    logger.info(f"Push log saved to {output_path}")


# ============================================================================
# DEMO OUTPUT
# ============================================================================

def generate_demo_output():
    """Generate demo output showing the pipeline in action"""
    
    print("=" * 70)
    print("INSTANTLY.AI PUSH PIPELINE - DEMO MODE")
    print("=" * 70)
    print()
    
    pipeline = InstantlyPushPipeline(demo_mode=True)
    
    print("📋 Leads to push:")
    print("-" * 70)
    for lead in DEMO_LEADS[:5]:
        print(f"  {lead['first_name']} {lead['last_name']}")
        print(f"    📧 {lead['email']}")
        print(f"    🏢 {lead['company_name']} ({lead['title']})")
        print(f"    🎯 Campaign: {lead['persona']}")
        print()
    
    print("🚀 Pushing leads to Instantly.ai...")
    print("-" * 70)
    
    results = pipeline.run(DEMO_LEADS)
    
    print()
    print("✅ PUSH COMPLETE")
    print("-" * 70)
    print(f"  Total: {results['total']}")
    print(f"  Success: {results['success']}")
    print(f"  Failed: {results['failed']}")
    print()
    
    print("📊 Push Log Sample:")
    print("-" * 70)
    for log in pipeline.push_log[:3]:
        print(f"  ✓ {log['email']} → {log['campaign_id']}")
        print(f"    ID: {log['instantly_lead_id']}")
    
    print()
    print("=" * 70)
    print("Demo complete! In live mode, leads would be pushed to Instantly.ai")
    print("=" * 70)
    
    return results


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Push leads to Instantly.ai")
    parser.add_argument("--demo", action="store_true", help="Run in demo mode")
    parser.add_argument("--input", help="Input CSV with enriched leads")
    parser.add_argument("--campaign", help="Campaign ID (overrides persona mapping)")
    parser.add_argument("--log", default="push_log.json", help="Output log file")
    
    args = parser.parse_args()
    
    if args.demo:
        generate_demo_output()
        return
    
    # Get API key
    api_key = os.environ.get("INSTANTLY_API_KEY")
    if not api_key:
        print("Error: INSTANTLY_API_KEY environment variable required")
        return
    
    # Load leads
    if not args.input:
        print("Error: --input required for live mode")
        return
    
    leads = load_leads_from_csv(args.input)
    
    # Campaign mapping
    campaign_mapping = CAMPAIGNS
    if args.campaign:
        # Override with single campaign for all leads
        campaign_mapping = {
            "superintendent": args.campaign,
            "safety_director": args.campaign,
        }
    
    # Run pipeline
    pipeline = InstantlyPushPipeline(api_key=api_key)
    results = pipeline.run(leads, campaign_mapping)
    
    # Save log
    save_push_log(pipeline.push_log, args.log)
    
    print("\n✅ Push complete!")


if __name__ == "__main__":
    main()
