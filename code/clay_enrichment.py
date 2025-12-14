#!/usr/bin/env python3
"""
Clay API Enrichment Pipeline
=============================

Enriches Texas school districts with Superintendent and Safety Director contacts
using Clay's API.

Features:
- Find people at company by domain
- Filter by title (Superintendent, Safety Director, COO)
- Enrich with email, phone, LinkedIn
- Save to PostgreSQL or CSV

Usage:
    # Demo mode (no API calls, uses sample data)
    python clay_enrichment.py --demo
    
    # Live mode (requires CLAY_API_KEY)
    python clay_enrichment.py --input districts.csv --output enriched_leads.csv

Environment Variables:
    CLAY_API_KEY - Your Clay API key
    DATABASE_URL - PostgreSQL connection string (optional)

"""

import os
import json
import time
import argparse
import logging
from datetime import datetime
from typing import List, Dict, Optional
import csv

# Optional imports
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    import psycopg2
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

CLAY_API_BASE = "https://api.clay.com/v1"
RATE_LIMIT_DELAY = 1.0  # seconds between API calls

# Titles we're looking for
TARGET_TITLES = [
    "Superintendent",
    "Director of Safety",
    "Chief of Safety", 
    "Director of Security",
    "Chief Operations Officer",
    "COO",
    "Assistant Superintendent",
    "Chief of Police",
    "Director of Student Safety",
]

# ============================================================================
# DEMO DATA
# ============================================================================

DEMO_ENRICHED_LEADS = [
    {
        "district_name": "Leander ISD",
        "domain": "leanderisd.org",
        "enrollment": 42000,
        "contacts": [
            {
                "full_name": "Dr. Bruce Gearing",
                "first_name": "Bruce",
                "last_name": "Gearing",
                "title": "Superintendent",
                "email": "bruce.gearing@leanderisd.org",
                "phone": "(512) 570-0000",
                "linkedin_url": "https://linkedin.com/in/bruce-gearing",
                "persona": "superintendent"
            },
            {
                "full_name": "Shā Rogers",
                "first_name": "Shā",
                "last_name": "Rogers",
                "title": "Chief of Safety & Security",
                "email": "sha.rogers@leanderisd.org",
                "phone": "(512) 570-0024",
                "linkedin_url": "https://linkedin.com/in/sha-rogers",
                "persona": "safety_director"
            }
        ]
    },
    {
        "district_name": "Frisco ISD",
        "domain": "friscoisd.org",
        "enrollment": 67000,
        "contacts": [
            {
                "full_name": "Dr. Mike Waldrip",
                "first_name": "Mike",
                "last_name": "Waldrip",
                "title": "Superintendent",
                "email": "mike.waldrip@friscoisd.org",
                "phone": "(469) 633-6000",
                "linkedin_url": "https://linkedin.com/in/mike-waldrip",
                "persona": "superintendent"
            }
        ]
    },
    {
        "district_name": "Keller ISD",
        "domain": "kellerisd.net",
        "enrollment": 34000,
        "contacts": [
            {
                "full_name": "Dr. Rick Westfall",
                "first_name": "Rick",
                "last_name": "Westfall",
                "title": "Superintendent",
                "email": "rick.westfall@kellerisd.net",
                "phone": "(817) 744-1000",
                "linkedin_url": "https://linkedin.com/in/rick-westfall",
                "persona": "superintendent"
            }
        ]
    },
    {
        "district_name": "Georgetown ISD",
        "domain": "georgetownisd.org", 
        "enrollment": 14000,
        "contacts": [
            {
                "full_name": "Dr. Fred Brent",
                "first_name": "Fred",
                "last_name": "Brent",
                "title": "Superintendent",
                "email": "fred.brent@georgetownisd.org",
                "phone": "(512) 943-5000",
                "linkedin_url": "https://linkedin.com/in/fred-brent",
                "persona": "superintendent"
            }
        ]
    },
    {
        "district_name": "Round Rock ISD",
        "domain": "roundrockisd.org",
        "enrollment": 47000,
        "contacts": [
            {
                "full_name": "Dr. Hafedh Azaiez",
                "first_name": "Hafedh",
                "last_name": "Azaiez",
                "title": "Superintendent",
                "email": "hafedh_azaiez@roundrockisd.org",
                "phone": "(512) 464-5000",
                "linkedin_url": "https://linkedin.com/in/hafedh-azaiez",
                "persona": "superintendent"
            }
        ]
    },
    {
        "district_name": "Humble ISD",
        "domain": "humbleisd.net",
        "enrollment": 47000,
        "contacts": [
            {
                "full_name": "Dr. Elizabeth Fagen",
                "first_name": "Elizabeth",
                "last_name": "Fagen",
                "title": "Superintendent",
                "email": "elizabeth.fagen@humbleisd.net",
                "phone": "(281) 641-1000",
                "linkedin_url": "https://linkedin.com/in/elizabeth-fagen",
                "persona": "superintendent"
            }
        ]
    },
    {
        "district_name": "Prosper ISD",
        "domain": "prosper-isd.net",
        "enrollment": 30000,
        "contacts": [
            {
                "full_name": "Dr. Holly Ferguson",
                "first_name": "Holly",
                "last_name": "Ferguson",
                "title": "Superintendent",
                "email": "holly.ferguson@prosper-isd.net",
                "phone": "(469) 219-2000",
                "linkedin_url": "https://linkedin.com/in/holly-ferguson",
                "persona": "superintendent"
            }
        ]
    },
    {
        "district_name": "Lake Travis ISD",
        "domain": "ltisdschools.org",
        "enrollment": 12000,
        "contacts": [
            {
                "full_name": "Dr. Paul Norton",
                "first_name": "Paul",
                "last_name": "Norton",
                "title": "Superintendent",
                "email": "paul.norton@ltisdschools.org",
                "phone": "(512) 533-6000",
                "linkedin_url": "https://linkedin.com/in/paul-norton",
                "persona": "superintendent"
            }
        ]
    },
    {
        "district_name": "Boerne ISD",
        "domain": "boerneisd.net",
        "enrollment": 10000,
        "contacts": [
            {
                "full_name": "Dr. Thomas Price",
                "first_name": "Thomas",
                "last_name": "Price",
                "title": "Superintendent",
                "email": "thomas.price@boerneisd.net",
                "phone": "(830) 357-2000",
                "linkedin_url": "https://linkedin.com/in/thomas-price",
                "persona": "superintendent"
            }
        ]
    },
    {
        "district_name": "Aledo ISD",
        "domain": "aledoisd.org",
        "enrollment": 8400,
        "contacts": [
            {
                "full_name": "Dr. Susan Bohn",
                "first_name": "Susan",
                "last_name": "Bohn",
                "title": "Superintendent",
                "email": "susan.bohn@aledoisd.org",
                "phone": "(817) 441-5327",
                "linkedin_url": "https://linkedin.com/in/susan-bohn",
                "persona": "superintendent"
            }
        ]
    }
]

# ============================================================================
# CLAY API CLIENT
# ============================================================================

class ClayClient:
    """Client for Clay API interactions"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })
    
    def find_people(self, domain: str, titles: List[str] = None) -> List[Dict]:
        """Find people at a company by domain"""
        
        endpoint = f"{CLAY_API_BASE}/people/search"
        
        payload = {
            "domain": domain,
            "title_keywords": titles or TARGET_TITLES,
            "limit": 10
        }
        
        try:
            time.sleep(RATE_LIMIT_DELAY)
            response = self.session.post(endpoint, json=payload)
            response.raise_for_status()
            
            data = response.json()
            return data.get("people", [])
            
        except Exception as e:
            logger.error(f"Error finding people at {domain}: {e}")
            return []
    
    def enrich_person(self, person: Dict) -> Dict:
        """Enrich a person with email, phone, LinkedIn"""
        
        endpoint = f"{CLAY_API_BASE}/people/enrich"
        
        payload = {
            "first_name": person.get("first_name"),
            "last_name": person.get("last_name"),
            "company_domain": person.get("domain"),
            "title": person.get("title")
        }
        
        try:
            time.sleep(RATE_LIMIT_DELAY)
            response = self.session.post(endpoint, json=payload)
            response.raise_for_status()
            
            data = response.json()
            
            # Merge enrichment data
            person["email"] = data.get("email")
            person["phone"] = data.get("phone")
            person["linkedin_url"] = data.get("linkedin_url")
            
            return person
            
        except Exception as e:
            logger.error(f"Error enriching {person.get('full_name')}: {e}")
            return person
    
    def find_email(self, person: Dict) -> Optional[str]:
        """Find email for a person"""
        
        endpoint = f"{CLAY_API_BASE}/email/find"
        
        payload = {
            "first_name": person.get("first_name"),
            "last_name": person.get("last_name"),
            "domain": person.get("domain")
        }
        
        try:
            time.sleep(RATE_LIMIT_DELAY)
            response = self.session.post(endpoint, json=payload)
            response.raise_for_status()
            
            data = response.json()
            return data.get("email")
            
        except Exception as e:
            logger.error(f"Error finding email: {e}")
            return None


# ============================================================================
# ENRICHMENT PIPELINE
# ============================================================================

class EnrichmentPipeline:
    """Main enrichment pipeline"""
    
    def __init__(self, api_key: str = None, demo_mode: bool = False):
        self.demo_mode = demo_mode
        self.client = None
        
        if not demo_mode:
            if not api_key:
                raise ValueError("CLAY_API_KEY required for live mode")
            if not HAS_REQUESTS:
                raise ImportError("requests library required: pip install requests")
            self.client = ClayClient(api_key)
    
    def enrich_district(self, district: Dict) -> Dict:
        """Enrich a single district with contacts"""
        
        domain = district.get("domain")
        if not domain:
            logger.warning(f"No domain for {district.get('district_name')}")
            return district
        
        logger.info(f"Enriching {district.get('district_name')} ({domain})...")
        
        if self.demo_mode:
            # Return demo data if available
            for demo in DEMO_ENRICHED_LEADS:
                if demo["domain"] == domain:
                    district["contacts"] = demo["contacts"]
                    return district
            
            # Generate placeholder if not in demo data
            district["contacts"] = []
            return district
        
        # Live API calls
        people = self.client.find_people(domain)
        
        contacts = []
        for person in people:
            # Classify persona based on title
            title = person.get("title", "").lower()
            if "superintendent" in title:
                persona = "superintendent"
            elif any(x in title for x in ["safety", "security", "police"]):
                persona = "safety_director"
            elif any(x in title for x in ["coo", "operations"]):
                persona = "coo"
            else:
                persona = "other"
            
            # Enrich with contact info
            enriched = self.client.enrich_person(person)
            enriched["persona"] = persona
            
            contacts.append(enriched)
        
        district["contacts"] = contacts
        return district
    
    def run(self, districts: List[Dict]) -> List[Dict]:
        """Run enrichment on all districts"""
        
        logger.info(f"Starting enrichment for {len(districts)} districts...")
        logger.info(f"Mode: {'DEMO' if self.demo_mode else 'LIVE'}")
        
        enriched = []
        for i, district in enumerate(districts):
            logger.info(f"[{i+1}/{len(districts)}] Processing {district.get('district_name')}...")
            
            result = self.enrich_district(district)
            enriched.append(result)
            
            # Progress update
            if (i + 1) % 10 == 0:
                logger.info(f"Progress: {i+1}/{len(districts)} districts enriched")
        
        # Summary
        total_contacts = sum(len(d.get("contacts", [])) for d in enriched)
        superintendents = sum(1 for d in enriched for c in d.get("contacts", []) if c.get("persona") == "superintendent")
        safety_directors = sum(1 for d in enriched for c in d.get("contacts", []) if c.get("persona") == "safety_director")
        
        logger.info("=" * 50)
        logger.info("ENRICHMENT COMPLETE")
        logger.info("=" * 50)
        logger.info(f"Districts processed: {len(enriched)}")
        logger.info(f"Total contacts found: {total_contacts}")
        logger.info(f"  - Superintendents: {superintendents}")
        logger.info(f"  - Safety Directors: {safety_directors}")
        
        return enriched


# ============================================================================
# OUTPUT HANDLERS
# ============================================================================

def save_to_csv(enriched: List[Dict], output_path: str):
    """Save enriched leads to CSV"""
    
    rows = []
    for district in enriched:
        for contact in district.get("contacts", []):
            rows.append({
                "district_name": district.get("district_name"),
                "domain": district.get("domain"),
                "enrollment": district.get("enrollment"),
                "full_name": contact.get("full_name"),
                "first_name": contact.get("first_name"),
                "last_name": contact.get("last_name"),
                "title": contact.get("title"),
                "email": contact.get("email"),
                "phone": contact.get("phone"),
                "linkedin_url": contact.get("linkedin_url"),
                "persona": contact.get("persona"),
            })
    
    if not rows:
        logger.warning("No contacts to save")
        return
    
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    
    logger.info(f"Saved {len(rows)} contacts to {output_path}")


def save_to_json(enriched: List[Dict], output_path: str):
    """Save enriched leads to JSON"""
    
    with open(output_path, "w") as f:
        json.dump(enriched, f, indent=2)
    
    logger.info(f"Saved to {output_path}")


def save_to_postgres(enriched: List[Dict], database_url: str):
    """Save enriched leads to PostgreSQL"""
    
    if not HAS_POSTGRES:
        logger.error("psycopg2 required: pip install psycopg2-binary")
        return
    
    conn = psycopg2.connect(database_url)
    cur = conn.cursor()
    
    try:
        for district in enriched:
            # Insert or update district
            cur.execute("""
                INSERT INTO districts (district_name, domain, enrollment)
                VALUES (%s, %s, %s)
                ON CONFLICT (domain) DO UPDATE SET
                    district_name = EXCLUDED.district_name,
                    enrollment = EXCLUDED.enrollment
                RETURNING id
            """, (district.get("district_name"), district.get("domain"), district.get("enrollment")))
            
            district_id = cur.fetchone()[0]
            
            # Insert contacts
            for contact in district.get("contacts", []):
                cur.execute("""
                    INSERT INTO leads (
                        district_id, full_name, first_name, last_name,
                        title, email, phone, linkedin_url, persona, enriched_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (email) DO UPDATE SET
                        title = EXCLUDED.title,
                        phone = EXCLUDED.phone,
                        linkedin_url = EXCLUDED.linkedin_url
                """, (
                    district_id,
                    contact.get("full_name"),
                    contact.get("first_name"),
                    contact.get("last_name"),
                    contact.get("title"),
                    contact.get("email"),
                    contact.get("phone"),
                    contact.get("linkedin_url"),
                    contact.get("persona")
                ))
        
        conn.commit()
        logger.info(f"Saved {len(enriched)} districts to PostgreSQL")
        
    finally:
        cur.close()
        conn.close()


# ============================================================================
# CLI
# ============================================================================

def load_districts_from_csv(input_path: str) -> List[Dict]:
    """Load districts from CSV file"""
    
    districts = []
    with open(input_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            districts.append({
                "district_name": row.get("district_name") or row.get("name"),
                "domain": row.get("domain"),
                "enrollment": int(row.get("enrollment", 0) or 0),
                "city": row.get("city"),
            })
    
    return districts


def main():
    parser = argparse.ArgumentParser(description="Clay API Enrichment Pipeline")
    parser.add_argument("--demo", action="store_true", help="Run in demo mode (no API calls)")
    parser.add_argument("--input", help="Input CSV file with districts")
    parser.add_argument("--output", default="enriched_leads.csv", help="Output CSV file")
    parser.add_argument("--json", help="Also save to JSON file")
    parser.add_argument("--database", help="PostgreSQL connection URL")
    
    args = parser.parse_args()
    
    # Get API key from environment
    api_key = os.environ.get("CLAY_API_KEY")
    
    # Load districts
    if args.input:
        districts = load_districts_from_csv(args.input)
    elif args.demo:
        # Use demo districts
        districts = [
            {"district_name": d["district_name"], "domain": d["domain"], "enrollment": d["enrollment"]}
            for d in DEMO_ENRICHED_LEADS
        ]
    else:
        print("Error: --input required for live mode, or use --demo")
        return
    
    # Run pipeline
    pipeline = EnrichmentPipeline(api_key=api_key, demo_mode=args.demo)
    enriched = pipeline.run(districts)
    
    # Save outputs
    save_to_csv(enriched, args.output)
    
    if args.json:
        save_to_json(enriched, args.json)
    
    if args.database:
        save_to_postgres(enriched, args.database)
    
    print("\n✅ Enrichment complete!")
    print(f"   Output: {args.output}")


if __name__ == "__main__":
    main()
