#!/usr/bin/env python3
"""
Texas School District Scraper - ALL DISTRICTS
==============================================

Scrapes ALL Texas school districts with website domains.
Output is designed for Clay import for enrichment.

Data Sources:
1. Texas Tribune Public Schools Explorer API
2. NCES (National Center for Education Statistics)
3. Wikipedia list of Texas school districts

Usage:
    pip install requests beautifulsoup4 pandas lxml
    python texas_districts_all.py

Output:
    - texas_districts_all.csv (for Clay import)
    - texas_districts_all.json (full data)
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import re
import os
import time
from datetime import datetime
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIG
# ============================================================================

OUTPUT_DIR = "./output"
RATE_LIMIT = 0.5  # seconds between requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ============================================================================
# DATA SOURCES
# ============================================================================

class TexasTribuneScaper:
    """
    Scrapes Texas Tribune Schools Explorer
    They have data on all Texas public school districts
    """
    
    # Texas Tribune has a searchable district list
    BASE_URL = "https://schools.texastribune.org"
    DISTRICTS_URL = "https://schools.texastribune.org/districts/"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    def get_all_districts(self) -> List[Dict]:
        """Get all Texas districts from Tribune"""
        logger.info("Scraping Texas Tribune for all districts...")
        districts = []
        
        # They paginate by letter A-Z
        for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
            url = f"{self.DISTRICTS_URL}?letter={letter}"
            logger.info(f"  Fetching districts starting with {letter}...")
            
            try:
                time.sleep(RATE_LIMIT)
                resp = self.session.get(url, timeout=30)
                if resp.status_code != 200:
                    continue
                    
                soup = BeautifulSoup(resp.text, "lxml")
                
                # Find district links
                links = soup.select("a[href*='/districts/']")
                
                for link in links:
                    href = link.get("href", "")
                    name = link.get_text(strip=True)
                    
                    # Skip navigation links
                    if not name or name in ["Districts", "Schools", "?"]:
                        continue
                    if "/districts/" not in href:
                        continue
                    
                    # Extract slug from URL
                    slug = href.split("/districts/")[-1].strip("/")
                    if not slug or slug == "":
                        continue
                    
                    districts.append({
                        "name": name,
                        "slug": slug,
                        "tribune_url": f"{self.BASE_URL}{href}",
                    })
                    
            except Exception as e:
                logger.error(f"Error fetching letter {letter}: {e}")
        
        # Deduplicate
        seen = set()
        unique = []
        for d in districts:
            if d["name"] not in seen:
                seen.add(d["name"])
                unique.append(d)
        
        logger.info(f"Found {len(unique)} districts from Tribune")
        return unique
    
    def enrich_district(self, district: Dict) -> Dict:
        """Get additional details from district page"""
        url = district.get("tribune_url", "")
        if not url:
            return district
        
        try:
            time.sleep(RATE_LIMIT)
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200:
                return district
            
            soup = BeautifulSoup(resp.text, "lxml")
            text = soup.get_text()
            
            # Find enrollment
            enrollment_match = re.search(r"([\d,]+)\s*students", text, re.I)
            if enrollment_match:
                district["enrollment"] = int(enrollment_match.group(1).replace(",", ""))
            
            # Find website link
            website_link = soup.select_one("a[href*='http'][target='_blank']")
            if website_link:
                href = website_link.get("href", "")
                if "texastribune" not in href and "facebook" not in href:
                    district["website"] = href
            
            # Find location
            location_elem = soup.select_one(".location, [class*='location']")
            if location_elem:
                district["city"] = location_elem.get_text(strip=True)
                
        except Exception as e:
            logger.debug(f"Error enriching {district['name']}: {e}")
        
        return district


class NCESscraper:
    """
    Scrapes NCES (National Center for Education Statistics)
    Official federal database of all US school districts
    """
    
    SEARCH_URL = "https://nces.ed.gov/ccd/districtsearch/"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    def get_texas_districts(self) -> List[Dict]:
        """Get Texas districts from NCES"""
        logger.info("Scraping NCES for Texas districts...")
        
        # NCES search parameters
        params = {
            "State": "48",  # Texas FIPS code
            "BasicPageNum": "1",
            "NumSearchResults": "1500",  # Get all
        }
        
        districts = []
        
        try:
            time.sleep(RATE_LIMIT)
            resp = self.session.get(self.SEARCH_URL, params=params, timeout=30)
            soup = BeautifulSoup(resp.text, "lxml")
            
            # Find results table
            rows = soup.select("table tr")
            
            for row in rows[1:]:  # Skip header
                cells = row.select("td")
                if len(cells) >= 3:
                    name = cells[0].get_text(strip=True)
                    city = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    
                    if name and "ISD" in name or "CISD" in name or "School" in name:
                        districts.append({
                            "name": name,
                            "city": city,
                            "source": "NCES"
                        })
                        
        except Exception as e:
            logger.error(f"Error fetching NCES: {e}")
        
        logger.info(f"Found {len(districts)} districts from NCES")
        return districts


class WikipediaScraper:
    """
    Scrapes Wikipedia list of Texas school districts
    Good backup source with website links
    """
    
    URLS = [
        "https://en.wikipedia.org/wiki/List_of_school_districts_in_Texas",
    ]
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    def get_districts(self) -> List[Dict]:
        """Get districts from Wikipedia"""
        logger.info("Scraping Wikipedia for Texas districts...")
        districts = []
        
        for url in self.URLS:
            try:
                time.sleep(RATE_LIMIT)
                resp = self.session.get(url, timeout=30)
                soup = BeautifulSoup(resp.text, "lxml")
                
                # Find all links that look like school districts
                links = soup.select("a[href*='/wiki/']")
                
                for link in links:
                    text = link.get_text(strip=True)
                    
                    # Filter for school district names
                    if any(x in text for x in ["ISD", "CISD", "Independent School District", "Consolidated"]):
                        # Skip disambiguation pages
                        if "disambiguation" in link.get("href", "").lower():
                            continue
                            
                        districts.append({
                            "name": text,
                            "wikipedia_url": "https://en.wikipedia.org" + link.get("href", ""),
                            "source": "Wikipedia"
                        })
                        
            except Exception as e:
                logger.error(f"Error fetching Wikipedia: {e}")
        
        # Deduplicate
        seen = set()
        unique = []
        for d in districts:
            name = d["name"]
            if name not in seen:
                seen.add(name)
                unique.append(d)
        
        logger.info(f"Found {len(unique)} districts from Wikipedia")
        return unique


# ============================================================================
# DOMAIN FINDER
# ============================================================================

class DomainFinder:
    """Finds website domains for school districts"""
    
    # Common domain patterns for Texas ISDs
    DOMAIN_PATTERNS = [
        "{slug}.org",
        "{slug}.net", 
        "{slug}.us",
        "www.{slug}.org",
        "www.{slug}.net",
        "{slug}schools.org",
        "{slug}schools.net",
    ]
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
        # Known domain mappings (verified)
        self.known_domains = {
            "Frisco ISD": "friscoisd.org",
            "Leander ISD": "leanderisd.org",
            "Round Rock ISD": "roundrockisd.org",
            "Keller ISD": "kellerisd.net",
            "Humble ISD": "humbleisd.net",
            "Prosper ISD": "prosper-isd.net",
            "Georgetown ISD": "georgetownisd.org",
            "Hays CISD": "hayscisd.net",
            "Aledo ISD": "aledoisd.org",
            "Dripping Springs ISD": "dsisdtx.us",
            "Lake Travis ISD": "ltisdschools.org",
            "Boerne ISD": "boerneisd.net",
            "Plano ISD": "pisd.edu",
            "McKinney ISD": "mckinneyisd.net",
            "Allen ISD": "allenisd.org",
            "Denton ISD": "dentonisd.org",
            "Northwest ISD": "nisdtx.org",
            "Mansfield ISD": "mansfieldisd.org",
            "Coppell ISD": "coppellisd.com",
            "Southlake Carroll ISD": "southlakecarroll.edu",
            "Grapevine-Colleyville ISD": "gcisd.net",
            "Highland Park ISD": "hpisd.org",
            "Eanes ISD": "eanesisd.net",
            "Wylie ISD": "wylieisd.net",
            "Lovejoy ISD": "lovejoyisd.com",
            "Rockwall ISD": "rockwallisd.com",
            "Midlothian ISD": "misd.gs",
            "Forney ISD": "forneyisd.net",
            "Little Elm ISD": "leisd.net",
            "Comal ISD": "comalisd.org",
            "Conroe ISD": "conroeisd.net",
            "Cypress-Fairbanks ISD": "cfisd.net",
            "Spring Branch ISD": "springbranchisd.com",
            "Klein ISD": "kleinisd.net",
            "Tomball ISD": "tomballisd.net",
            "Pearland ISD": "pearlandisd.org",
            "Clear Creek ISD": "ccisd.net",
            "Fort Bend ISD": "fortbendisd.com",
            "Katy ISD": "katyisd.org",
            "Lamar CISD": "lcisd.org",
            "Pasadena ISD": "pasadenaisd.org",
            "Spring ISD": "springisd.org",
            "Aldine ISD": "aldineisd.org",
            "Houston ISD": "houstonisd.org",
            "Dallas ISD": "dallasisd.org",
            "Fort Worth ISD": "fwisd.org",
            "Austin ISD": "austinisd.org",
            "San Antonio ISD": "saisd.net",
            "Arlington ISD": "aisd.net",
            "Garland ISD": "garlandisd.net",
            "Irving ISD": "irvingisd.net",
            "Mesquite ISD": "mesquiteisd.org",
            "Richardson ISD": "risd.org",
            "Carrollton-Farmers Branch ISD": "cfbisd.edu",
            "Lewisville ISD": "lisd.net",
            "Birdville ISD": "birdvilleschools.net",
            "Crowley ISD": "crowleyisdtx.org",
            "Eagle Mountain-Saginaw ISD": "emsisd.com",
            "Hurst-Euless-Bedford ISD": "hebisd.edu",
            "Northwest ISD": "nisdtx.org",
            "Waxahachie ISD": "wisd.org",
            "Weatherford ISD": "weatherfordisd.com",
            "Burleson ISD": "burleson.k12.tx.us",
            "Joshua ISD": "joshuaisd.org",
            "Cleburne ISD": "c-isd.com",
            "Granbury ISD": "granburyisd.org",
            "New Braunfels ISD": "nbisd.org",
            "Schertz-Cibolo-Universal City ISD": "scuc.txed.net",
            "Judson ISD": "judsonisd.org",
            "North East ISD": "neisd.net",
            "Northside ISD": "nisd.net",
            "San Marcos CISD": "smcisd.net",
            "Pflugerville ISD": "pfisd.net",
            "Manor ISD": "manorisd.net",
            "Del Valle ISD": "dvisd.net",
            "Cedar Park": "leanderisd.org",  # Part of Leander
            "Bastrop ISD": "bfrisk.org",
            "Lockhart ISD": "lockhartisd.org",
            "Seguin ISD": "seguinisd.net",
            "Killeen ISD": "killeenisd.org",
            "Temple ISD": "tisd.org",
            "Belton ISD": "bisd.net",
            "Waco ISD": "wacoisd.org",
            "Midway ISD": "midwayisd.org",
            "Bryan ISD": "bryanisd.org",
            "College Station ISD": "csisd.org",
            "Tyler ISD": "tylerisd.org",
            "Longview ISD": "lisd.org",
            "Nacogdoches ISD": "nacisd.org",
            "Lufkin ISD": "lufkinisd.org",
            "Texarkana ISD": "txkisd.net",
            "Amarillo ISD": "amaisd.org",
            "Lubbock ISD": "lubbockisd.org",
            "Midland ISD": "midlandisd.net",
            "Odessa": "ectorcountyisd.org",
            "Ector County ISD": "ectorcountyisd.org",
            "El Paso ISD": "episd.org",
            "Socorro ISD": "sisd.net",
            "Ysleta ISD": "yisd.net",
            "Corpus Christi ISD": "ccisd.us",
            "Flour Bluff ISD": "flourbluffschools.net",
            "Calallen ISD": "calallen.org",
            "Laredo ISD": "laredoisd.org",
            "United ISD": "uisd.net",
            "McAllen ISD": "mcallenisd.org",
            "Edinburg CISD": "ecisd.us",
            "Pharr-San Juan-Alamo ISD": "psjaisd.us",
            "Brownsville ISD": "bisd.us",
            "Harlingen CISD": "hcisd.org",
        }
    
    def find_domain(self, district_name: str) -> str:
        """Find domain for a district"""
        
        # Check known domains first
        if district_name in self.known_domains:
            return self.known_domains[district_name]
        
        # Try to construct domain from name
        slug = self._make_slug(district_name)
        
        for pattern in self.DOMAIN_PATTERNS:
            domain = pattern.format(slug=slug)
            if self._check_domain(domain):
                return domain
        
        return ""
    
    def _make_slug(self, name: str) -> str:
        """Convert district name to URL slug"""
        # "Frisco ISD" -> "friscoisd"
        slug = name.lower()
        slug = slug.replace(" independent school district", "isd")
        slug = slug.replace(" consolidated independent school district", "cisd")
        slug = slug.replace(" ", "")
        slug = re.sub(r"[^a-z0-9]", "", slug)
        return slug
    
    def _check_domain(self, domain: str) -> bool:
        """Check if domain exists (quick HEAD request)"""
        try:
            url = f"https://www.{domain}" if not domain.startswith("www.") else f"https://{domain}"
            resp = self.session.head(url, timeout=3, allow_redirects=True)
            return resp.status_code < 400
        except:
            return False


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

class TexasDistrictScraper:
    """Main orchestrator to get all Texas districts"""
    
    def __init__(self):
        self.tribune = TexasTribuneScaper()
        self.wikipedia = WikipediaScraper()
        self.domain_finder = DomainFinder()
    
    def run(self, enrich_all: bool = False) -> List[Dict]:
        """Run full scraping pipeline"""
        
        logger.info("=" * 60)
        logger.info("Texas School District Scraper - ALL DISTRICTS")
        logger.info("=" * 60)
        
        all_districts = {}
        
        # Source 1: Texas Tribune
        try:
            tribune_districts = self.tribune.get_all_districts()
            for d in tribune_districts:
                name = d["name"]
                if name not in all_districts:
                    all_districts[name] = d
                else:
                    all_districts[name].update(d)
        except Exception as e:
            logger.error(f"Tribune scraping failed: {e}")
        
        # Source 2: Wikipedia (backup)
        try:
            wiki_districts = self.wikipedia.get_districts()
            for d in wiki_districts:
                name = d["name"]
                if name not in all_districts:
                    all_districts[name] = d
        except Exception as e:
            logger.error(f"Wikipedia scraping failed: {e}")
        
        # Convert to list
        districts = list(all_districts.values())
        
        logger.info(f"\nTotal unique districts: {len(districts)}")
        
        # Find domains
        logger.info("\nFinding website domains...")
        for i, d in enumerate(districts):
            if i % 50 == 0:
                logger.info(f"  Processing {i}/{len(districts)}...")
            
            # Skip if already has website
            if d.get("website"):
                # Extract domain from URL
                match = re.search(r"https?://(?:www\.)?([^/]+)", d["website"])
                if match:
                    d["domain"] = match.group(1)
                continue
            
            # Find domain
            domain = self.domain_finder.find_domain(d["name"])
            if domain:
                d["domain"] = domain
                d["website"] = f"https://www.{domain}"
        
        # Enrich with Tribune data if requested
        if enrich_all:
            logger.info("\nEnriching with detailed data (this will take a while)...")
            for i, d in enumerate(districts):
                if i % 20 == 0:
                    logger.info(f"  Enriching {i}/{len(districts)}...")
                if d.get("tribune_url"):
                    districts[i] = self.tribune.enrich_district(d)
        
        # Clean up and standardize
        for d in districts:
            d["state"] = "TX"
            d["scraped_at"] = datetime.now().isoformat()
            if "enrollment" not in d:
                d["enrollment"] = 0
        
        # Sort by enrollment (known first)
        districts.sort(key=lambda x: x.get("enrollment", 0), reverse=True)
        
        return districts
    
    def save_outputs(self, districts: List[Dict]):
        """Save to CSV and JSON"""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # JSON (full data)
        with open(f"{OUTPUT_DIR}/texas_districts_all.json", "w") as f:
            json.dump(districts, f, indent=2)
        
        # CSV for Clay import
        df = pd.DataFrame(districts)
        
        # Reorder columns for Clay
        cols = ["name", "domain", "website", "city", "state", "enrollment"]
        cols = [c for c in cols if c in df.columns]
        other_cols = [c for c in df.columns if c not in cols]
        df = df[cols + other_cols]
        
        df.to_csv(f"{OUTPUT_DIR}/texas_districts_all.csv", index=False)
        
        # Also save Clay-optimized version (just essential columns)
        clay_cols = ["name", "domain", "website", "city", "enrollment"]
        clay_cols = [c for c in clay_cols if c in df.columns]
        df_clay = df[clay_cols]
        df_clay.to_csv(f"{OUTPUT_DIR}/texas_districts_for_clay.csv", index=False)
        
        logger.info(f"\nSaved to {OUTPUT_DIR}/")
        logger.info(f"  - texas_districts_all.json ({len(districts)} districts)")
        logger.info(f"  - texas_districts_all.csv")
        logger.info(f"  - texas_districts_for_clay.csv (Clay import ready)")
    
    def print_summary(self, districts: List[Dict]):
        """Print summary stats"""
        total = len(districts)
        with_domain = len([d for d in districts if d.get("domain")])
        with_enrollment = len([d for d in districts if d.get("enrollment", 0) > 0])
        
        # Size buckets
        small = len([d for d in districts if 0 < d.get("enrollment", 0) < 5000])
        medium = len([d for d in districts if 5000 <= d.get("enrollment", 0) < 20000])
        large = len([d for d in districts if 20000 <= d.get("enrollment", 0) < 50000])
        xlarge = len([d for d in districts if d.get("enrollment", 0) >= 50000])
        
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Total districts:     {total}")
        print(f"With domain:         {with_domain} ({100*with_domain//total}%)")
        print(f"With enrollment:     {with_enrollment}")
        print("-" * 60)
        print("BY SIZE (enrollment):")
        print(f"  Small (<5K):       {small}")
        print(f"  Medium (5K-20K):   {medium}  <-- Sweet spot")
        print(f"  Large (20K-50K):   {large}   <-- Sweet spot")
        print(f"  XLarge (50K+):     {xlarge}")
        print("=" * 60)
        
        # Show top 20 by enrollment
        print("\nTOP 20 BY ENROLLMENT:")
        print(f"{'Enrollment':<12} {'District':<35} {'Domain':<25}")
        print("-" * 72)
        
        sorted_districts = sorted(districts, key=lambda x: x.get("enrollment", 0), reverse=True)
        for d in sorted_districts[:20]:
            enrollment = d.get("enrollment", 0)
            name = d.get("name", "")[:33]
            domain = d.get("domain", "")[:23]
            print(f"{enrollment:<12,} {name:<35} {domain:<25}")


# ============================================================================
# CLI
# ============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Scrape ALL Texas school districts")
    parser.add_argument("--enrich", action="store_true", help="Enrich with detailed data (slow)")
    parser.add_argument("--output", default="./output", help="Output directory")
    
    args = parser.parse_args()
    
    global OUTPUT_DIR
    OUTPUT_DIR = args.output
    
    scraper = TexasDistrictScraper()
    districts = scraper.run(enrich_all=args.enrich)
    scraper.save_outputs(districts)
    scraper.print_summary(districts)


if __name__ == "__main__":
    main()
