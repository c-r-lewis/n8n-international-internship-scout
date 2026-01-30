"""
Eurostat City Quality of Life MCP Server

This MCP server provides tools to analyze European city quality of life data
from the Eurostat database.
"""

import json
import logging
from pathlib import Path
from fastmcp import FastMCP
from eurostat_data_manager import EurostatDataManager
from tavily_manager import TavilyManager
from dotenv import load_dotenv
import os


# Configure logging
logger = logging.getLogger(__name__)

# Initialize MCP server
app = FastMCP("eurostat-city-quality")

# SQLite database configuration
DATA_DIR = Path("./eurostat_data")
DB_PATH = DATA_DIR / "cities.db"
DATA_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv()
TAVILY_API_KEY=os.getenv("TAVILY_API_KEY")

# Initialize data manager
data_manager = EurostatDataManager(DB_PATH)
data_manager.ingest_csv("./ressources/worldcities.csv")
tavily = TavilyManager(TAVILY_API_KEY)

@app.tool()
async def search_offers(city: str, technology: str, max_results: int = 5) -> str:
    """Search internship offers on the web using Tavily."""
    # Since we are in an async tool, we call the sync search method
    # It is safe to call it without await if it's a standard function
    results = tavily.search_sync(technology, city, max_results)
    
    offers = []
    for r in results:
        offers.append({
            "title": r.get("title", "No Title"),
            "url": r.get("url", "#"),
            "content": r.get("content", ""),
            "city": city,
            "technology": technology,
            "score": r.get("score", 0),
        })
    
    return json.dumps(offers)
@app.tool()
def get_city_quality_data(city_code: str) -> str:
    """Get quality of life indicators for a specific European city."""
    data = data_manager.get_city_data(city_code)
    return json.dumps(data)

@app.tool()
def rank_cities(weights: dict, limit: int = 10) -> str:
    """Rank European cities based on quality of life criteria."""
    rankings = data_manager.rank_cities(weights, limit)
    return json.dumps(rankings)

@app.tool()
def get_cities_by_country(country: str) -> str:
    """
    Get a list of all cities/towns in a specific country.
    Returns a JSON list of city names, or 'null' if the country is not found.
    """
    cities = data_manager.get_cities_by_country(country)
    return json.dumps(cities)

@app.tool()
def get_criteria_list() -> str:
    """Returns the list of available quality of life criteria codes and names."""
    return json.dumps(data_manager.list_available_indicators())

@app.tool()
def rank_towns_by_country(weights: dict, country: str = None, country_code: str = None, limit: int = 5) -> str:
    """Rank towns in a specific country based on weighted criteria.
    
    Args:
        weights: Dict of {indicator_code: weight, ...}
        country: Full country name like 'France' (or country_code)
        country_code: ISO2 code like 'FR' (preferred for Eurostat)
        limit: Top N towns (default 5)
    """
    if country_code:
        country_name = country_code
    elif country:
        country_name = country
    else:
        return json.dumps({"error": "Must provide country or country_code"})
    
    return json.dumps(data_manager.rank_cities_advanced(weights, country_name, limit))


@app.tool()
async def download_eurostat_data(dataset_code: str = "urt_ce_gj") -> str:
    """
    Downloads and stores Eurostat city data. 
    Use 'urt_ce_gj' for general city quality of life indicators.
    """
    try:
        dataset = await data_manager.download_dataset(dataset_code)
        data_manager.store_data(dataset)
        return f"Successfully downloaded and ingested dataset: {dataset_code}"
    except Exception as e:
        return f"Error downloading dataset: {str(e)}"

if __name__ == "__main__":
    # Run as an HTTP server for n8n to connect to
    app.run(transport="http", host="0.0.0.0", port=3000)