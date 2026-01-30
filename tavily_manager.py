from typing import List, Dict
import json
from tavily import TavilyClient

class TavilyManager:
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("TAVILY_API_KEY not set")
        self._client = TavilyClient(api_key)

    def search_sync(
        self,
        technologies: str,
        city: str,
        max_results: int = 5,
        search_depth: str = "advanced",
    ) -> List[Dict]:
        # The standard TavilyClient.search is synchronous (blocking)
        query = f"internship {technologies} in {city}"
        response = self._client.search(
            query=query,
            search_depth=search_depth,
            max_results=max_results,
        )
        # Tavily returns a dict: {"query": "...", "results": [...], "images": [...]}
        return response.get("results", [])