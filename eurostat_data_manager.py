import sqlite3
import logging
import httpx
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
from contextlib import contextmanager

# Constants
EUROSTAT_API = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EurostatDataManager:
    """Manages downloading, parsing, and querying Eurostat city data."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_db()

    def _init_db(self) -> None:
        """
        Initialize SQLite database schema.
        """
        with self._get_cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cities (
                    city_code TEXT PRIMARY KEY,
                    city_name TEXT,
                    country TEXT,
                    lat REAL,
                    lng REAL,
                    population INTEGER
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS indicators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city_code TEXT,
                    indicator_code TEXT,
                    indicator_name TEXT,
                    year INTEGER,
                    value REAL,
                    status TEXT,
                    FOREIGN KEY (city_code) REFERENCES cities(city_code),
                    UNIQUE(city_code, indicator_code, year)
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_city_indicator 
                ON indicators(city_code, indicator_code, year)
            """)
        self.conn.commit()
        logger.info("Database schema initialized.")

    @contextmanager
    def _get_cursor(self):
        """
        Context manager for database cursor.
        """
        cursor = self.conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def ingest_csv(self, csv_path: str) -> None:
        """
        Ingest worldcities.csv into the database.
        """
        logger.info(f"Ingesting data from {csv_path}...")
        df = pd.read_csv(csv_path)

        # Prepare data for insertion: Use 'id' as city_code and 'city_ascii' as city_name
        city_data = df[['id', 'city_ascii', 'country', 'lat', 'lng', 'population']].values.tolist()

        with self._get_cursor() as cursor:
            cursor.executemany("""
                INSERT OR REPLACE INTO cities (city_code, city_name, country, lat, lng, population)
                VALUES (?, ?, ?, ?, ?, ?)
            """, city_data)

        self.conn.commit()
        logger.info(f"Successfully ingested {len(city_data)} cities.")

    async def download_dataset(self, dataset_code: str) -> dict:
        """Download a dataset from Eurostat API in JSON-STAT format."""
        url = f"{EUROSTAT_API}/data/{dataset_code}"
        params = {"format": "JSON", "lang": "EN"}

        logger.info(f"Downloading dataset {dataset_code}...")
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            logger.info(f"Dataset {dataset_code} downloaded successfully.")
            return response.json()

    def parse_jsonstat(self, data: dict) -> List[Dict[str, Any]]:
        dimensions = data.get('dimension', {})
        values = data.get('value', [])
        status = data.get('status', {}) if 'status' in data else {}
        sizes = data.get('size', [])
        dim_names = data.get('id', [])
        
        # Build category index dynamically
        indices = {}
        for dim_name in dim_names:
            dim_data = dimensions.get(dim_name, {})
            category = dim_data.get('category', {})
            indices[dim_name] = {
                'index': category.get('index', {}),
                'label': category.get('label', {})
            }
        
        records = []
        for idx, value in enumerate(values):  
            coords = self._index_to_coords(idx, sizes)
            record = {}
            for i, dim_name in enumerate(dim_names):
                dim_index = coords[i]
                cat_index = indices[dim_name]['index']
                cat_label = indices[dim_name]['label']
                for key, val in cat_index.items():
                    if val == dim_index:
                        record[dim_name] = key
                        record[f'{dim_name}_label'] = cat_label.get(key, key)
                        break
            record['value'] = value
            record['status'] = status.get(idx, '') if status else ''
            records.append(record)
        
        logger.info(f"Parsed {len(records)} records.")
        return records

    @staticmethod
    def _index_to_coords(index: int, sizes: List[int]) -> List[int]:
        coords = []
        for size in reversed(sizes):
            coords.append(index % size)
            index //= size
        return list(reversed(coords))

    def store_data(self, dataset: dict) -> None:
        records = self.parse_jsonstat(dataset)
        cities_added = indicators_added = skipped = errors = 0
        
        with self._get_cursor() as cursor:
            for record in records:
                try:
                    # Dynamic city dim (usually 'cities')
                    city_key = next((k for k in record if k.endswith('ies')), None)
                    if not city_key:
                        skipped += 1
                        continue
                    city_code = record.get(city_key)
                    city_name = record.get(f'{city_key}_label')
                    
                    if not city_code or not city_code.endswith('C'):
                        skipped += 1
                        continue
                    
                    country = city_code[:2].upper()  # e.g., 'FR'
                    
                    cursor.execute("""
                        INSERT OR IGNORE INTO cities (city_code, city_name, country)
                        VALUES (?, ?, ?)
                    """, (city_code, city_name, country))
                    if cursor.rowcount > 0:
                        cities_added += 1
                    
                    # Dynamic indicator dim (usually 'indic_ur')
                    indic_key = next((k for k in record if k.startswith('indic')), None)
                    if not indic_key:
                        continue
                    year_key = next((k for k in record if k == 'time'), None)
                    
                    year = int(record.get(year_key, 0)) if year_key else None
                    value = float(record.get('value', None)) if record.get('value') is not None else None
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO indicators 
                        (city_code, indicator_code, indicator_name, year, value, status)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (city_code, record[indic_key], record.get(f'{indic_key}_label'), 
                        year, value, record.get('status', '')))
                    indicators_added += 1
                    
                except Exception as e:
                    errors += 1
                    logger.error(f"Error processing record: {e}")
        
        self.conn.commit()
        logger.info(f"Added {cities_added} cities, {indicators_added} indicators, skipped {skipped}, errors {errors}")

    def rank_cities_advanced(self, weights: Dict[str, float], country: str, limit: int = 5) -> List[Dict[str, Any]]:
        if not weights:
            # Fallback to population from worldcities
            with self._get_cursor() as cursor:
                cursor.execute("""
                    SELECT city_name, country, population 
                    FROM cities WHERE UPPER(country) = UPPER(?) 
                    ORDER BY population DESC NULLS LAST LIMIT ?
                """, (country, limit))
                return [{"city_name": r[0], "country": r[1], "score": r[2]} for r in cursor.fetchall()]
        
        # Check if any cities in country have data for at least one criterion
        with self._get_cursor() as cursor:
            placeholders = ','.join('?' * len(weights))
            cursor.execute(f"""
                SELECT 1 FROM indicators i
                JOIN cities c ON i.city_code = c.city_code
                WHERE UPPER(c.country) = UPPER(?) AND i.indicator_code IN ({placeholders})
                LIMIT 1
            """, [country] + list(weights.keys()))
            has_data = cursor.fetchone() is not None
        
        if not has_data:
            return self.rank_cities_advanced({}, country, limit)  # Recursive fallback
        
        # Rank using weights
        return self.rank_cities(weights, limit=limit, country_filter=country)


    def get_cities_by_country(self, country_name: str) -> Optional[List[str]]:
        """Returns a list of city names or None if no cities exist."""
        with self._get_cursor() as cursor:
            cursor.execute("""
                SELECT city_name FROM cities 
                WHERE LOWER(country) = LOWER(?) 
                ORDER BY city_name
            """, (country_name,))
            results = cursor.fetchall()
            return [row[0] for row in results] if results else None

    def get_city_data(self, city_code: str, latest_only: bool = True) -> Dict[str, Any]:
        """Get all indicators for a specific city."""
        with self._get_cursor() as cursor:
            cursor.execute("SELECT city_name, country FROM cities WHERE city_code = ?", (city_code,))
            city_info = cursor.fetchone()

            if not city_info:
                return {"error": f"City {city_code} not found"}

            sql = """
                SELECT indicator_code, indicator_name, year, value, status
                FROM indicators WHERE city_code = ?
            """
            if latest_only:
                sql = """
                    SELECT indicator_code, indicator_name, MAX(year) as year, value, status
                    FROM indicators WHERE city_code = ?
                    GROUP BY indicator_code ORDER BY indicator_code
                """
            else:
                sql += " ORDER BY indicator_code, year DESC"

            cursor.execute(sql, (city_code,))
            results = cursor.fetchall()
            
            return {
                "city_code": city_code,
                "city_name": city_info[0],
                "country": city_info[1],
                "indicators": [
                    {"code": r[0], "name": r[1], "year": r[2], "value": r[3], "status": r[4]}
                    for r in results
                ]
            }

    def calculate_score(self, city_code: str, weights: Dict[str, float]) -> float:
        """Calculate a normalized quality score (0-1) for a city."""
        total_score = 0.0
        total_weight = 0.0

        with self._get_cursor() as cursor:
            for indicator_code, weight in weights.items():
                cursor.execute("""
                    SELECT value, indicator_name FROM indicators
                    WHERE city_code = ? AND indicator_code = ?
                    ORDER BY year DESC LIMIT 1
                """, (city_code, indicator_code))

                result = cursor.fetchone()
                if result and result[0] is not None:
                    value, indicator_name = result[0], result[1]
                    normalized = self._normalize_value(value, indicator_name)

                    if self._is_lower_better(indicator_name):
                        normalized = 1 - normalized

                    total_score += normalized * weight
                    total_weight += weight

        return (total_score / total_weight) if total_weight > 0 else 0.0

    def _normalize_value(self, value: float, indicator_name: str) -> float:
        name = indicator_name.lower()
        if "%" in name:
            return min(value / 100.0, 1.0)
        if "minutes" in name:
            return min(value / 60.0, 1.0)
        if "km" in name:
            return min(value / 50.0, 1.0)
        if "eur" in name:
            return min(value / 100.0, 1.0)
        if "per 1000" in name:
            return min(value / 1000.0, 1.0)
        return min(value / 100000.0, 1.0)

    def _is_lower_better(self, indicator_name: str) -> bool:
        keywords = ["car", "motor cycle", "death", "accident", "cost", "time", "minutes", "eur"]
        return any(k in indicator_name.lower() for k in keywords)

    def rank_cities(self, weights: Dict[str, float], limit: int = 10, country_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Rank all cities by quality score."""
        with self._get_cursor() as cursor:
            if country_filter:
                cursor.execute("SELECT city_code, city_name, country FROM cities WHERE country = ?", (country_filter,))
            else:
                cursor.execute("SELECT city_code, city_name, country FROM cities")
            cities = cursor.fetchall()

        rankings = []
        for city_code, city_name, country in cities:
            score = self.calculate_score(city_code, weights)
            if score > 0:
                rankings.append({
                    "city_code": city_code,
                    "city_name": city_name,
                    "country": country,
                    "score": round(score, 3)
                })

        rankings.sort(key=lambda x: x["score"], reverse=True)
        return rankings[:limit]

    def rank_cities_advanced(self, weights: Dict[str, float], country: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Ranks cities. If weights are empty, ranks by population."""
        if not weights:
            with self._get_cursor() as cursor:
                cursor.execute("""
                    SELECT city_name, country, population as score 
                    FROM cities WHERE country = ? 
                    ORDER BY population DESC LIMIT ?
                """, (country, limit))
                return [{"city_name": r[0], "country": r[1], "score": r[2]} for r in cursor.fetchall()]
        
        return self.rank_cities(weights, limit=limit, country_filter=country)

    def list_cities(self, country: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all cities, optionally filtered by country."""
        with self._get_cursor() as cursor:
            if country:
                cursor.execute("""
                    SELECT city_code, city_name, country, population, lat, lng
                    FROM cities
                    WHERE UPPER(country) = UPPER(?)
                    ORDER BY city_name
                """, (country,))
            else:
                cursor.execute("""
                    SELECT city_code, city_name, country, population, lat, lng
                    FROM cities
                    ORDER BY city_name
                """)
            
            columns = ["city_code", "city_name", "country", "population", "lat", "lng"]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def list_available_indicators(self) -> List[Dict[str, str]]:
        """List all unique indicators in the database."""
        with self._get_cursor() as cursor:
            cursor.execute("SELECT DISTINCT indicator_code, indicator_name FROM indicators ORDER BY indicator_code")
            return [{"code": row[0], "name": row[1]} for row in cursor.fetchall()]

    def close(self):
        """Close the database connection."""
        self.conn.close()
        logger.info("Database connection closed.")