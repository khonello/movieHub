import requests
import bs4
import json
from datetime import datetime
from time import sleep
from typing import Dict, Optional, Tuple
from pathlib import Path
from urllib.parse import urljoin
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
import logging

class SeriesIndexer:
    def __init__(self, save_file: str = "output/series.json"):
        self.save_file = Path(save_file)
        self.working_index = [2, 3, 4, 5, 11, 12]
        self.processed_urls = set()

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            filename="log/series.log"
        )

    def _load_progress(self) -> Dict:
        try:
            with open(self.save_file, "r") as f:
                data = json.load(f)
                for year_data in data.get("series", {}).values():
                    for series in year_data:
                        self.processed_urls.add(series["url"])
                        for content_url in series.get("content", []):
                            self.processed_urls.add(content_url)
                return data
        except FileNotFoundError:
            return {"series": {}, "last_processed": {"index": 2, "series": 0}}

    def _save_progress(self, indexes: Dict) -> None:
        with open(self.save_file, "w+") as f:
            json.dump(indexes, fp=f, indent=8, sort_keys=True)

    def _fetch(self, url: str, retries: int = 3) -> Optional[bs4.BeautifulSoup]:
        for attempt in range(retries):
            try:
                response = requests.get(url, timeout=10)
                if response.ok:
                    return bs4.BeautifulSoup(response.content, "html.parser")
                logging.warning(f"Failed to fetch {url}: Status {response.status_code}")
            except requests.RequestException as e:
                logging.error(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt < retries - 1:
                    sleep(2)
        return None

    def _extract_series_info(self, url: str) -> Tuple[str, str, str]:
        parts = url.split("Series/")
        if len(parts) < 2:
            return "", "", ""

        path_parts = parts[1].split("/")
        year, series_name, season = "", "", ""

        for i, part in enumerate(path_parts):
            if part.isdigit() and len(part) == 4:
                year = part
                if i + 1 < len(path_parts):
                    series_name = path_parts[i + 1].replace(".", " ").strip()
                    indicators = [
                        "1080p", "720p", "2160p", "480p", "BluRay", "WEB-DL",
                        "HEVC", "x264", "x265", "WEBRip", "BRRip", "HDTV", "DVDRip"
                    ]
                    for indicator in indicators:
                        series_name = series_name.replace(indicator, "").strip()
                    if year in series_name:
                        series_name = series_name.replace(year, "").strip()
            elif part.lower().startswith("s") and part[1:].isdigit():
                season = part
            elif "season" in part.lower() and any(c.isdigit() for c in part):
                season = f"S{int(''.join(c for c in part if c.isdigit())):02d}"
                print(season)

        return year, series_name, season

    def _extract_quality(self, url: str, filename: str) -> Dict[str, str]:
        quality_info = {
            "resolution": "unknown",
            "codec": "unknown",
            "source": "unknown",
            "bit_depth": "unknown"
        }

        resolutions = ["720p", "1080p", "2160p", "480p", "360p"]
        codecs = ["x264", "x265", "HEVC", "AVC", "H264", "H.264", "H265", "H.265"]
        sources = ["WEB-DL", "WEBRip", "BRRip", "BluRay", "HDTV", "DVDRip", "NF"]
        bit_depths = ["8bit", "10bit", "10Bit", "8Bit"]

        search_text = f"{url} {filename}".upper()

        for res in resolutions:
            if res.upper() in search_text:
                quality_info["resolution"] = res
                break

        for codec in codecs:
            if codec.upper() in search_text:
                quality_info["codec"] = codec
                break

        for source in sources:
            if source.upper() in search_text:
                quality_info["source"] = source
                break

        for depth in bit_depths:
            if depth.upper() in search_text:
                quality_info["bit_depth"] = depth.lower()
                break

        if "HEVC" in search_text:
            quality_info["codec"] = "HEVC"
        if "NF." in search_text or ".NF." in search_text:
            quality_info["source"] = "NF"

        return quality_info

    def _process_series(self, series_url: str, indexes: Dict) -> None:

        if series_url in self.processed_urls:
            return

        soup = self._fetch(series_url)
        if not soup:
            return

        year, series_name, _ = self._extract_series_info(series_url)

        self._recursive_fetch(series_url, indexes, year, year, series_name)
        self.processed_urls.add(series_url)

        self._save_progress(indexes)

    def _recursive_fetch(self, base_url: str, indexes: Dict, year_title: str, year: str, series_name: str) -> None:
        stack = [base_url]
        visited = set()

        while stack:
            current_url = stack.pop()
            if current_url in visited or current_url in self.processed_urls:
                continue

            visited.add(current_url)
            soup = self._fetch(current_url)
            if not soup:
                continue

            links = soup.find_all("a")
            for link in links:
                if not link.get("title"):
                    continue
                
                href = link.get("href")
                full_url = urljoin(current_url, href)

                if any(href.endswith(ext) for ext in ["mkv", "mp4"]):
                    quality_info = self._extract_quality(full_url, href)
                    
                    quality_detail_entry = {
                        "filename": href,
                        "quality": quality_info,
                        "url": full_url
                    }
                    
                    series_entry = indexes["series"].setdefault(year_title.lower(), [])
                    existing_series = next((s for s in series_entry if s["title"] == series_name), None)
                    if existing_series:
                        existing_series["content"].append(full_url)
                        existing_series["extra_info"]["quality_details"].append(quality_detail_entry)
                    else:
                        indexes["series"][year_title.lower()].append({
                            "title": series_name,
                            "url": urljoin(base_url, series_name),
                            "content": [full_url],
                            "extra_info": {
                                "added_date": datetime.now().isoformat(),
                                "extracted_name": "",
                                "extracted_year": "",
                                "quality_details": [quality_detail_entry]
                            }
                        })
                    self.processed_urls.add(full_url)
                else:
                    stack.append(full_url)

    def create_index(self):
        indexes = self._load_progress()
        start_index = indexes.get("last_processed", {}).get("index", self.working_index[0])

        for index in self.working_index[self.working_index.index(start_index):]:
            url = f"https://dl{index}.sermoviedown.pw/Series"
            soup = self._fetch(url)
            if not soup:
                continue

            links = soup.find_all("a")
            for link in links:
                if not link.get("title"):
                    continue
            
                year_url = f"{url}/{link.get('href')}"
                soup = self._fetch(year_url)

                series_links = soup.find_all("a")
                for link in series_links:
                    if not link.get("title"):
                        continue

                    series_url = urljoin(year_url, link.get("href"))
                    self._process_series(series_url, indexes)

        indexes.pop("last_processed", None)
