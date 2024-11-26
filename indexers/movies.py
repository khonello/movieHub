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

class MoviesIndexer:
    def __init__(self, save_file: str = "output/movies.json"):
        self.save_file = Path(save_file)
        self.working_index = [2, 3, 4, 5, 11, 12]
        self.processed_urls = set()
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='log/movies.log'
        )

    def _load_progress(self) -> Dict:
        try:
            with open(self.save_file, "r") as f:
                data = json.load(f)
                for year_data in data.get("movies", {}).values():
                    for movie in year_data:
                        self.processed_urls.add(movie["url"])
                        for content_url in movie.get("content", []):
                            self.processed_urls.add(content_url)
                return data
        except FileNotFoundError:
            return {"movies": {}, "last_processed": {"index": 2, "movie": 0}}

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

    def _extract_movie_info(self, url: str) -> Tuple[str, str]:
        parts = url.split('Movie/')
        if len(parts) < 2:
            return '', ''
        
        path_parts = parts[1].split('/')
        year = ''
        movie_name = ''
        
        for i, part in enumerate(path_parts):
            if part.isdigit() and len(part) == 4:
                year = part
                if i + 1 < len(path_parts):
                    movie_name = path_parts[i + 1]
                    movie_name = movie_name.replace('.', ' ').strip()
                    indicators = ['1080p', '720p', '2160p', '480p', 
                                'BluRay', 'WEB-DL', 'HEVC', 'x264', 'x265',
                                'WEBRip', 'BRRip', 'HDTV', 'DVDRip']
                    for indicator in indicators:
                        movie_name = movie_name.replace(indicator, '').strip()
                    if year in movie_name:
                        movie_name = movie_name.replace(year, '').strip()
        
        return year, movie_name

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

    def _process_movie(self, year_url: str, movie_tag, year_title: str, indexes: Dict, index: int) -> None:
        if movie_tag.attrs.get("title") is None:
            return
        
        movie_url = year_url + movie_tag.attrs["href"]
        if movie_url in self.processed_urls:
            return
            
        content_soup = self._fetch(movie_url)
        if not content_soup:
            return
        
        year, movie_name = self._extract_movie_info(movie_url)
        
        content_resultset = content_soup.find_all("a")
        extra_info = []
        
        content_urls = []
        for content_tag in content_resultset:
            if (content_tag.attrs.get("title") and 
                any(content_tag.attrs["href"].endswith(ext) for ext in ["mkv", "mp4"])):
                
                content_url = movie_url + content_tag.attrs["href"]
                if content_url not in self.processed_urls:
                    content_urls.append(content_url)
                    self.processed_urls.add(content_url)
                    
                    quality_info = self._extract_quality(content_url, content_tag.attrs["href"])
                    extra_info.append({
                        "url": content_url,
                        "filename": content_tag.attrs["href"],
                        "quality": quality_info
                    })
        
        if content_urls:
            movie_data = {
                "url": movie_url,
                "title": movie_tag.attrs["title"],
                "content": content_urls,
                "extra_info": {
                    "extracted_year": year,
                    "extracted_name": movie_name,
                    "quality_details": extra_info,
                    "added_date": datetime.now().isoformat()
                }
            }
            
            if year_title.lower() not in indexes["movies"]:
                indexes["movies"][year_title.lower()] = []
            
            indexes["movies"][year_title.lower()].append(movie_data)
            self.processed_urls.add(movie_url)
            
            indexes["last_processed"] = {
                "index": index,
                "movie": movie_url
            }
            self._save_progress(indexes)
            logging.info(f"Added movie: {movie_data['title']} ({year}) - Files: {len(content_urls)}")

    def create_index(self):
        indexes = self._load_progress()
        start_index = indexes.get("last_processed", {}).get("index", self.working_index[0])
        
        for index in self.working_index[self.working_index.index(start_index):]:
            url = f"https://dl{index}.sermoviedown.pw/"
            soup = self._fetch(url)
            if not soup:
                continue

            links = soup.find_all("a")
            for link in links:
                if not link.get("title") or "movie" not in link.get("title").lower():
                    continue
                
                movie_path = url + link.get("href")
                movie_soup = self._fetch(movie_path)
                
                if not movie_soup:
                    continue

                years = movie_soup.find_all("a")
                for year in years:
                    if not year.attrs.get("title"):
                        continue
                    
                    year_url = movie_path + year.attrs["href"]
                    year_soup = self._fetch(year_url)
                    if not year_soup:
                        continue

                    movies_resultset = year_soup.find_all("a")
                    for movie_tag in movies_resultset:
                        self._process_movie(year_url, movie_tag, year.attrs["title"], 
                                            indexes, index)

        indexes.pop("last_processed", None)
        self._save_progress(indexes)
