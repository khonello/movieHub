import requests
import bs4
import json
from time import sleep
from typing import Dict, List
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

def load_progress() -> Dict:
    try:
        with open("movie.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"movies": {}, "last_processed": {"index": 2, "movie": 0, "year": 0}}

def save_progress(indexes: Dict):
    with open("movie.json", "w+") as f:
        json.dump(indexes, fp=f, indent=8, sort_keys=True)

def fetch(url: str, retries=3) -> bs4.BeautifulSoup:
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            if response.ok:
                return bs4.BeautifulSoup(response.content, "html.parser")
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            sleep(2)
    return None

def process(year_url: str, movie_tag, year_title: str, indexes: Dict):
    if movie_tag.attrs.get("title") is None:
        return
    
    movie_url = year_url + movie_tag.attrs["href"]
    content_soup = fetch(movie_url)
    if not content_soup:
        return
    
    content_resultset = content_soup.find_all("a")
    movie_data = {
        "url": movie_url,
        "title": movie_tag.attrs["title"],
        "content": [ 
            movie_url + content_tag.attrs["href"] for content_tag in content_resultset 
                if content_tag.attrs.get("title") and any(content_tag.attrs["href"].endswith(ext) for ext in ["mkv", "mp4"] )
        ]
    }
    
    if year_title.lower() not in indexes["movies"]:

        indexes["movies"][year_title.lower()] = []
    indexes["movies"][year_title.lower()].append(movie_data)

    indexes["last_processed"] = {"index": index, "movie": year_url + movie_tag.attrs["href"]}
    save_progress(indexes)

indexes = load_progress()
working_index = [2, 3, 4, 5, 11, 12]
start_index = indexes.get("last_processed", {}).get("index")

with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%")) as progress:
    
    main_task = progress.add_task("[cyan]Processing directories", total= len(working_index[start_index:]))
    for index in working_index[working_index.index(start_index):]:
        
        url = f"https://dl{index}.sermoviedown.pw/"
        soup = fetch(url)
        if not soup:
            progress.advance(main_task)
            continue

        links = soup.find_all("a")
        for link in links:
            if not link.get("title") or "movie" not in link.get("title").lower():
                continue
                
            movie_path = url + link.get("href")
            movie_soup = fetch(movie_path)
            
            if not movie_soup:
                continue

            years = movie_soup.find_all("a")
            for year in years:
                if not year.attrs.get("title"):
                    continue
                    
                year_url = movie_path + year.attrs["href"]
                year_soup = fetch(year_url)
                if not year_soup:
                    continue

                movies_resultset = year_soup.find_all("a")
                for movie_tag in movies_resultset:
                    process(year_url, movie_tag, year.attrs["title"], indexes)
        
        progress.advance(main_task)

indexes.pop("last_processed", None)
save_progress(indexes)
