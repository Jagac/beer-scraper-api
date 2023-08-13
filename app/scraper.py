import re
from typing import Dict, List
from unicodedata import normalize

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from proxy import Proxy


class Scraper:
    def __init__(self, proxy=False) -> None:
        self.base = "https://www.beeradvocate.com"
        self.proxy = proxy

        if self.proxy:
            self.proxy = Proxy().main()

        self.beer_df = []
        self.brewer_df = []

        print(self.proxy)

    def main(self, category: str) -> list[dict]:
        """Joins the 2 dataframes and converts to dict

        Args:
            category (str): any extension after beeradvocate.com/beer/

        Returns:
            Dict[str, str]: complete dataset
        """

        self.parse(f"{category}")
        self.brewer_df = [x for x in self.brewer_df if x != []]

        final_df = pd.merge(
            pd.DataFrame(self.beer_df,
                         columns=[
                             "beer_name", "brewer_name",
                             "location", "beer_style",
                             "alcohol_percentage", "average_rating",
                             "number_of_reviews", "number_of_ratings",
                             "status", "last_rated",
                             "date_added", "wants", "gets"
                         ]),
            pd.DataFrame(self.brewer_df,
                         columns=[
                             "brewer_name_2", "service_style",
                             "st_address", "city", "state", "zip_code",
                             "country", "phone_number", "website", "notes",
                             "avg_rating_all_beers",
                             "num_of_active_beers",
                             "num_of_ratings_all_beers",
                             "avg_rating_place",
                             "num_reviews",
                             "num_ratings",
                             "pdev"
                         ]),

            left_index=True, right_index=True
        )
        final_df = final_df.fillna("na")

        return final_df.to_dict(orient="records")

    def make_request(self, url: str) -> requests:
        if self.proxy:
            r = requests.get(
                url, proxies={"http": self.proxy, "https": self.proxy}, timeout=5
            )
        else:
            r = requests.get(url, timeout=5)

        return r.text

    def parse(self, category: str):
        """Find links on html table and use to parse the website

        Args:
            category (str): any of the categories from the website
        """
        html = self.make_request(self.base + f"/beer/{category}")
        soup = BeautifulSoup(html, "html.parser")
        table = soup.table.find_all("tr")

        temp_links = []
        for link in range(len(table)):
            for a in table[link].find_all("a", href=True):
                temp_links.append(a["href"])

        beer_stat_links = temp_links[::3]
        brewer_stats_links = temp_links[1::3]

        for link in tqdm(beer_stat_links, desc="Scraping Beers"):
            beer_data = []

            data = self.make_request(self.base + link)
            soup = BeautifulSoup(data, "html.parser")

            title = soup.find("div", {"class": "titleBar"}).find("h1")
            for word in title:
                beer_data.append(word.text)

            box = soup.find("div", {"id": "info_box"})
            for i in box.find_all("dd"):
                beer_data.append(i.text)

            beer_data = self.beer_data_string_cleanup(beer_data)
            self.beer_df.append(beer_data)

        for brewer in tqdm(brewer_stats_links, desc="Scraping Brewers"):
            brewer_data = []

            data = self.make_request(self.base + brewer)
            soup = BeautifulSoup(data, "html.parser")

            title = soup.find("div", {"class": "titleBar"}).find("h1")
            for word in title:
                brewer_data.append(word.text)

            info_box = soup.find("div", {"id": "info_box"})
            for i in info_box:
                brewer_data.append(i.text)

            stats_box = soup.find("div", {"id": "stats_box"})
            for i in stats_box.find_all("dd"):
                brewer_data.append(i.text)

            brewer_data = self.brewer_data_string_cleanup(brewer_data)

            if len(brewer_data) > 17:
                brewer_data.clear()

            self.brewer_df.append(brewer_data)

    def beer_data_string_cleanup(self, parsed_data: List[str]) -> List[str]:
        """Normalizes the list before appending to pandas

        Args:
            parsed_data (list): extracted html strings

        Returns:
            list: more normalized list of string
        """
        parsed_data = list(filter(None, parsed_data))
        parsed_data = [normalize("NFKD", x) for x in parsed_data]
        parsed_data = [x.strip() for x in parsed_data]
        parsed_data.pop(2)
        parsed_data.pop(5)
        parsed_data[5] = parsed_data[5].split("|")
        parsed_data[5] = parsed_data[5][0]
        parsed_data[3] = re.sub(r"(\w)([A-Z])", r"\1 \2", parsed_data[3])
        parsed_data[3] = parsed_data[3].rsplit(" ", 1)[0]
        parsed_data[3] = parsed_data[3].rsplit(" ", 1)[0]
        parsed_data = [x.strip() for x in parsed_data]

        return parsed_data

    def brewer_data_string_cleanup(self, parsed_data: List[str]) -> List[str]:
        """Normalizes the list before appending to pandas

        Args:
            parsed_data (list): extracted html strings

        Returns:
            list: more normalized list of string
        """
        parsed_data = [x.replace(",", " ") for x in parsed_data]
        parsed_data = list(filter(None, parsed_data))
        parsed_data = [normalize("NFKD", x) for x in parsed_data]
        parsed_data = [x.strip() for x in parsed_data]
        parsed_data = list(filter(None, parsed_data))
        parsed_data.pop(8)
        parsed_data[7] = parsed_data[7].replace("|", " ")
        parsed_data = [x.strip() for x in parsed_data]

        return parsed_data
