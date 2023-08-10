import re
from typing import List, Dict
from unicodedata import normalize

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from proxy import Proxy


class Scraper:
    def __init__(self, proxy=False) -> None:
        self.base = "https://www.beeradvocate.com"
        self.beer_df = pd.DataFrame()
        self.brewer_df = pd.DataFrame()
        self.proxy = proxy

        if self.proxy == True:
            self.proxy = Proxy().main()

        print(self.proxy)

    def main(self, category: str) -> list[dict]:
        """Joins the 2 dataframes and converts to dict

        Args:
            category (str): any extension after beeradvocate.com/beer/

        Returns:
            Dict[str, str]: complete dataset
        """

        self.parse(f"{category}")
        final_df = pd.merge(
            self.beer_df, self.brewer_df, left_index=True, right_index=True
        )
        final_df = final_df.fillna("na")

        return final_df.to_dict(orient="records")

    def make_request(self, url) -> requests:
        if self.proxy == True:
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
            self.beer_df = pd.concat(
                [self.beer_df, pd.DataFrame([beer_data])], ignore_index=True
            )

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
            self.brewer_df = pd.concat(
                [self.brewer_df, pd.DataFrame([brewer_data])], ignore_index=True
            )

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
