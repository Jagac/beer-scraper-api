"""Scrapes top new brewers only and saves it as json
"""
from datetime import datetime
from typing import Generator

import scrapy
from itemloaders import ItemLoader
from scrapy.http import Response
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from ..items import BrewerItem


class TopnewbrewersSpider(CrawlSpider):
    name = "topnewbrewersspider"
    allowed_domains = ["www.beeradvocate.com"]
    start_urls = ["https://www.beeradvocate.com/beer/top-new/"]

    custom_settings = {
        "FEED_URI": "/tmp/topnewbrewersspider_"
        + datetime.today().strftime("%y%m%d")
        + ".json",
        "FEED_FORMAT": "json",
    }

    rules = [Rule(LinkExtractor(allow=r"profile"), callback="parse_item")]

    def parse_item(self, response: Response) -> Generator[int, float, str]:
        """Parse the complete page and hit all the links

        Args:
            response (Response): page response

        Yields:
            Generator[int, float, str]: json data
        """
        loader = ItemLoader(item=BrewerItem(), selector=response)
        loader.add_value("last_updated", str(datetime.today().strftime("%Y-%m-%d")))
        loader.add_xpath(
            "brewer_name", '//*[@id="content"]/div/div/div[3]/div/div/div[2]/h1/text()'
        )
        loader.add_xpath(
            "brewer_avg_beer_score",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[4]/div[2]/div/dl/dd[1]/span",
        )
        loader.add_xpath(
            "number_of_beers",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[4]/div[2]/div/dl/dd[2]/span",
        )
        loader.add_xpath(
            "avg_brewer_score",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[4]/div[4]/div/dl/dd[1]/a",
        )
        loader.add_xpath(
            "brewer_number_of_reviews",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[4]/div[4]/div/dl/dd[2]/span",
        )
        loader.add_xpath(
            "city",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[3]/a[1]",
        )
        loader.add_xpath(
            "establishment_type",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[3]/text()[2]",
        )
        yield loader.load_item()
