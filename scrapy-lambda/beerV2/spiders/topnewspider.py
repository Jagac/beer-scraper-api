"""Scrapes top new beers only and saves it as json
"""
from datetime import datetime
from typing import Generator

import scrapy
from itemloaders import ItemLoader
from scrapy.http import Response
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from ..items import Beerv2Item


class TopnewspiderSpider(CrawlSpider):
    name = "topnewbeersspider"
    allowed_domains = ["beeradvocate.com"]
    start_urls = ["https://www.beeradvocate.com/beer/top-new/"]

    custom_settings = {
        "FEED_URI": "/tmp/topnewbeer_" + datetime.today().strftime("%y%m%d") + ".json",
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
        loader = ItemLoader(item=Beerv2Item(), selector=response)

        loader.add_value("last_updated", str(datetime.today().strftime("%Y-%m-%d")))

        loader.add_xpath(
            "beer_name", '//*[@id="content"]/div/div/div[3]/div/div/div[2]/h1/text()'
        )
        loader.add_xpath(
            "brewer_name",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[1]/div[1]/dl/dd[1]/a/b",
        )
        loader.add_xpath(
            "beer_style",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[1]/div[1]/dl/dd[3]/a[1]/b",
        )
        loader.add_xpath(
            "location",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[1]/div[1]/dl/dd[2]/a[1]",
        )
        loader.add_xpath(
            "percent_alcohol",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[1]/div[1]/dl/dd[4]/span/b",
        )
        loader.add_xpath(
            "beer_avg_score",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[1]/div[1]/dl/dd[5]/span/b",
        )
        loader.add_xpath(
            "avg_across_all",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[1]/div[1]/dl/dd[6]/b/span",
        )
        loader.add_xpath(
            "beer_number_of_reviews",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[1]/div[1]/dl/dd[7]/span/b",
        )
        loader.add_xpath(
            "number_of_ratings",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[1]/div[1]/dl/dd[8]/span/b",
        )
        loader.add_xpath(
            "date_added",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[1]/div[1]/dl/dd[11]/span",
        )
        loader.add_xpath(
            "number_of_wants",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[1]/div[1]/dl/dd[12]/a/span",
        )
        loader.add_xpath(
            "number_of_gots",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[1]/div[1]/dl/dd[13]/a/span",
        )
        loader.add_xpath(
            "status",
            "/html/body/div[2]/div/div[2]/div[2]/div[2]/div/div/div[3]/div/div/div[4]/div[4]/div[1]/div[1]/dl/dd[9]/span/span",
        )

        yield loader.load_item()
