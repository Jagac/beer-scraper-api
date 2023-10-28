# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import MapCompose, TakeFirst
from w3lib.html import remove_tags


class Beerv2Item(scrapy.Item):
    # define the fields for your item here like:
    beer_name = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )
    brewer_name = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )

    beer_style = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )
    percent_alcohol = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )
    beer_avg_score = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )
    avg_across_all = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )
    beer_number_of_reviews = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )
    number_of_ratings = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )
    status = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )
    date_added = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )
    number_of_wants = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )
    number_of_gots = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )
    last_updated = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )
    location = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )


class BrewerItem(scrapy.Item):
    brewer_name = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )

    last_updated = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )

    brewer_avg_beer_score = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )

    number_of_beers = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )

    avg_brewer_score = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )

    brewer_number_of_reviews = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )

    city = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )

    establishment_type = scrapy.Field(
        input_processor=MapCompose(remove_tags), output_processor=TakeFirst()
    )
