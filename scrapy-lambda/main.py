import json
import os
from datetime import datetime

import boto3
import scrapy
from scrapy.crawler import CrawlerProcess
from subprocess import call

from beerV2.spiders.topnewbrewers import TopnewbrewersSpider
from beerV2.spiders.topnewspider import TopnewspiderSpider

call("rm -rf /tmp/*", shell=True)


process = CrawlerProcess()
process.crawl(TopnewspiderSpider)
process.crawl(TopnewbrewersSpider)
process.start()

aws_access_key_id = os.environ["aws_access_key_id"]
aws_secret_access_key = os.environ["aws_secret_access_key"]


s3 = boto3.resource(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

s3object1 = s3.Object(
    "scrapy-beeradvocate-raw",
    "raw/topnewbrewersspider_" + datetime.today().strftime("%y%m%d") + ".json",
)
file1 = open(
    "/tmp/topnewbrewersspider_" + datetime.today().strftime("%y%m%d") + ".json"
)
json_data1 = json.load(file1)

s3object1.put(Body=(bytes(json.dumps(json_data1).encode("UTF-8"))))


s3object2 = s3.Object(
    "scrapy-beeradvocate-raw",
    "raw/topnewbeer_" + datetime.today().strftime("%y%m%d") + ".json",
)

file2 = open("/tmp/topnewbeer_" + datetime.today().strftime("%y%m%d") + ".json")
json_data2 = json.load(file2)
s3object2.put(Body=(bytes(json.dumps(json_data2).encode("UTF-8"))))
