"""10/28 loads raw json from s3, loads it to postgres (RDS) and saves a preprocessed copy to another bucket
"""

import json
from datetime import datetime
import boto3
import os
import utils


# =================ENV========================#
aws_access_key_id = os.environ["aws_access_key_id"]
aws_secret_access_key = os.environ["aws_secret_access_key"]
rds_user = os.environ["rds_user"]
rds_password = os.environ["rds_password"]
rds_host = os.environ["rds_host"]
# ============================================#


# =================AWS========================#
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

READ_BUCKET = "scrapy-beeradvocate-raw"
WRITE_BUCKET = "scrapy-beeradvocate-preprocessed"
# ============================================#


def preprocess_beers():
    file = s3.get_object(
        Bucket=READ_BUCKET,
        Key="raw/topnewbeer_" + datetime.today().strftime("%y%m%d") + ".json",
    )

    content = file["Body"]
    raw_data = json.loads(content.read())

    data = utils.apply_filter(data=raw_data)

    for i in data:
        try:
            i["beer_name"] = str(i["beer_name"])
            i["brewer_name"] = str(i["brewer_name"])
            i["percent_alcohol"] = float(i["percent_alcohol"].replace("%", ""))
            i["beer_avg_score"] = int(i["beer_avg_score"])
            i["avg_across_all"] = float(i["avg_across_all"])
            i["beer_number_of_reviews"] = int(i["beer_number_of_reviews"])
            i["number_of_ratings"] = int(i["number_of_ratings"])
            i["date_added"] = utils.reformat_date(i["date_added"])
            i["number_of_wants"] = int(i["number_of_wants"])
            i["number_of_gots"] = int(i["number_of_gots"])
            i["status"] = str(i["status"])
            i["last_updated"] = str(i["last_updated"])

        except:
            i["percent_alcohol"] = 0

    json_object = json.dumps(data, indent=4)

    utils.dump_to_rds(
        rds_host=rds_host,
        rds_user=rds_user,
        rds_password=rds_password,
        data=json_object,
        table="beers",
    )

    s3.put_object(
        Body=json_object,
        Bucket=WRITE_BUCKET,
        Key="topnewbeer_" + datetime.today().strftime("%y%m%d") + ".json",
    )


def preprocess_brewers():
    file = s3.get_object(
        Bucket=READ_BUCKET,
        Key="raw/topnewbrewersspider_" + datetime.today().strftime("%y%m%d") + ".json",
    )

    content = file["Body"]
    raw_data = json.loads(content.read())
    data = utils.apply_filter(data=raw_data)

    for i in data:
        try:
            i["brewer_avg_beer_score"] = float(i["brewer_avg_beer_score"])
            i["number_of_beers"] = int(i["number_of_beers"].replace(",", ""))
            i["establishment_type"] = i["establishment_type"].strip()
            i["brewer_number_of_reviews"] = int(
                i["brewer_number_of_reviews"].replace(",", "")
            )
            i["avg_brewer_score"] = float(i["avg_brewer_score"])
        except:
            print("DNE")

    json_object = json.dumps(data, indent=4)

    utils.dump_to_rds(
        rds_host=rds_host,
        rds_user=rds_user,
        rds_password=rds_password,
        data=json_object,
        table="brewers",
    )

    s3.put_object(
        Body=json_object,
        Bucket=WRITE_BUCKET,
        Key="topnewbrewer_" + datetime.today().strftime("%y%m%d") + ".json",
    )


def main():
    utils.drop_tables()
    preprocess_beers()
    preprocess_brewers()


if __name__ == "__main__":
    main()
