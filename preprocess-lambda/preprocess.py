"""10/28 loads raw json from s3, loads it to postgres (RDS) and saves a preprocessed copy to another bucket
"""

import json
from datetime import datetime
import boto3
import os
import psycopg2


# ===========================================#
aws_access_key_id = os.environ["aws_access_key_id"]
aws_secret_access_key = os.environ["aws_secret_access_key"]

rds_user = os.environ["rds_user"]
rds_password = os.environ["rds_password"]
rds_host = os.environ["rds_host"]
# ===========================================#

s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

READ_BUCKET = "scrapy-beeradvocate-raw"
WRITE_BUCKET = "scrapy-beeradvocate-preprocessed"


def reformat_date(date: str):
    """Makes Oct 28, 2023 to 2023-10-28

    Args:
        date (str)

    Returns:
        str: reformated
    """
    return datetime.strptime(date, "%b %d, %Y").strftime("%Y-%m-%d")


def apply_filter(data: list[dict]) -> list[dict]:
    """Cleans up scrapy's Crawl Spider

    Args:
        data (dict): raw json

    Returns:
        dict : removes unnecessary data
    """
    for i in data:
        if len(i) <= 2:
            i.clear()

    data = [i for i in data if i]

    return data


def drop_tables() -> None:
    with psycopg2.connect(
        database="postgres",
        user=rds_user,
        password=rds_password,
        host=rds_host,
        port="5432",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""drop table if exists brewers""")
            cur.execute("""drop table if exists beers""")
            cur.execute("""drop table if exists final_table""")


def dump_to_rds(data: list[dict], table: str) -> None:
    """Load to postgres

    Args:
        data (dict): raw json
        table (str): name of table in RDS
    """
    with psycopg2.connect(
        database="postgres",
        user=rds_user,
        password=rds_password,
        host=rds_host,
        port="5432",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""create table if not exists beers(
                        last_updated text,
                        beer_name text,
                        brewer_name text,
                        beer_style text,
                        location text,
                        percent_alcohol float,
                        beer_avg_score float,
                        avg_across_all float,
                        beer_number_of_reviews int,
                        number_of_ratings int,
                        date_added text,
                        number_of_wants int,
                        number_of_gots int,
                        status text
                        );"""
            )

            cur.execute(
                f"""create table if not exists brewers(
                        last_updated text,
                        brewer_name text,
                        brewer_avg_beer_score float,
                        number_of_beers int,
                        avg_brewer_score float,
                        brewer_number_of_reviews int,
                        city text,
                        establishment_type text
                        );"""
            )

            query_sql = f""" insert into {table}
                select * from json_populate_recordset(NULL::{table}, %s) """
            cur.execute(query_sql, (json.dumps(data),))


class PreprocessBeers:
    def __init__(self) -> None:
        file = s3.get_object(
            Bucket=READ_BUCKET,
            Key="raw/topnewbeer_" + datetime.today().strftime("%y%m%d") + ".json",
        )

        content = file["Body"]
        self.data = json.loads(content.read())

    def start_preprocess(self):
        data = apply_filter(self.data)

        for i in data:
            try:
                i["beer_name"] = str(i["beer_name"])
                i["brewer_name"] = str(i["brewer_name"])
                i["percent_alcohol"] = float(i["percent_alcohol"].replace("%", ""))
                i["beer_avg_score"] = int(i["beer_avg_score"])
                i["avg_across_all"] = float(i["avg_across_all"])
                i["beer_number_of_reviews"] = int(i["beer_number_of_reviews"])
                i["number_of_ratings"] = int(i["number_of_ratings"])
                i["date_added"] = reformat_date(i["date_added"])
                i["number_of_wants"] = int(i["number_of_wants"])
                i["number_of_gots"] = int(i["number_of_gots"])
                i["status"] = str(i["status"])
                i["last_updated"] = str(i["last_updated"])

            except:
                i["percent_alcohol"] = 0

        json_object = json.dumps(data, indent=4)
        s3.put_object(
            Body=json_object,
            Bucket=WRITE_BUCKET,
            Key="topnewbeer_" + datetime.today().strftime("%y%m%d") + ".json",
        )

        dump_to_rds(data, "beers")


class PreprocessBrewers:
    def __init__(self) -> None:
        file = s3.get_object(
            Bucket=READ_BUCKET,
            Key="raw/topnewbrewersspider_"
            + datetime.today().strftime("%y%m%d")
            + ".json",
        )

        content = file["Body"]
        self.data = json.loads(content.read())

    def start_process(self):
        data = apply_filter(self.data)

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

        s3.put_object(
            Body=json_object,
            Bucket=WRITE_BUCKET,
            Key="topnewbrewer_" + datetime.today().strftime("%y%m%d") + ".json",
        )
        dump_to_rds(data, "brewers")


def main():
    drop_tables()
    beer = PreprocessBeers()
    beer.start_preprocess()

    brewer = PreprocessBrewers()
    brewer.start_process()


if __name__ == "__main__":
    main()
