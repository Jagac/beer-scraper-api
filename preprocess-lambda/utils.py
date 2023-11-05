import psycopg2
from datetime import datetime
import json


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


def reformat_date(date: str) -> str:
    """Makes Oct 28, 2023 to 2023-10-28

    Args:
        date (str)

    Returns:
        str: reformated
    """
    return datetime.strptime(date, "%b %d, %Y").strftime("%Y-%m-%d")


def drop_tables(rds_user: str, rds_password: str, rds_host: str) -> None:
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


def dump_to_rds(
    rds_user: str, rds_password: str, rds_host: str, data: list[dict], table: str
) -> None:
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
