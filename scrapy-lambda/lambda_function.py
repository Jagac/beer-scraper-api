import sys


def handler(event, context):
    # Run scrapy
    import subprocess

    subprocess.run(["python3", "main.py"])

    return {
        "statusCode": "200",
        "body": "Scrapy Lambda function successfully invoked",
    }
