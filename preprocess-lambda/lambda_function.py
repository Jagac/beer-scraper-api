import sys


def handler(event, context):
    # Run scrapy
    import subprocess

    subprocess.run(["python3", "preprocess.py"])

    return {
        "statusCode": "200",
        "body": "Preprocess Lambda function successfully invoked",
    }

