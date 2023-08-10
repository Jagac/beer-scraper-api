from http import HTTPStatus

from fastapi import FastAPI, Request

from scraper import Scraper

app = FastAPI(
    title="Beer Data",
    description="Get data from beeradvocate.com",
    version="0.1",
)


@app.get("/")
def _index(request: Request) -> dict:
    """Health check"""
    response = {
        "message": HTTPStatus.OK.phrase,
        "status-code": HTTPStatus.OK,
        "data": {},
    }
    return response


@app.get("/beer/{category}")
def _data(category: str):
    try:
        beer_scraper = Scraper(proxy=True)
        return beer_scraper.main(category)
    except:
        beer_scraper = Scraper(proxy=False)
        return beer_scraper.main(category)
