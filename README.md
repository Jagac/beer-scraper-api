# beer-scraper-api
 
### Scrapes beeradvocate.com and returns a JSON response with beer and brewer data.
#### To run you can follow these commands
    docker pull jagaccar/beer-scraper-api
    docker run -p 8000:80 jagaccar/beer-scraper-api
#### or build it yourself 

    docker build -t "name" .
    docker run -p 8000:80 "name"

#### To check if the API is working go to localhost:8000 on your browser. Currently, the only pages working are the ones not using country codes. To start scraping, use 
    localhost:8000/beer/top-rated
    localhost:8000/beer/trending
    localhost:8000/beer/top-new
    localhost:8000/beer/fame
    localhost:8000/beer/popular
    localhost:8000/beer/worst

#### Can be slow at times, depending on the proxy, up to 10 mins but generally expect 3-5 mins per page. Am looking to add country support so links like localhost:8000/beer/top-rated/br can work.

