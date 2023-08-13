import requests
from bs4 import BeautifulSoup
import random
from concurrent.futures import ThreadPoolExecutor


class Proxy:
    def __init__(self) -> None:
        self.usable_proxies = []

    def main(self):
        proxylist = self.get_proxies()
        with ThreadPoolExecutor() as executor:
            executor.map(self.extract, proxylist)

        final_choice = random.choice(self.usable_proxies)

        return final_choice

    def get_proxies(self):
        r = requests.get('https://free-proxy-list.net/')
        soup = BeautifulSoup(r.content, 'html.parser')
        table = soup.find('tbody')
        proxies = []
        for row in table:
            if row.find_all('td')[4].text == 'elite proxy':
                proxy = ':'.join([row.find_all('td')[0].text, row.find_all('td')[1].text])
                proxies.append(proxy)
            else:
                pass
        return proxies

    def extract(self, proxy):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0'}
        try:
            r = requests.get('https://www.beeradvocate.com/', headers=headers, proxies={'http': proxy, 'https': proxy},
                             timeout=1)
            if r.status_code == 200:
                self.usable_proxies.append(proxy)

        except requests.ConnectionError as err:
            pass

        return proxy
