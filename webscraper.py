import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
import time
from lxml.html import fromstring
from itertools import cycle
import traceback

def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    for i in parser.xpath('//tbody/tr')[:10]:
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
            proxies.add(proxy)
    return proxies

def validate_response(response):
    if response.status_code != 200:
        print("Failed to fetch data")
        print(response.status_code)
        return False
    return True

def get_data(url, proxy):
    try:
        response = requests.get(url, proxies={"http": proxy, "https": proxy})
        print("Fetched data")
        return response
    except:
        print("Skipping. Connection error")
        return get_data(url, next(proxy_pool))

proxies = get_proxies()
proxy_pool = cycle(proxies)

standings_url = 'https://fbref.com/en/comps/9/Premier-League-Stats'

years = list(range(2024, 2020, -1))
all_matches = []

for year in years:
    #Request No.1
    #Get the standings table
    proxie = next(proxy_pool)
    data = get_data(standings_url, proxie)
    if not validate_response(data):
        continue
    time.sleep(1)
    soup = BeautifulSoup(data.text)
    standings_table = soup.select('table.stats_table')[0]

    links = standings_table.find_all('a')
    links = [l.get('href') for l in links]
    links = [l for l in links if '/squads/' in l]

    team_urls = [urljoin("https://fbref.com", l) for l in links]

    previous_season = soup.select("a.prev")[0].get("href")
    standings_url = urljoin("https://fbref.com", previous_season)

    for team_url in team_urls:
        team_name = team_url.split('/')[-1].replace('-Stats', ' ').replace('-', ' ')

        #Request No.2
        #Get the matches table
        proxie = next(proxy_pool)
        data = get_data(team_url, proxie)
        if not validate_response(data):
            continue
        time.sleep(1)
        matches = pd.read_html(data.text, match="Scores & Fixtures")
        soup = BeautifulSoup(data.text)
        links = soup.find_all('a')
        links = [l.get('href') for l in links]
        links = [l for l in links if l and '/all_comps/shooting' in l]

        #Request No.3
        #Get the shooting table
        proxie = next(proxy_pool)
        data = get_data(urljoin("https://fbref.com", links[0]), proxie)
        if not validate_response(data):
            continue
        time.sleep(1)
        try:
            shooting = pd.read_html(data.text, match="Shooting")[0]
            shooting.columns = shooting.columns.droplevel()
        except ValueError:
            continue
        try:
            team_data = matches[0].merge(shooting[["Date", "Sh", "SoT", "Dist", "FK", "PK", "PKatt"]], on="Date")
        except ValueError:
            continue

        team_data = team_data[team_data["Comp"] == "Premier League"]
        team_data["Season"] = year
        team_data["Team"] = team_name

        all_matches.append(team_data)

        time.sleep(1)

match_df = pd.concat(all_matches)
match_df.columns = [c.lower() for c in match_df.columns]

print(match_df.head())
match_df.to_csv("match_data.csv")