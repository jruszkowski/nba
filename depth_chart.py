from bs4 import BeautifulSoup, SoupStrainer
import urllib3
import pandas as pd
from itertools import combinations
import numpy as np
import datetime

http = urllib3.PoolManager()
url = 'http://www.espn.com/nba/depth'
response = http.request('GET', url)
soup = BeautifulSoup(response.data)

depth_chart_names = {
    'Milwaukee': 'MIL',
    'Cleveland': 'CLE',
    'Atlanta': 'ATL',
    'New Orleans': 'NO',
    'Indiana': 'IND',
    'Dallas': 'DAL',
    'Washington': 'WSH',
    'Charlotte': 'CHA',
    'New York': 'NY',
    'Chicago': 'CHI',
    'Toronto': 'TOR',
    'LA Clippers': 'LAC',
    'San Antonio': 'SA',
    'Brooklyn': 'BKN',
    'Denver': 'DEN',
    'Philadelphia': 'PHI',
    'Utah': 'UTAH',
    'Memphis': 'MEM',
    'Portland': 'POR',
    'Oklahoma City': 'OKC',
    'Sacramento': 'SAC',
    'Detroit': 'DET',
    'Orlando': 'ORL',
    'Boston': 'BOS',
    'LA Lakers': 'LAL',
    'Miami': 'MIA',
    'Phoenix': 'PHX',
    'Minnesota': 'MIN',
    'Houston': 'HOU',
    'Golden State': 'GS'}

def return_int(s):
	if s == '--':
		return 0
	return float(s)


rows = soup.find_all('tr', {"class": ["oddrow", "evenrow"]})

depth_chart = {}
for row in rows:
    if len(row) == 13:
            depth_chart[depth_chart_names[row.a.get_text()]] = [td.string.split("\n")[0] for td in row.find_all('td') if td.string != None][1:]
)[0]: y for (x,y) in plyr_dict.items() if x.split(' ')[1] == 'D/ST'}
