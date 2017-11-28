from bs4 import BeautifulSoup, NavigableString, Tag
import urllib2
import pandas as pd
from itertools import combinations, product, islice
import numpy as np
from joblib import Parallel, delayed
import datetime
import sys
from collections import Counter
from copy import deepcopy


pages = [
	'http://www.espn.com/nba/team/stats/_/name/atl',
	'http://www.espn.com/nba/team/stats/_/name/bos',
	'http://www.espn.com/nba/team/stats/_/name/bkn',
	'http://www.espn.com/nba/team/stats/_/name/cha',
	'http://www.espn.com/nba/team/stats/_/name/chi',
	'http://www.espn.com/nba/team/stats/_/name/cle',
	'http://www.espn.com/nba/team/stats/_/name/dal',
	'http://www.espn.com/nba/team/stats/_/name/den',
	'http://www.espn.com/nba/team/stats/_/name/det',
	'http://www.espn.com/nba/team/stats/_/name/gs',
	'http://www.espn.com/nba/team/stats/_/name/hou',
	'http://www.espn.com/nba/team/stats/_/name/ind',
	'http://www.espn.com/nba/team/stats/_/name/lac',
	'http://www.espn.com/nba/team/stats/_/name/lal',
	'http://www.espn.com/nba/team/stats/_/name/mem',
	'http://www.espn.com/nba/team/stats/_/name/mia',
	'http://www.espn.com/nba/team/stats/_/name/mil',
	'http://www.espn.com/nba/team/stats/_/name/min',
	'http://www.espn.com/nba/team/stats/_/name/no',
	'http://www.espn.com/nba/team/stats/_/name/ny',
	'http://www.espn.com/nba/team/stats/_/name/okc',
	'http://www.espn.com/nba/team/stats/_/name/orl',
	'http://www.espn.com/nba/team/stats/_/name/phi',
	'http://www.espn.com/nba/team/stats/_/name/phx',
	'http://www.espn.com/nba/team/stats/_/name/por',
	'http://www.espn.com/nba/team/stats/_/name/sac',
	'http://www.espn.com/nba/team/stats/_/name/sa',
	'http://www.espn.com/nba/team/stats/_/name/tor',
	'http://www.espn.com/nba/team/stats/_/name/utah',
	'http://www.espn.com/nba/team/stats/_/name/wsh']

plyr_dict = {p.split('/')[-1].upper(): {} for p in pages}
total_team_dict = {x: {p.split('/')[-1].upper(): {} for p in pages} for x in ['Game', 'Shooting']}


def convert_totals(s):
	if s == '--':
		return 0
	return float(s)


for base_page in pages:
	team = base_page.split('/')[-1].upper()
	plyr_dict[team] = {'Game': {}, 'Shooting': {}}
	total_team_dict[team] = {'Game': {}, 'Shooting': {}}
	get_page = urllib2.urlopen(base_page)
	soup = BeautifulSoup(get_page, 'html.parser')
	rows = soup.find_all('tr', {"class": ["oddrow", "evenrow"]})
	for row in rows:
		if len(row) == 15:
			if row.a.get_text() in plyr_dict[team]['Game']:
				plyr_dict[team]['Shooting'][row.a.get_text()] = [float(td.string) for td in row.find_all('td') if
														   td.string != None]
			else:
				plyr_dict[team]['Game'][row.a.get_text()] = [float(td.string) for td in row.find_all('td') if
														   td.string != None]

	rows = soup.find_all('tr', {"class": ["total"]})
	for row in rows:
		if len(row) == 15:
			if len(total_team_dict['Game'][team]) > 0:
				total_team_dict['Shooting'][team] = [convert_totals(td.string) for td in row.find_all('td') if td.string!='Totals']
			else:
				total_team_dict['Game'][team] = [convert_totals(td.string) for td in row.find_all('td') if td.string!='Totals']

game_columns = ['GP', 'GS', 'MIN', 'PPG', 'OFFR', 'DEFR', 'RPG', 'APG', 'SPG', 'BPG', 'TPG', 'FPG', 'A/TO', 'PER']
shooting_columns = ['FGM', 'FGA', 'FG',	'3PM', '3PA', '3P', 'FTM', 'FTA', 'FT',	'2PM', '2PA', '2P', 'PPS', 'AFG']

scoring = {
	'RPG': 1.25,
	'APG': 1.5,
	'BPG': 2,
	'SPG': 2,
	'TPG': -0.5,
	'PPG': 1
	}

shooting = {'3PM': 0.5}

df_total = pd.DataFrame.from_dict(total_team_dict['Game']).T
df_shooting = pd.DataFrame.from_dict(total_team_dict['Shooting']).T

df_stats = pd.DataFrame(columns=[list(range(14))])
df_shooting = pd.DataFrame(columns=[list(range(14))])

for team in plyr_dict.keys():
	df_stats = pd.concat([df_stats, pd.DataFrame.from_dict(plyr_dict[team]['Game'], orient='index')])
	df_shooting = pd.concat([df_shooting, pd.DataFrame.from_dict(plyr_dict[team]['Shooting'], orient='index')])

df_stats.columns = game_columns
df_shooting.columns = shooting_columns

df_total.columns = game_columns
df_shooting.columns = shooting_columns

team_total_dict = df_total['PPG'].to_dict()
df_stats = df_stats[[key for key in scoring.keys()]]

df_stats['3PM'] = df_shooting['3PM']

book = sys.argv[1]
min_projection = float(sys.argv[2])
projection_method = int(sys.argv[3])

base_page = 'http://www.espn.com/nba/lines'
get_page = urllib2.urlopen(base_page)
soup = BeautifulSoup(get_page, 'html.parser')

team_abbr = {
	'Milwaukee': 'MIL',
	'Cleveland': 'CLE',
	'Atlanta': 'ATL',
	'New Orleans': 'NO',
	'Indiana': 'IND',
	'Dallas': 'DAL',
	'Washington': 'WSH',
	'Charlotte': 'CHA',
	'NY Knicks': 'NY',
	'Bulls': 'CHI',
	'Toronto': 'TOR',
	'LA': 'LAC',
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

odd_dict = {}
key_rows = soup.find_all("tr", {"class": ["stathead"]})
for row in key_rows:
	if len(row) == 1:
		odd_dict[row.text] = {team_abbr[row.text.split(' at ')[0]]: {'Home': False},
							  team_abbr[row.text.split(' at ')[1].split(',')[0]]: {'Home': True}}

odds_list_home = []
odds_list_away = []
over_under_list = []
team_list_home = []
team_list_away = []
rows = soup.find_all("tr", {"class": ["oddrow", "evenrow"]})
for row in rows:
	for td in row.find_all('td'):
		if isinstance(td.string, NavigableString):
			if td.string == book:
				no_name_home = True
				no_name_away = True
				even_odds = True
				for br in row.find_all('br'):
					if len(str(br.nextSibling).split(':')) == 1:
						odds_list_home += str(br.nextSibling).split(':')
					elif len(str(br.nextSibling).split(':')) == 2 and no_name_home and len(str(br.nextSibling).split(':')[0]) > 1:
						team_list_home.append(str(br.nextSibling).split(':')[0])
						no_name_home = False
					if len(str(br.previousSibling).split(':')) == 1:
						odds_list_away += str(br.previousSibling).split(':')
					elif len(str(br.previousSibling).split(':')) == 2 \
							and no_name_away \
							and len(str(br.previousSibling).split(':')[0]) > 1:
						team_list_away.append(str(br.previousSibling).split(':')[0])
						no_name_away = False
			if td.string != 'EVEN':
				over_under_list.append(td.string)
			if td.string == 'EVEN' and even_odds:
				odds_list_home += ['0']
				odds_list_away += ['0']
				even_odds = False


odds_list_home = [float(x) for x in odds_list_home]
odds_list_away = [float(x) for x in odds_list_away]
assert 0 == sum(odds_list_home) + sum(odds_list_away)

over_under_book = [x for x in over_under_list[::2]]
over_under_level = [x for x in over_under_list[1::2]]
over_under = [(x, y) for (x, y) in zip(over_under_book, over_under_level)]
over_under_wynn = [float(y) for (x, y) in over_under if x == book and y!='N/A']

team_list = team_list_home + team_list_away
odds_list = odds_list_home + odds_list_away
over_under_wynn *= 2
assert len(team_list) == len(odds_list) == len(over_under_wynn)

odds_dict = {x: {'ou': y, 'line': z} for (x, y, z) in zip(team_list, over_under_wynn, odds_list)}

def get_score(team):
	if team in odds_dict.keys():
		return (odds_dict[team]['ou'] - odds_dict[team]['line']) / 2.
	return 0

team_dict = {}
for item in odd_dict.items():
	for team in item[1].keys():
		team_dict[team] = get_score(team)

assert sum(over_under_wynn) / 2 == sum([x for x in team_dict.values()])

dk_name_dict = {'UTAH': 'UTA',
                'PHX': 'PHO',
                'WSH': 'WAS'}

df = pd.read_csv('draftkings.csv').set_index('Name')
df_grouped = df.groupby('teamAbbrev')['AvgPointsPerGame'].sum()
df = df.reset_index().set_index('teamAbbrev')
df['Team Total'] = df_grouped
df['Percent of Total'] = df['AvgPointsPerGame'] / df['Team Total']
for key in dk_name_dict.keys():
    if key in team_dict.keys():
        team_dict[dk_name_dict[key]] = team_dict.pop(key)
    if key in team_total_dict.keys():
        team_total_dict[dk_name_dict[key]] = team_total_dict.pop(key)
df['Projected Score'] = pd.DataFrame.from_dict(team_dict, orient='index')
df['Average Score'] = pd.DataFrame.from_dict(team_total_dict, orient='index')
df['Factor'] = df['Projected Score'] / df['Average Score']
df['Projection'] = df['Percent of Total'] * df['Projected Score']
df = df.reset_index().set_index('Name')
df = pd.merge(df, df_stats, how='left', left_index=True, right_index=True)
df['Stat Projection'] = (df['RPG'] * scoring['RPG'] * df['Factor']
					+ df['APG'] * scoring['APG'] * df['Factor']
					+ df['BPG'] * scoring['BPG']
					+ df['SPG'] * scoring['SPG']
					+ df['TPG'] * scoring['TPG']
					+ df['PPG'] * scoring['PPG'] * df['Factor']
					+ df['3PM'] * shooting['3PM'] * df['Factor'])
df['Stat Projection'] = df['Stat Projection'].fillna(0)

if projection_method == 1:
	df = df[df['Projection'] > min_projection]
elif projection_method == 2:
	df['Projection'] = df['Stat Projection']
	df['Value'] = df['Salary'] / df['Projection']
	df = df[df['Value'] < min_projection]


def date_format(s):
	if len(s) == 2:
		return s
	return '0' + s


date_label = datetime.datetime.now()
df.to_csv('projection_data/dk_output'
	+ str(date_label.year) 
	+ '_'
	+ date_format(str(date_label.month))
	+ '_'
	+ date_format(str(date_label.day))
	+ '.csv')
all_plyr_dict = df.to_dict(orient='index')
position_dict = df.groupby(['Position']).apply(lambda x: x.to_dict(orient='index'))

multiple_position = {
		'C': 'PF/C',
		 'PF': 'PF/C',
		 'PG': ('PG/SF', 'PG/SG'),
		 'SF': ('PG/SF', 'SF/PF', 'SG/SF'),
		 'SG': ('PG/SG', 'SG/SF')}

position_dict_all = {}
position_dict_all['C'] = deepcopy(position_dict['C'])
position_dict_all['PG'] = deepcopy(position_dict['PG'])
position_dict_all['SG'] = deepcopy(position_dict['SG'])
position_dict_all['SF'] = deepcopy(position_dict['SF'])
position_dict_all['PF'] = deepcopy(position_dict['PF'])

for key in multiple_position.keys():
	if isinstance(multiple_position[key], str) and multiple_position[key] in position_dict.keys():
		for item in position_dict[multiple_position[key]].items():
			position_dict_all[key][item[0]] = item[1]
	if isinstance(multiple_position[key], tuple):
		for sub_type in multiple_position[key]:
			if sub_type in position_dict.keys():
				for item in position_dict[sub_type].items():
					position_dict_all[key][item[0]] = item[1]


position_dict_util = {}
position_dict_util['G'] = deepcopy(position_dict_all['PG'])
for item in position_dict_all['SG'].items():
    if item[0] not in position_dict_util['G'].keys():
        position_dict_util['G'][item[0]] = item[1]

position_dict_util['F'] = deepcopy(position_dict_all['PF'])
for item in position_dict_all['SF'].items():
    if item[0] not in position_dict_util['F'].keys():
        position_dict_util['F'][item[0]] = item[1]

position_dict_util['Util'] = deepcopy(all_plyr_dict)

def main(c):
	print (c)
	plyr_3_set = (i for i in product(*[position_dict_util[key].keys() 
			for key in position_dict_util.keys()]) 
			if len(set(i)) == 3)
	plyr_5_set = (i for i in product(*[position_dict_all[key].keys() 
			for key in position_dict_all.keys() 
			if key != 'C']) 
			if len(set(i)) == 4)
	plyr_8_set = ((sum([all_plyr_dict[z]['Stat Projection'] 
			for z in ((c,) + x + y)]), (c,) + x + y) 
			for x,y in product(*[plyr_5_set, plyr_3_set])
		      if len(set((c,) + x + y)) == 8
		      and 49000 < sum([all_plyr_dict[z]['Salary'] for z in ((c,) + x + y)]) <= 50000)

	team_list = {i[0]: [x for x in i[1]] for i in plyr_8_set}
	df = pd.DataFrame.from_dict(team_list, orient='index')
	df = df.sort_index(ascending=False)
	print df.head(1)
	return df.head(1)


if __name__=="__main__":
        start_time = datetime.datetime.now()
        results = Parallel(n_jobs=-1)(delayed(main)(c) for c in position_dict_all['C'].keys())
        df = pd.concat(results).sort_index(ascending=False)
        print (df)
	df.to_csv('dk_lineup.csv')
        print (datetime.datetime.now() - start_time)	
