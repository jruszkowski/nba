from bs4 import BeautifulSoup, NavigableString, Tag
import urllib2
import pandas as pd
from itertools import combinations
import numpy as np
from joblib import Parallel, delayed
import datetime
import sys
from collections import Counter

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
shooting_columns = ['FGM', 'FGA', 'FG',	'3PM', '3PA', '3P',	'FTM', 'FTA', 'FT',	'2PM', '2PA', '2P', 'PPS', 'AFG']

scoring = {
    'RPG': 1.2,
    'APG': 1.5,
    'BPG': 3,
    'SPG': 3,
    'TPG': -1,
    'PPG': 1,
    }


df_total = pd.DataFrame.from_dict(total_team_dict['Game']).T
df_stats = pd.DataFrame(columns=[list(range(14))])
for team in plyr_dict.keys():
    df_stats = pd.concat([df_stats, pd.DataFrame.from_dict(plyr_dict[team]['Game'], orient='index')])

df_stats.columns = game_columns
df_total.columns = game_columns
team_total_dict = df_total['PPG'].to_dict()
df_stats = df_stats[[key for key in scoring.keys()]]


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

df = pd.read_csv('fanduel.csv').set_index('Nickname')
df = df[df['Injury Indicator'].isnull()]
df_grouped = df.groupby('Team')['FPPG'].sum()
df = df.reset_index().set_index('Team')
df['Team Total'] = df_grouped
df['Percent of Total'] = df['FPPG'] / df['Team Total']
df['Projected Score'] = pd.DataFrame.from_dict(team_dict, orient='index')
df['Average Score'] = pd.DataFrame.from_dict(team_total_dict, orient='index')
df['Factor'] = df['Projected Score'] / df['Average Score']
df['Projection'] = df['Percent of Total'] * df['Projected Score']
df = df.reset_index().set_index('Nickname')
df = pd.merge(df, df_stats, how='left', left_index=True, right_index=True)
df['Stat Projection'] = (df['RPG'] * scoring['RPG'] * df['Factor']
                    + df['APG'] * scoring['APG'] * df['Factor']
                    + df['BPG'] * scoring['BPG']
                    + df['SPG'] * scoring['SPG']
                    + df['TPG'] * scoring['TPG']
                    + df['PPG'] * scoring['PPG'] * df['Factor'])
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
df.to_csv('projection_data/output' 
	+ str(date_label.year) 
	+ '_'
	+ date_format(str(date_label.month))
	+ '_'
	+ date_format(str(date_label.day))
	+ '.csv')
all_plyr_dict = df.to_dict(orient='index')
position_dict = df.groupby(['Position']).apply(lambda x: x.to_dict(orient='index'))

def create_salary_dict():
	return {salary: {'players': [], 'projection': 0} for salary in range(0,60100,100)}


def add_func(position, plyrs, key):
	plyrs = [x for x in plyrs]
	return sum([position_dict[position][x][key] for x in plyrs])


pg_dict = create_salary_dict()
sg_dict = create_salary_dict()
sf_dict = create_salary_dict()
pf_dict = create_salary_dict()
c_dict = create_salary_dict()

combos = {'PG': 2, 'SG': 2, 'SF': 2, 'PF': 2, 'C': 1}

def create_combo_dictionaries(combo_args):
	position = combo_args[0]
	count = combo_args[1]
	if position == 'PG': 
		for combo in combinations(position_dict[position], count):
			projection = add_func(position, combo, 'Projection')
			salary = add_func(position, combo, 'Salary')
			if projection > pg_dict[salary]['projection']:
				pg_dict[salary]['projection'] = projection
				pg_dict[salary]['players'] = combo
	if position == 'SG': 
		for combo in combinations(position_dict[position], count):
			projection = add_func(position, combo, 'Projection')
			salary = add_func(position, combo, 'Salary')
			if projection > sg_dict[salary]['projection']:
				sg_dict[salary]['projection'] = projection
				sg_dict[salary]['players'] = combo
	if position == 'SF': 
		for combo in combinations(position_dict[position], count):
			projection = add_func(position, combo, 'Projection')
			salary = add_func(position, combo, 'Salary')
			if projection > sf_dict[salary]['projection']:
				sf_dict[salary]['projection'] = projection
				sf_dict[salary]['players'] = combo
	if position == 'PF': 
		for combo in combinations(position_dict[position], count):
			projection = add_func(position, combo, 'Projection')
			salary = add_func(position, combo, 'Salary')
			if projection > pf_dict[salary]['projection']:
				pf_dict[salary]['projection'] = projection
				pf_dict[salary]['players'] = combo
	if position == 'C': 
		for combo in combinations(position_dict[position], count):
			projection = add_func(position, combo, 'Projection')
			salary = add_func(position, combo, 'Salary')
			if projection > c_dict[salary]['projection']:
				c_dict[salary]['projection'] = projection
				c_dict[salary]['players'] = combo



def clean_dict(dict_zeros):
	for key in dict_zeros.keys():
		if dict_zeros[key]['projection'] == 0:
			del dict_zeros[key]
	return dict_zeros


def return_df():
	return df


def total_lineup_all(combo, key):
	return sum([all_plyr_dict[name][key] for name in [item for sublist in combo for item in sublist]])


column_names = ['C', 'PG1', 'PG2', 'SG1', 'SG2', 'SF1', 'SF2', 'PF1', 'PF2']

def create_total_dict(c, c_dict_clean, pg_dict_clean, sg_dict_clean, sf_dict_clean, pf_dict_clean):
        td = {(c_dict_clean[c]['players'], \
                        pg_dict_clean[pg]['players'], \
                        sg_dict_clean[sg]['players'], \
                        sf_dict_clean[sf]['players'], \
                        pf_dict_clean[pf]['players']): \
                {'salary': total_lineup_all((c_dict_clean[c]['players'], \
                                pg_dict_clean[pg]['players'], \
                                sg_dict_clean[sg]['players'], \
                                sf_dict_clean[sf]['players'], \
                                pf_dict_clean[pf]['players']), 'Salary'),\
                 'projection': total_lineup_all((c_dict_clean[c]['players'], \
                                pg_dict_clean[pg]['players'], \
                                sg_dict_clean[sg]['players'], \
                                sf_dict_clean[sf]['players'], \
                                pf_dict_clean[pf]['players']), 'Projection')} \
                for pg in pg_dict_clean.keys() \
                for sg in sg_dict_clean.keys() \
                for sf in sf_dict_clean.keys() \
                for pf in pf_dict_clean.keys() \
                if 59500 < total_lineup_all((c_dict_clean[c]['players'], \
                                        pg_dict_clean[pg]['players'], \
                                        sg_dict_clean[sg]['players'], \
                                        sf_dict_clean[sf]['players'], \
                                        pf_dict_clean[pf]['players']), 'Salary') <= 60000}



	total_dict = {}
	for key in td.keys():
		total_dict[key] = {}
		total_dict[key]['players'] = []
		total_dict[key]['projection'] = td[key]['projection']

        print (len(total_dict))
        for x in total_dict.keys():
                for plyr_tuple in x:
                        total_dict[x]['players'] += list(plyr_tuple)

        total_dict = {y['projection']: y['players'] for x,y in total_dict.items()}
        df = pd.DataFrame.from_dict(total_dict, orient='index').sort_index(ascending=False)
        df = pd.concat([df, df], axis=1)
        dup_column_names = [x + '_2' for x in column_names]
        df.columns = column_names + dup_column_names
        df.replace({dup_column_names[x]: {item[0]: item[1]['Team'] \
                for item in all_plyr_dict.items()} \
                        for x in range(len(dup_column_names))}, inplace=True)
        df = df.reset_index()
        team_count_dict = df[dup_column_names].to_dict(orient='index')
        team_count_dict = {key: max(Counter([team for team in team_count_dict[key].values()]).values()) \
                                for key in team_count_dict.keys()}
        df['Team Count'] = pd.DataFrame.from_dict(team_count_dict, orient='index')
        df = df[df['Team Count'] <= 4].set_index('index')
        df = df[column_names + ['Team Count']].sort_index(ascending = False)
        return df.head(1)

def main():
	for i in combos.items():
		create_combo_dictionaries(i)

        pg_dict_clean = clean_dict(pg_dict)
        sg_dict_clean = clean_dict(sg_dict)
        sf_dict_clean = clean_dict(sf_dict)
        pf_dict_clean = clean_dict(pf_dict)
        c_dict_clean = clean_dict(c_dict)

	c_list = [(c, c_dict_clean, pg_dict_clean, sg_dict_clean, sf_dict_clean, pf_dict_clean) for c in c_dict_clean.keys()]
	print (len(c_list))

	results = Parallel(n_jobs=-1)(delayed(create_total_dict)(*i) for i in c_list)
	df = pd.concat(results).sort_index(ascending=False)
	return df 

if __name__=="__main__":
        start_time = datetime.datetime.now()
	df = main()
	print (df.head(15))
	print (datetime.datetime.now() - start_time)
