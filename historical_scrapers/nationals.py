import datetime as dt
from bs4 import BeautifulSoup
import pandas as pd
import xlrd
import numpy as np
import conformer.conform as cf
import re, requests
_CATCH_MARKERS = ['*', '^', '!']
_WC8_URL = 'https://web.archive.org/web/20170703075617/http://usqworldcup.com/scores/'
_WC7_URL = 'https://web.archive.org/web/20150206234411/http://iqaworldcup.com/scores'
_USQ12_FNAME = 'usq_cup12.xlsx'
_COLUMNS = ['Tournament', 'Date', 'Winner', 'Winning_Score', '*1', '^1', '!1',
               'Loser', 'Losing_Score', '*2', '^2', '!2', 'OTS', 'Gametime']

conformer = cf.Conformer()


def parse_usq12_result(row):
    days, pdays, t1, s1, s2, t2, period, c1, c2, c3, _ = row
    if t1 == 'BYE' or t2 == 'BYE':
        return []
    seconds = int(np.round(pdays * 86400))
    time = dt.datetime(1900, 1, 1) + dt.timedelta(days=days - 2) + dt.timedelta(seconds=seconds)
    team1 = [t1, int(s1)]
    team2 = [t2, int(s2)]
    if period == 'Reg':
        ots = 0
    else:
        ots = 1 if period == 'OT' else 2
    if c1 == t1:
        team1.append(True)
        team2.append(False)
    else:
        team2.append(True)
        team1.append(False)
    if c2 == t1:
        team1.append(True)
        team2.append(False)
    elif c2 == t2:
        team2.append(True)
        team1.append(False)
    else:
        team2.append(False)
        team1.append(False)
    if c3 == t1:
        team1.append(True)
        team2.append(False)
    elif c3 == t2:
        team2.append(True)
        team1.append(False)
    else:
        team2.append(False)
        team1.append(False)
    if team1[1] > team2[1]:
        winner = team1
        loser = team2
    else:
        loser = team1
        winner = team2
    return ['US Quidditch Cup 12', time, *winner, *loser, ots, None]


def parse_bracket_12(game, long_conf: dict):
    t1, time, t2 = game
    pattern = r'(?P<name>\D+)\s(?P<score>\d+)(?P<extras>[*^!]*)'
    pattern_time = r'.*(?P<hour>\d):(?P<minute>\d+).*'
    m1 = re.match(pattern, t1)
    m2 = re.match(pattern, t2)
    mt = re.match(pattern_time, time)
    if m1 and m2 and mt:
        name1, score1, extras1 = m1.groups()
        name2, score2, extras2 = m2.groups()
        hour, minute = mt.groups()
        team1 = [conformer.conform(long_conf[name1]), int(score1)]
        team2 = [conformer.conform(long_conf[name2]), int(score2)]
        ots = 0
        for i, catch in enumerate(_CATCH_MARKERS):
            if catch in extras1:
                team1.append(True)
                team2.append(False)
                ots = i
            elif catch in extras2:
                team1.append(False)
                team2.append(True)
                ots = i
            else:
                team1.append(False)
                team2.append(False)
        if team1[1] > team2[1]:
            winner = team1
            loser = team2
        else:
            winner = team2
            loser = team1
        gtime = dt.datetime(2019, 4, 14, hour=int(hour) + 12, minute=int(minute))
        return ['US Quidditch Cup 12', gtime, *winner, *loser, ots, None]
    return []


def parse_wc8_result(idx, time, team1, score1, score2, team2, period, gtime, pitch):
    day_idx = 160
    date = '04/11/2015 ' if idx < day_idx else '04/12/2015 '
    tstamp = dt.datetime.strptime(date + time, '%m/%d/%Y %I:%M %p')
    team1_results, team2_results = [[team1, score1], [team2, score2]]
    for catch in _CATCH_MARKERS:
        if catch in team1 or catch in score1:
            team1_results[0] = team1_results[0].replace(catch, '')
            team1_results[1] = team1_results[1].replace(catch, '')
            team1_results.append(True)
            team2_results.append(False)
        elif catch in team2 or catch in score2:
            team2_results[0] = team2_results[0].replace(catch, '')
            team2_results[1] = team2_results[1].replace(catch, '')
            score2 = score2.replace(catch, '')
            team2_results.append(True)
            team1_results.append(False)
        else:
            team1_results.append(False)
            team2_results.append(False)

    if period == '2OT' or period == 'SD':
        ots = 2
    else:
        ots = 1 if period == 'OT' else 0
    gametime = sum([int(s) * (60 ** (1 - i)) for i, s in enumerate(gtime.split(':'))])
    return [tstamp, *team1_results, *team2_results, ots, gametime]

def parse_wc7_result(idx, team1, score1, team2, score2, description):
    _CATCH_MARKERS = ['*', '^', '!']
    team1_result = [team1, score1]
    team2_result = [team2, score2]
    for i, catch in enumerate(_CATCH_MARKERS):
        if catch in score1:
            team1_result[1] = team1_result[1].replace(catch, '')
            team1_result.append(True)
            team2_result.append(False)
            ots = i
        elif catch in score2:
            team2_result[1] = team2_result[1].replace(catch, '')
            team2_result.append(True)
            team1_result.append(False)
            ots = i
        else:
            team1_result.append(False)
            team2_result.append(False)
    ots = max(ots, 1) if 'OT' in description else ots
    gtime = 0
    date = dt.datetime(year=2014, month=4, day=(5 if idx < 156 else 6))
    tournament = 'IQA World Cup 7'
    winner, loser = (team1_result, team2_result) if int(team1_result[1]) > int(team2_result[1]) else (
    team2_result, team1_result)
    return [tournament, date, *winner, *loser, ots, gtime]


def get_wc8(url) -> list:
    response = requests.get(url)
    soup = BeautifulSoup(response.text)
    scores = [[v.text for v in row.findAll('td')] for row in soup.find("table", {"class": "igsv-table"}).findAll('tr')][
             1:]
    score_list = [['USQ World Cup 8', *parse_wc8_result(i, *res)] for i, res in enumerate(scores)]
    for i, result in enumerate(score_list):
        score_list[i][2] = conformer.conform(result[2], prompt=True)
        score_list[i][7] = conformer.conform(result[7], prompt=True)
    return score_list


def get_wc7(url) -> list:
    response = requests.get(url)
    soup = BeautifulSoup(response.text)
    result_soup = [(result.findAll('div', {'class': 'scorebox-body'}),
                    result.find('span', {'style': 'margin-right:10px;float:right;'})) for result in
                   soup.findAll('div', {"class": "scorebox"})]
    results = [[val.text for score in result for val in score.findAll('td')[1:]] + [desc.text] for result, desc in
               result_soup]
    score_list = [parse_wc7_result(idx, *r) for idx, r in enumerate(results)]
    for i, result in enumerate(score_list):
        score_list[i][2] = conformer.conform(result[2], prompt=True)
        score_list[i][7] = conformer.conform(result[7], prompt=True)


def usq_cup12_scraper(fname):
    # Open Workbook
    wb = xlrd.open_workbook(fname)
    scores = []
    # Get Day1 Scores for Flight/Pool A/B/C/D
    for group in ('Flight', 'Pool'):
        for i in ('A', 'B', 'C', 'D'):
            matches = wb.sheet_by_name('{} {} Matches'.format(group, i))
            rankings = wb.sheet_by_name('{} {} Rankings'.format(group, i))
            # Make a dictionary matching the four letter abbreviation to a full team name
            long_conf = {row [1]:row[0] for row in rankings._cell_values[1:] if row[1]!='BYE'}
            cutoff = 0 if i=='A' else 1
            raw = [parse_usq12_result(row) for row in matches._cell_values[1:]]
            for j, result in enumerate(raw):
                raw[j][2] = conformer.conform(long_conf[result[2]], prompt=True)
                raw[j][7] = conformer.conform(long_conf[result[7]], prompt=True)
            scores = [*scores, *raw]

    # Get Day2 Scores (Bracket Play)
    collegiate = wb.sheet_by_name('Sun Collegiate Bracket - Full')
    bracket_scores = []
    values = collegiate._cell_values
    LEFT, RIGHT = (0, len(values[0]) - 1)
    round_no = 32
    round_games = []
    # Two-sided bracket, work games from outer bounds to inner
    while LEFT < RIGHT and round_no > 1:
        round_games.append([val[LEFT] for val in values if val[LEFT]] + [val[RIGHT] for val in values if val[RIGHT]])
        LEFT += 1
        RIGHT -= 1
        round_no = round_no / 2
    # Club bracket, one sided
    club = wb.sheet_by_name('Sun Community Bracket - Full')
    values = club._cell_values
    round_no, i = (16, 0)
    while round_no >= 1:
        round_games.append([val[i] for val in values if val[i]])
        i += 1
        round_no /= 2
    for rd in round_games:
        if len(rd) % 3 == 0:
            bracket_scores = [*bracket_scores, *[rd[i:i + 3] for i in range(0, max(3, len(rd) - 3), 3)]]
        else:
            bracket_scores.append([rd[0], 'P1 9:30 PM', rd[1]])
    scores = [*scores, *[parse_bracket_12(game) for game in bracket_scores]]

    return [r for r in scores if r]


