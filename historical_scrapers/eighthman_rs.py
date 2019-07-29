import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import bs4
import re
import requests
import datetime as dt
import qscore_scraper.conformer.conform as cf
# *****************************************************************************
# Historical scraper for all Eighthman regular season data (fall + spring of wc6 season)
# Data Source:  eighthman.com
# Missing Elements:
# ----------------- Rosters
# ----------------- Tournament Names
# ----------------- Game times (Incomplete)
# ----------------- Snitch catches (Sometimes incomplete)
# *****************************************************************************

_CATCH_MARKERS = ['*', '^', '!']
T8M_FALL_URL = "http://www.eighthman.com/results/2012-2013-season/chronological-list-of-all-games-2012-2013-season/chronological-list-of-all-games-fall-2012/"
T8M_SPRING_URL = 'http://www.eighthman.com/results/2012-2013-season/chronological-list-of-all-games-2012-2013-season/chronological-list-of-all-games-spring-2013/'
T8M_WC6D1_URL = 'http://www.eighthman.com/results/world-cup/world-cup-vi-d1-pool-play/'


def parse_wc6_rs_result(tournament, date, t1name, t1score, extras1, t2name, t2score, extras2, minutes=0, seconds=0):
    gtime = int(minutes) * 60 + int(seconds)
    t1 = [t1name, int(t1score)]
    t2 = [t2name, int(t2score)]
    ots = 0
    for i, catch in enumerate(_CATCH_MARKERS):
        if catch in extras1:
            t1.append(True)
            t2.append(False)
            ots = i
        elif catch in extras2:
            t2.append(True)
            t1.append(False)
            ots = i
        else:
            t1.append(False)
            t2.append(False)
    return [tournament, date, *t1, *t2, ots, gtime]


def parse_wc6season(url):
    #Create Conformer object
    conformer = cf.Conformer()
    soup = BeautifulSoup(requests.get(url).text)
    data = [result for result in soup.find("div", {"id": "content-area"}).findAll('p') if not result.find('em')]
    results = []
    date = dt.datetime(year=2012, month=7, day=1)
    for line in data:
        date_match = None
        if line.find('strong'):
            date_match = re.match(r'(?P<month>\d+)/(?P<day>\d+)/(?P<year>\d+)', line.find('strong').text)
            if date_match:
                year = int(date_match.group('year'))
                month = int(date_match.group('month'))
                day = int(date_match.group('day'))
                date = dt.datetime(year=year if year // 100 != 0 else year + 2000, month=month, day=day)

        pattern = r'[(\d)]*\s*(?P<t1name>\D+)\s*(?P<t1score>\d+)(?P<extras1>[*^!]*)\s*[\sv–-]\s*[(\d)]*(?P<t2name>\D+)\s*(?P<t2score>\d+)(?P<extras2>[\*\^\!]*)'
        breaks = line.findAll('br')
        str_line = str(line)
        for br in set(breaks):
            str_line = str_line.replace(str(br), '\n')
        if date_match:
            str_line = str_line.replace(date_match.group(0), '')
        line = BeautifulSoup(str_line)
        for val in line.text.split('\n'):
            # print(val)
            match = re.match(pattern, val)
            if val == '' or val == '\n':
                pass
            elif not match:
                print('{} could not be matched to the pattern'.format(val))
            else:
                groups = match.groups()
                #Conform group names
                groups[0] = conformer.conform(groups[0], purified=False, prompt=True)
                groups[3] = conformer.conform(groups[3], purified=False, prompt=True)
                parsed = parse_wc6_rs_result('Unknown', date, *groups)
                results.append(parsed)
    return results


def parse_wc6_nationals(url):
    conformer = cf.Conformer()
    ps = BeautifulSoup(requests.get(url).text).findAll('p')
    rows = [''.join([str(x) for x in c.contents]) if type(c) == bs4.element.Tag else str(c).strip() for p in ps for c in p.contents]
    results = []
    date = dt.datetime(year =2013, month=4, day=13)
    for row in rows:
        data = row.replace('<br/>', '').split('\n')
        for datum in data:
            if 'Play Ins' in datum:
                date = dt.datetime(year=2013,month=4,day=14)
                continue
            pattern = r'[(\d)]*\s*(?P<t1name>\D+)\s*(?P<t1score>\d+)(?P<extras1>[\*\^\!]*)\s*[\sv–-]\s*[(\d)]*(?P<t2name>\D+)\s*(?P<t2score>\d+)(?P<extras2>[\*\^\!]*)\s*\((?P<minutes>\d*):(?P<seconds>\d*)\)'
            match = re.match(pattern,datum)
            if datum =='' or datum =='\n':
                continue
            if not match:
                abridged_pattern = r'[(\d)]*\s*(?P<t1name>\D+)\s*(?P<t1score>\d+)(?P<extras1>[\*\^\!]*)\s*[\sv–-]\s*[(\d)]*(?P<t2name>\D+)\s*(?P<t2score>\d+)(?P<extras2>[\*\^\!]*)\s*'
                new_match = re.match(abridged_pattern,datum)
                if not new_match:
                    print('{} could not be matched to the pattern'.format(datum))
                    continue
                else:
                    pass
            groups = match.groups() if match else new_match.groups()
            groups[0] = conformer.conform(groups[0])
            groups[3] = conformer.conform(groups[3])
            parsed = parse_wc6_rs_result('IQA World Cup 6', date, *groups)
            results.append(parsed)
    return results
