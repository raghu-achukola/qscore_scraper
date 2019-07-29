import re
import requests
from bs4 import BeautifulSoup
import asyncio
from pyppeteer import launch
import pandas as pd
import json
import datetime as dt

_CATCH_MARKERS = ['*', '^', '!']
_LOG_ERROR_FILE = 'error_log.txt'
_LOG_PROGRESS_FILE = 'log.txt'
_EVENTS_LINK = "https://www.usquidditch.org/events/calendar/{}"
_COLUMNS = ['Tournament', 'Date', 'Winner', 'Winning_Score', '*1', '^1', '!1',
               'Loser', 'Losing_Score', '*2', '^2', '!2', 'OTS', 'Gametime']

def clean_soup(soup_val):
    return soup_val.get_text().strip().replace(',', '').replace("'", "").replace('"', '')


def _log_exception(e: Exception, doing: str, tournament: str, extra=''):
    with open(_LOG_ERROR_FILE, 'a') as err:
        err.write('[{}]|Error {}|{}|{}|\n'.format(dt.datetime.now(), e, doing, tournament, extra))


def _log_progress(message: str):
    with open(_LOG_PROGRESS_FILE, 'a') as log:
        log.write(message.strip() + '\n')


def clear_file(file):
    open(file, 'w').close()


def process_input(start, end):
    try:
        str_format = r'^[0-9]*-[0-9]*$'
        if not re.match(str_format, start):
            raise Exception('Invalid format on start')
        if not re.match(str_format, end):
            raise Exception('Invalid format on end')
        start_date = dt.date(*[int(s) for s in start.split('-')], 1)
        end_date = dt.date(*[int(s) for s in end.split('-')], 1)
        if start_date > end_date:
            raise Exception('Start date after end')
        elif end_date > dt.date.today():
            raise Exception('End date in the future')
        elif start_date < dt.date(year=2013, month=6, day=1):
            raise Exception('Start date before USQ records')
        return [(start_date.month, start_date.year), (end_date.month, end_date.year)]
    except Exception as e:
        _log_exception(e, 'obtaining scraper positions', '')
        print(e)
        return None


def get_event_urls(start, end) -> list:
    month, year = start
    month_til, year_til = end
    event_urls = []
    slug = ''
    while year < year_til or (year == year_til and month <= month_til):
        try:
            slug = str(year)
            slug += '0' + str(month) if month // 10 == 0 else str(month)
            print(slug)
            loop = asyncio.get_event_loop()
            tournaments = loop.run_until_complete(
                fetch_data(_EVENTS_LINK.format(slug)))
            if tournaments:
                soup_month = BeautifulSoup(tournaments)
                event_urls = [*event_urls,
                              *['https://www.usquidditch.org' + v['href'] for v in soup_month.findAll('div', {'class': 'event'})]]
            else:
                _log_exception(Exception('No events in month'), 'obtaining event list from page', slug)
                _log_progress('WARNING: No events in month {}'.format(slug))
        except Exception as e:
            _log_exception(e, 'obtaining event list from page', slug)
            _log_progress('FAILURE: Obtaining event list from page {}'.format(slug))
        if month == 12:
            month, year = 1, year + 1
        else:
            month, year = month + 1, year
    return event_urls


@asyncio.coroutine
async def fetch_data(url):
    browser = await launch({"headless": True})
    page = await browser.newPage()
    # ... do stuff with page ...
    await page.goto(url)
    val = await page.content()
    if 'No events in selected timeframe' in val:
        return None
    else:
        await page.waitForSelector('div.event')
        val = await page.content()
    # Dispose context once it's no longer needed
    await browser.close()
    return val


def process_result(result):
    try:
        date, team1, score, team2, gtime = result
        team_vals = process_score(score, team1, team2)
        if not team_vals:
            raise Exception('Forfeit encountered')  # Change this later to raise exception
        winning_team = 1 if team_vals['Team_1'][1] > team_vals['Team_2'][1] else 2
        if winning_team == 1:
            winner = team_vals['Team_1']
            loser = team_vals['Team_2']
        else:
            winner = team_vals['Team_2']
            loser = team_vals['Team_1']
        ots = 0
        search_ot = re.search(r'\s(.*)', gtime)  # Search for \xa0(2OT) or \xa0 (OT)
        if search_ot:
            ots = 2 if re.search(r'2OT', gtime) or re.search(r'SD', gtime) else 1
            gtime = gtime[:search_ot.start()]
        hours, minutes, seconds = gtime.split(':')
        return [date, *winner, *loser, ots, int(hours) * 3600 + int(minutes) * 60 + int(seconds)]
    except Exception as e:
        _log_exception(e, 'processing result', '')
        return []


def process_score(score: str, team1=None, team2=None):
    try:
        result1, result2 = score.split('\xa0-\xa0')
        score1 = int(''.join([char for char in result1 if char not in _CATCH_MARKERS]))
        extras1 = [x in result1 for x in _CATCH_MARKERS]
        extras2 = [x in result2 for x in _CATCH_MARKERS]
        score2 = int(''.join([char for char in result2 if char not in _CATCH_MARKERS]))
        if team1 and team2:
            return {'Team_1': [team1, score1, *extras1], 'Team_2': [team2, score2, *extras2]}
        else:
            raise Exception('Forfeit or invalid Team Names')

    except Exception as e:
        _log_exception(e, 'processing score', score)
        return None


def get_tournament_info(url):
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text)
        # Get TournamentName
        name = soup.find('title').get_text().split('|')[0].strip()
        # print('Parsing Tournament: {}'.format(name))
        return [name, soup]

    except Exception as e:
        _log_exception(e, 'obtaining tournament info', url)
        return [None, None]


def get_roster_info(team_list, soup, tournament_name):
    rosters = {}
    if len(team_list) != len(soup):
        _log_exception(Exception('Mismatched roster lengths'), 'getting roster info', tournament_name)
        _log_progress('FAILURE: getting rosters from tournament {}'.format(tournament_name))
    else:
        for tname, roster in zip(team_list, soup):
            team = [clean_soup(val) for player_soup in roster for val in player_soup.findAll('td')][:-1]
            team_roster = {'Coach': [], 'Players': []}
            for i in range(0, len(team), 2):
                if team[i + 1] == 'Coach':
                    team_roster['Coach'].append(team[i])
                else:
                    team_roster['Players'].append(team[i])
            rosters[tname] = [{tournament_name: team_roster}]
    return rosters


def parse_tournament(url) -> dict:
    name, soup = get_tournament_info(url)
    if soup:
        name = name if name else "Unknown @ {}".format(url)
        try:
            # Get Rosters
            team_list = [clean_soup(team) for team in soup.findAll('a', {'target': '_BLANK'})]
            roster_soup = [roster.findAll('tr')[1:] for roster in soup.findAll('table', 'roster')]
            rosters = get_roster_info(team_list, roster_soup, name)
            if rosters:
                _log_progress('SUCCESS: Rosters parsed for tournament {}'.format(name))
            # Get Scores
            score_soup = soup.findAll('table')[-1].findAll('tr')[1:] if soup.findAll('table') else []
            score_list = [[name, *process_result([clean_soup(v) for v in row.findAll('td')])] for row in score_soup if
                          row]
            scores = list(filter(lambda x: len(x) == 14, score_list))
            if scores:
                _log_progress('SUCCESS: Scores parsed for tournament {}'.format(name))
            return {'Rosters': rosters, 'Scores': scores}
        except Exception as e:
            _log_exception(e, "preparing roster or score information", name)
            return {}
    else:
        return {}


def parse_tournament_list(url_list):
    rosters = {}
    scores = []
    warning = False
    for url in url_list:
        _log_progress('Parsing tournament @ url: {}'.format(url))
        val = parse_tournament(url)
        if val:
            if not val['Scores']:
                _log_progress('WARNING: Scores not parsed for tournament')
                warning = True
            new_roster = val['Rosters']
            if not new_roster:
                _log_progress('WARNING: Rosters not parsed for tournament')
                warning = True
            for team in new_roster:
                if team in rosters:
                    rosters[team].append(new_roster[team])
                else:
                    rosters[team] = new_roster[team]
            scores.append(val['Scores'])
        else:
            _log_progress('FAILURE: Tournament parse @ URL {}'.format(url))
            _log_exception(Exception('Empty parse tournamnet result'), 'parsing tournament', url)
        if warning:
            _log_exception(Exception('Tournament is missing either scores or roster'), 'parsing tournament', url)

    return {'Rosters': rosters, 'Scores': scores}


def store_data(val):
    with open('rosters.json', 'w') as fp:
        json.dump(val['Rosters'], fp)
    columns = ['Tournament', 'Date', 'Winner', 'Winning_Score', '*1', '^1', '!1',
               'Loser', 'Losing_Score', '*2', '^2', '!2', 'OTS', 'Gametime']
    df = pd.DataFrame([score for t in val['Scores'] for score in t], columns=columns)
    df.to_csv('scores.csv')


if __name__ == '__main__':
    scraper_start = input('Enter start month and year for scraper in the format YYYY-MM\n')
    scraper_end = input('Enter end month and year for scraper in the format YYYY-MM\n')
    positions = process_input(scraper_start, scraper_end)
    if positions:
        SCRAPE_FROM, SCRAPE_UNTIL = positions
        clear_file(_LOG_ERROR_FILE)
        clear_file(_LOG_PROGRESS_FILE)
        urls = get_event_urls(SCRAPE_FROM, SCRAPE_UNTIL)
        if urls:
            data = parse_tournament_list(urls)
            store_data(data)
