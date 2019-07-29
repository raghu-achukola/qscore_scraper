import pandas as pd
import numpy as np


def can_encode(string):
    try:
        string.encode('utf-8')
        string.encode('iso-8859-1')
        return True
    except:
        return False


def force_encodable(string):
    if can_encode(string):
        return string
    for c in string:
        if not can_encode(c):
            string = string.replace(c,'@')
    return string


def purify(string):
    if type(string) == str:
        return force_encodable(string.replace('\xa0', '').replace("'", "").replace('"', '').replace(',', '').upper().strip())
    else:
        return None


class Conformer:
    def __init__(self, conformers='raw_conf.csv', teams='teams.csv'):
        self.conformer_file = conformers
        self.team_file = teams
        self.conformer_dict = pd.read_csv(conformers, encoding = 'iso-8859-1', index_col = 0).to_dict()['Conformed']
        self.team_dict = pd.read_csv(teams, encoding='iso-8859-1', index_col=0).to_dict()['Name']
        self.source = {k: self.team_dict[v] for k, v in self.conformer_dict.items()}
        for v in set(self.source.values()):
            self.source[purify(v)] = v

    def set_source(self, source):
        self.source = source

    def conform(self, n, purified=False, prompt=False):
        if not purified:
            pure = purify(n)
            return self.conform(pure, True, prompt)
        elif n:
            if n in self.source:
                return self.source[n]
            elif prompt:
                cname = input('Conform {} to ?'.format(n)).strip().replace("'", "").replace('"', '').replace(',', '')
                college = input('College (y/n) ?')[0].upper() == 'Y'
                self.add_team(n, cname, college)
                return self.conform(n, True, False)
            else:
                return None
        else:
            return None

    def get_id(self, team_name):
        if purify(team_name) in self.conformer_dict:
            return self.conformer_dict[purify(team_name)]
        for k, v in self.team_dict.items():
            if v == team_name:
                return k
        return None

    def add_team(self, raw: str, conformed: str, college: bool):
        new_index = max([int(i) for i in self.team_dict.keys()])+1
        self.team_dict[new_index] = conformed
        self.conformer_dict[raw] = new_index
        self.source[raw] = conformed
        self.source[purify(conformed)] = conformed
        with open(self.team_file, 'ab')as t:
            t.write('{},{},{}\n'.format(new_index, conformed, college).encode('iso-8859-1'))
        with open(self.conformer_file, 'ab') as c:
            c.write('{},{}\n'.format(raw, new_index).encode('iso-8859-1'))


