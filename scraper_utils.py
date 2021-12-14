import pandas as pd
import os.path
import requests
import scraper_utils
import numpy as np
from bs4 import BeautifulSoup
import pdb 
import unidecode
import random


## Remove Team_caller', 'Level_caller', 'Age_caller'
## 'AVG_caller', 'PlayerId_caller', 'Team_other', 'Level_other', 'Age_other',
##'PA_other', PlayerId_other
def clean_batting_cols(batting_data):
    batting_data = batting_data.rename(columns={'Team_x': 'team',
                                        'Level_x': 'Level',
                                        'Age_x': 'age',
                                        'AVG_x': 'BA',
                                        'PA_x': 'PA',
                                        'PlayerId_x': 'PlayerId'})
    batting_data = batting_data.drop(labels=['PlayerId_y', 'Team_y', 'Level_y', 'Age_y', 'AVG_y', 'PA_y'], axis=1)
    return batting_data


##array(['Team_caller', 'Level_caller', 'Age_caller', 'W', 'L',
##       'ERA_caller', 'G', 'GS', 'CG', 'ShO', 'SV', 'BS', 'HLD',
##       'IP_caller', 'TBF', 'H', 'R', 'ER', 'HR', 'BB', 'IBB', 'HBP', 'WP',
##       'BK', 'SO', 'PlayerId_caller', 'Team_other', 'Level_other',
##       'Age_other', 'IP_other', 'K/9', 'BB/9', 'K/BB', 'HR/9', 'K%',
##       'BB%', 'K-BB%', 'AVG', 'WHIP', 'BABIP', 'LOB%', 'ERA_other', 'FIP',
##       'E-F', 'xFIP', 'PlayerId_other'], dtype=object)
def clean_pitching_cols(pitching_data):
    pitching_data = pitching_data.rename(columns={'Team_x': 'team',
                                        'Level_x': 'Level',
                                        'Age_x': 'age',
                                        'ERA_x': 'era',
                                        'IP_x': 'IP',
                                        'PlayerId_x': 'PlayerId'})
    pitching_data = pitching_data.drop(labels=['PlayerId_y','Team_y', 'Level_y', 'Age_y', 'ERA_y', 'IP_y'], axis=1)
    return pitching_data

##array([ 'age',
##      
##      , 'PlayerID',  
##        'ISO', 'Spd', 'BABIP', 'wSB', 'wRC', 'wRAA', 'wOBA', 'wRC+'],
## Create dob column
## Create isPitcher col 
## keep Name, date of birth, isPitcher, team, age, pos, G, PA, AB, R, H, 2B, 3B, HR, RBI, SB, CS, BB, SO, BA, OBP, SLG, 
## 'Name', 'team', 'Level', 'age', 'G', 'AB', 'PA', 'H', '1B', '2B', '3B',
 ##      'HR', 'R', 'RBI', 'BB', 'IBB', 'SO', 'HBP', 'SF', 'SH', 'GDP', 'SB',
 ##      'CS', 'BA', 'PlayerId', 'BB%', 'K%', 'BB/K', 'OBP', 'SLG', 'OPS', 'ISO',
  ##     'Spd', 'BABIP', 'wSB', 'wRC', 'wRAA', 'wOBA', 'wRC+']
## OPS
def prepare_batting_data(batting_data):
    batting_data = batting_data[batting_data['Level'] == 'AAA']
    batting_data['dob'] = [generate_birthday() for k in batting_data.index]
    batting_data['pos'] = [generate_pos() for k in batting_data.index]
    batting_data['age'] = np.nan
    batting_data['isPitcher'] = 0
    batting_data = batting_data.drop(['Level', 'PlayerId','IBB', 'HBP', 'SF', 'SH', 'GDP', 'ISO', 'Spd', 'BABIP', 'wSB', 'wRC', 'wRAA', 'wOBA', 'wRC+', 'BB%', 'K%','BB/K'], axis=1)
    batting_data['BA'] = batting_data['BA'].round(2)
    batting_data['OBP'] = batting_data['OBP'].round(2)
    batting_data['OPS'] = batting_data['OPS'].round(2)
    batting_data['SLG'] = batting_data['SLG'].round(2)
    batting_data['SB'] = batting_data['SB'].round(2)
    return batting_data


##['team', 'Level', 'age', 'W', 'L', 'era', 'G', 'GS', 'CG', 'ShO', 
##       'SV', 'BS', 'HLD', 'IP_caller', 'TBF', 'H', 'R', 'ER', 'HR', 'BB',
##       'IBB', 'HBP', 'WP', 'BK', 'SO', 'PlayerId', 'K/9', 'BB/9', 'K/BB',
##       'HR/9', 'K%', 'BB%', 'K-BB%', 'AVG', 'WHIP', 'BABIP', 'LOB%',
##       'FIP', 'E-F', 'xFIP']
def prepare_pitching_data(pitching_data):
    pitching_data = pitching_data[pitching_data['Level'] == 'AAA']
    pitching_data['dob'] = [generate_birthday() for k in pitching_data.index]
    pitching_data['age'] = np.nan
    pitching_data['isPitcher'] = 1
    pitching_data['pos'] = 'P'
    pitching_data = pitching_data.drop(['Level', 'GS', 'CG', 'ShO', 'SV', 'BS', 'HLD', 'H', 'R', 'ER', 'HR', 'BB', 'IBB',
                                       'HBP', 'WP', 'BK', 'SO', 'K/9', 'BB/9', 'K/BB', 'HR/9', 'K%', 'BB%', 'TBF', 
                                       'K-BB%', 'AVG', 'BABIP', 'LOB%','FIP', 'E-F', 'xFIP', 'PlayerId'], axis=1)
    
    pitching_data['era'] = pitching_data['era'].round(2)
    pitching_data['WHIP'] = pitching_data['WHIP'].round(2)
    return pitching_data
    
    

def alphanum(element):
    clean_string = [s for s in element if s.isalnum() or s.isspace()]

    # rejoin intermediate list into a string
    clean_string = "".join(clean_string)
    return clean_string

def clean_stats_table(table):
    table = table.dropna()
    table = table[pd.to_numeric(table['Rk'], errors='coerce').notnull()]
    table.loc[:,'Name'] = [alphanum(x) for x in table.Name]
    return table
    

## Extract info from player Series object 
## and add requisite rows into the rows list 
def add_player_info_to_df(player, isPitcher, team, rows):
    ## Normalize the name to remove any accents or markers
    name = unidecode.unidecode(player['Name'])
    player_name = name.split()
    player_first = player_name[0]
    player_last = ""
    
    ## Extract the last name from the player
    if (player_name[len(player_name) - 1] == 'Jr') | (player_name[len(player_name) - 1] == 'Sr'):
        player_last = player_name[len(player_name) - 2]
    else:
        player_last = player_name[len(player_name) - 1]
        
    ## Retrieve unique identifier for player in baseball reference links
    player_nickname = f'{player_last[:min(len(player_last), 5)]}{player_first[:min(len(player_first), 2)]}'
    
    ## Get URL based off of nickname 
    player_url = f'https://www.baseball-reference.com/players/{player_last[0]}/{player_nickname}01.shtml'.lower()
    
    ## Some players have '02' at the end of their link instead of '01'
    player_url_two = f'https://www.baseball-reference.com/players/{player_last[0]}/{player_nickname}02.shtml'.lower()
    
    ## Try to reach the player URL
    player_page = requests.get(player_url)
    
    if player_page.status_code == 404:
        ## If it doesn't work, try one more time
        player_url = player_url_two
        player_page = requests.get(player_url)
        ## If it doesn't work a second time, give up. 
        if player_page.status_code == 404:
            return
            
    ## Object to parse the player page
    player_parser = BeautifulSoup(player_page.text, 'html.parser')
    
    ## Extract the birthday of the player if it is there 
    birthday = player_parser.find("span", itemprop="birthDate").get('data-birth', np.nan)
    
    profile_pic_html = player_parser.find("div", class_="media-item")
    
    ## Extract the profile picture link if you can get it
    profile_pic = ""
    if profile_pic_html is not None: 
        profile_pic = profile_pic_html.contents[0]['src']
        
    if not isPitcher: 
        rows.append({"pid": len(rows),
                    "name": name,
                     "dob": birthday,
                     "isPitcher": False,
                     "team": team,
                       "age": player['Age'],
                       "pos": player['Pos'],
                      "G": player['G'],
                      "PA": player['PA'],
                      "AB": player['AB'],
                      "R": player['R'],
                      "H": player['H'],
                      "2B": player['2B'],
                      "3B": player['3B'],
                      "HR": player['HR'],
                      "RBI": player['RBI'],
                      "SB": player['SB'],
                      "CS": player['CS'],
                      "BB": player['BB'],
                      "SO": player['SO'], 
                      "BA": player['BA'], 
                      "OBP": player['OBP'], 
                      "SLG": player['SLG'], 
                      "OPS": player['OPS'], 
                      "OPS_plus": player['OPS+'],
                      "W": np.nan, 
                      "L":np.nan, 
                    "ERA":np.nan,
                     "GS":np.nan,
                     "IP":np.nan, 
                     "WHIP":np.nan})
    else:
         rows.append({"pid": len(rows),
                     "name": name,
                      "dob": birthday,
                      "isPitcher": True,
                       "team": team,
                       "age": player['Age'],
                       "pos": player['Pos'],
                      "G": np.nan,
                      "PA": np.nan,
                      "AB": np.nan,
                      "R": np.nan,
                      "H": np.nan,
                      "2B": np.nan,
                      "3B": np.nan,
                      "HR": np.nan,
                      "RBI": np.nan,
                      "SB": np.nan,
                      "CS": np.nan,
                      "BB": np.nan,
                      "SO": np.nan, 
                      "BA": np.nan, 
                      "OBP": np.nan, 
                      "SLG": np.nan, 
                      "OPS": np.nan, 
                      "OPS_plus": np.nan,
                      "W": player['W'], 
                      "L": player['L'], 
                    "ERA": player['ERA'], 
                     "G": player['G'],
                     "GS": player['GS'],
                     "IP": player['IP'], 
                     "WHIP": player['WHIP']})
            
def generate_birthday():
    years = list(range(1987, 2002))
    months = list(range(1, 12))
    days = list(range(1, 29))
    return f'{random.choice(years)}-{random.choice(months)}-{random.choice(days)}'

def generate_pos():
    pos = ['1B', '2B', 'SS', '3B', 'LF', 'CF', 'RF', 'C']
    return random.choice(pos)
 

    
        
        
        
        