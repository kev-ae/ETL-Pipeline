from os import times
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = 'sqlite:///new_releases.sqlite'
DATA_FILE = '/home/kev/Coding/py/web_scraping/bs4/Reviews/db/data.json' # location of data

def validation_check(df: pd.DataFrame) -> bool:
    # check for empty dataframe
    if df.empty:
        raise Exception('DataFrame is empty')

    # primary key check
    if pd.Series(df['game_id']).is_unique:
        pass
    else:
        df.drop_duplicates(inplace=True)

    # Check for nulls
    if df.isnull().values.any():
        df.dropna(inplace=True)

    return True

    

if __name__ == "__main__":
    # extract
    with open(DATA_FILE, 'r') as file:
        data = json.load(file)

    today = datetime.date.today()

    game_id = []
    game_title = []
    score = []
    tags = []
    timestamp = []

    for game in data['games']:
        game_id.append(game['game_id'])
        game_title.append(game['game_title'])
        try:
            # If games have low reviews, it will show up as 'num user reviews'. Get rid by replacing with None
            opinion = int(game['overall_score'][0]) if game['overall_score'] != None else None
            opinion = None
        except:
            opinion = game['overall_score']
        score.append(opinion)
        tags.append(', '.join(game['tags']))
        timestamp.append(today)

    new_releases_dict = {
        'game_id' : game_id,
        'game_title' : game_title,
        'score' : score,
        'tags' : tags,
        'timestamp' : timestamp
    }

    game_df = pd.DataFrame(new_releases_dict, columns=['game_id', 'game_title', 'score', 'tags', 'timestamp'])

    # validate
    if validation_check(game_df):
        print('Data is valid, proceeding to load stage')

    # load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('new_releases.sqlite')
    cursor = conn.cursor()

    sql_query = '''
    CREATE TABLE IF NOT EXISTS new_releases(
        game_id INT(255),
        game_title VARCHAR(200),
        score VARCHAR(200),
        tags VARCHAR(1000),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (game_id)
    );
    '''
    cursor.execute(sql_query)
    print('Opening/Creating database')

    try:
        game_df.to_sql('new_releases', engine, index=False, if_exists='append')
    except:
        print('Data already exist in the database')

    conn.close()
    print('Connection close')

    # job scheduling