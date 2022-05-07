import os
import re
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import json
from datetime import datetime
import datetime
import sqlite3

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

    

def run_etl():
    database_location = 'sqlite:///new_releases.sqlite'
    data_file = os.environ['DB_LOC'] # location of database directory

    # extract
    with open(data_file + '/data.json', 'r') as file:
        data = json.load(file)

    today = datetime.date.today()

    game_id = []
    game_title = []
    prices = []
    score = []
    tags = []
    timestamp = []

    for game in data['games']:
        game_id.append(game['game_id'])
        game_title.append(game['game_title'])
        if 'price' in game:
            prices.append(game['price'])
        elif 'discount_original_price' in game:
            prices.append(game['discount_original_price'])
        else:
            prices.append(None)

        # If games have low reviews, it will show up as 'num user reviews'. Get rid by replacing with None
        first_char = game['overall_score'][0] if game['overall_score'] != None else None
        if first_char != None and re.search('[0-9]', first_char):
            opinion = None
        else:
            opinion = game['overall_score']
        score.append(opinion)
        tags.append(', '.join(game['tags']))
        timestamp.append(today)

    new_releases_dict = {
        'game_id' : game_id,
        'game_title' : game_title,
        'price' : prices,
        'score' : score,
        'tags' : tags,
        'timestamp' : timestamp
    }

    game_df = pd.DataFrame(new_releases_dict, columns=['game_id', 'game_title', 'price', 'score', 'tags', 'timestamp'])

    # validate
    if validation_check(game_df):
        print('Data is valid, proceeding to load stage')

    # load
    engine = sqlalchemy.create_engine(database_location)
    conn = sqlite3.connect('new_releases.sqlite')
    cursor = conn.cursor()

    sql_query = '''
    CREATE TABLE IF NOT EXISTS new_releases(
        game_id VARCHAR(200),
        game_title VARCHAR(200),
        price VARCHAR(200),
        score VARCHAR(200),
        tags VARCHAR(1000),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (game_id, timestamp)
    );
    '''
    cursor.execute(sql_query)
    print('Opening/Creating database')

    try:
        game_df.to_sql('new_releases', engine, index=False, if_exists='append')
    except:
        print('Error with inserting into the database')

    conn.close()
    print('Connection close')

if __name__ == '__main__':
    run_etl()