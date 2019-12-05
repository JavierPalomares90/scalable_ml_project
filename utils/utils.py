from sqlalchemy import create_engine
from sqlalchemy.sql import text
from multiprocessing import Pool as ThreadPool
import queries.queries as queries
import numpy as np
import os, argparse, time,glob,random
import pandas as pd

ENGINE_LOGIN='postgresql+psycopg2://postgres:mlb2018@localhost:5532/' #NOTE for this to work, you need to run the cloud sql proxy using port 5532
_PITCH_TYPES_ENCODING={'FA':1,'FF':2,'FT':3,'FC':4,'FO':5,'FS':6,'GY':7,'SI':8,'SF':9,'SL':10,'SC':11,'CH':12,'CB':13,'CU':14,'KC':15,'KN':16,'EP':17}
_PITCH_TYPES_DECODING={1:'FA',2:'FF',3:'FT',4:'FC',5:'FO',6:'FS',7:'GY',8:'SI',9:'SF',10:'SL',11:'SC',12:'CH',13:'CB',14:'CU',15:'KC',16:'KN',17:'EP'}

def get_engine(engine_login):
    engine = create_engine(engine_login)
    return engine

def get_connection(engine):
    return engine.connect()

def get_pitcher_data_summary():
    engine  = get_engine(ENGINE_LOGIN)
    conn = get_connection(engine)
    summaries = pd.read_sql(queries.PITCHER_DATA_SUMMARY_QUERY,conn)
    conn.close()
    return summaries

def get_pitches_in_game(gid):
    engine  = get_engine(ENGINE_LOGIN)
    conn = get_connection(engine)
    pitches = pd.read_sql(queries.PITCHES_IN_GAME_QUERY.format(gid=gid),conn)
    conn.close()
    return pitches

def get_pitches_by_pitcher(pitcher_id):
    engine  = get_engine(ENGINE_LOGIN)
    conn = get_connection(engine)
    pitches = pd.read_sql(queries.PITCHES_BY_PITCHER_ID_QUERY.format(pitcher_id=pitcher_id),conn)
    conn.close()
    return pitches

def get_pitches_with_batter(pitcher_id):
    engine  = get_engine(ENGINE_LOGIN)
    conn = get_connection(engine)
    pitches = pd.read_sql(queries.PITCHES_WITH_BATTER_INFO_QUERY.format(pitcher_id=pitcher_id),conn)
    conn.close()
    return pitches

def get_pitch_data():
    engine  = get_engine(ENGINE_LOGIN)
    conn = get_connection(engine)
    pitch_data = pd.read_sql(queries.PITCH_DATA_QUERY,conn)
    return pitch_data

##
## Data Pre-processing Utility Fucntions
##

def drop_columns_by_list(df, cols_to_drop):
    return df.drop(columns=cols_to_drop,axis=1)

def drop_columns(df, drop_col_csv_filename):
    drop_df = pd.read_csv(drop_col_csv_filename)
    for col in drop_df.columns:
        if col in df.columns:
            df = df.drop(col, 1)  # One (1) is the axis number for columns to drop
    return df

def numericize_columns(df, numeric_col_csv_filename):
    numeric_df = pd.read_csv(numeric_col_csv_filename)
    for col in numeric_df.columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col])
    return df

def categorize_columns(df, categoric_col_csv_filename):
    categoric_df = pd.read_csv(categoric_col_csv_filename)
    for col in categoric_df.columns:
        if col in df.columns:
            df[col] = df[col].astype('category') # Dataframe columns of type object or category are automatic encoded by pd.get_dummys()
    return df

# Categorize the pitch_types
def get_pitch_encoding(pitch_type):
    if pitch_type is None:
        return None
    return _PITCH_TYPES_ENCODING[pitch_type]
    
def get_pitch_decoding(num):
    if pitch_type is None:
        return None
    return _PITCH_TYPES_DECODING[num]

def encode_pitch_types(pitch_types):
    return pitch_types.apply(get_pitch_encoding)

def decode_pitch_types(categories):
    return pitch_types.apply(get_pitch_decoding)

def save_dataframe(df,filename):
    df.to_csv(filename)
    return filename

def load_dataframe(filename):
    df = pd.read_csv(filename)
    return df