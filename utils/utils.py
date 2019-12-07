from sqlalchemy import create_engine
from sqlalchemy.sql import text
from multiprocessing import Pool as ThreadPool
import queries.queries as queries
import numpy as np
import os, argparse, time,glob,random
import pandas as pd
import matplotlib.pyplot as plt 
from sklearn.model_selection import train_test_split


ENGINE_LOGIN='postgresql+psycopg2://postgres:mlb2018@localhost:5532/' #NOTE for this to work, you need to run the cloud sql proxy using port 5532
_PITCH_TYPES_ENCODING={'FA':1,'FF':2,'FT':3,'FC':4,'FO':5,'FS':6,'GY':7,'SI':8,'SF':9,'SL':10,'SC':11,'CH':12,'CB':13,'CU':14,'KC':15,'KN':16,'EP':17,'IN':18,'Unknown':19}
_PITCH_TYPES_DECODING={1:'FA',2:'FF',3:'FT',4:'FC',5:'FO',6:'FS',7:'GY',8:'SI',9:'SF',10:'SL',11:'SC',12:'CH',13:'CB',14:'CU',15:'KC',16:'KN',17:'EP',18:'IN',19:'Unknown'}

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

def get_pitch_data_by_pitcher(pitcher_id):
    engine  = get_engine(ENGINE_LOGIN)
    conn = get_connection(engine)
    pitch_data = pd.read_sql(queries.PITCH_DATA_BY_PITCHER_QUERY.format(pitcher_id=pitcher_id),conn)
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


def one_hot_encode(df):
    all_data = pd.get_dummies(df)
    return all_data

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


default_pd_dtype_map_dict = {
 'pitcher_id': 'object',  # Was 'int64',
 'team_abbrev': 'object',
 'era': 'float64',  # Was 'object',
 'wins': 'int64',
 'losses': 'int64',
 'throws': 'object',
 'b1_id': 'object',  # Was 'int64',
 'b1_team_id': 'object',
 'b1_stand': 'int64',  # Was 'object',
 'b1_height': 'int64',
 'b1_bats': 'object',
 'b1_avg': 'float64',
 'b1_hr': 'int64',
 'b1_rbi': 'int64',
 'b1_bat_order': 'object',  # Was 'float64',
 'b1_game_position': 'object',
 'p1_pitch_id': 'object',
 'p0_pitch_id': 'object',
 'p1_pitch_seqno': 'int64',
 'p0_pitch_seqno': 'int64',  # Was 'float64',
 'p0_inning': 'object',  # Was 'float64',
 'result_type': 'object',
 'result_type_simple': 'object',
 'x': 'float64',
 'y': 'float64',
 'start_speed': 'float64',
 'end_speed': 'float64',
 'sz_top': 'float64',
 'sz_bot': 'float64',
 'pfx_x': 'float64',
 'pfx_z': 'float64',
 'px': 'float64',
 'pz': 'float64',
 'x0': 'float64',
 'y0': 'float64',
 'z0': 'float64',
 'vx0': 'float64',
 'vy0': 'float64',
 'vz0': 'float64',
 'ax': 'float64',
 'ay': 'float64',
 'az': 'float64',
 'break_y': 'float64',
 'break_angle': 'float64',
 'break_length': 'float64',
 'p0_pitch_type': 'object',
 'type_confidence': 'float64',
 'zone': 'object',  # Was 'float64',
 'nasty': 'float64',
 'spin_dir': 'float64',
 'spin_rate': 'float64',
 'outcome': 'int64',  # Was 'float64',
 'inning': 'object',  # Was 'int64',
 'pitch_count_atbat': 'int64',
 'pitch_count_team': 'int64',
 'balls': 'int64',
 'strikes': 'int64',
 'outs': 'int64',  # Was 'object',
 'is_runner_on_first': 'int64',  # Was 'object',
 'is_runner_on_second': 'int64',  # Was 'object',
 'is_runner_on_third': 'int64',  # Was 'object',
 'runs_pitcher_team': 'int64',  # Was 'object',
 'runs_batter_team': 'int64',  # Was 'object',
 'p1_pitch_type': 'object'
}

def set_dtypes(df,dtype_map_dict=None):
    if dtype_map_dict is None:
        dtype_map_dict = default_pd_dtype_map_dict
    # Iterate through dictionary, treating keys as DataFrame column names,
    #  and associated paired values as intended data type.
    for col, datatype in dtype_map_dict.items():
        if col in df.columns:
            # Make sure column is NULL and/or NaN free, otherwise an error is thrown
            if not df[col].isnull().any():
                df[col] = df[col].astype(dtype=datatype)
    return df


def save_dataframe(df,filename):
    df.to_csv(filename)
    return filename

def load_dataframe(filename):
    df = pd.read_csv(filename)
    return df

def drop_pickoffs(df):
    #pickoffs have pitch_type 'PO'
    df = df[df.p0_pitch_type != 'PO']
    df = df[df.p1_pitch_type != 'PO']
    return df

def split_dataset_into_train_and_test(X,y,test_sz=0.2,rand_state=42, do_shuffle=True):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_sz, random_state=rand_state, shuffle=do_shuffle)
    return X_train, X_test, y_train, y_test  
