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

##
## Data Pre-processing Utility Fucntions
##
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

def save_dataframe(df,filename):
    df.to_csv(filename)
    return filename

def load_dataframe(filename):
    df = pd.read_csv(filename)
    return df

def split_dataset_into_train_and_test(X,y,test_size=0.2,random_state=42):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size,random_state)
    return X_train, X_test, y_train, y_test  