import utils.utils as utils
import models.nn_model as nn_model
import keras
import pandas as pd
import numpy as np



def get_pitch_data():
    # Read csv files of saved pitch data from the MLB 2016-2019 seasons
    pitch_data = pd.read_csv('raw_pitch_data_all_base_v2.csv', index_col=0)
    print("pitch data loaded")
    return pitch_data

def filter_pitch_data(pitch_data):
    pre_filter_rows = len(pitch_data.index)
    pitch_data = pitch_data[pd.notnull(pitch_data['p1_pitch_type'])]
    post_filter_rows = len(pitch_data.index)

    filter_diff = pre_filter_rows - post_filter_rows
    filter_pcnt = (filter_diff)/pre_filter_rows

    print('Removed Null/NaN labeled pitch types rows, filtered %d of %d rows at %f%%' % (filter_diff, pre_filter_rows, filter_pcnt))
    return pitch_data

def drop_pitch_types(pitch_data):
    #
    # Drop rows with unwanted pitchtypes (including automatic ball/strikes, pitchouts, etc)
    #
    pre_filter_rows = len(pitch_data.index)
    pitch_data = utils.drop_unwanted_pitches(pitch_data)
    post_filter_rows = len(pitch_data.index)

    filter_diff = pre_filter_rows - post_filter_rows
    filter_pcnt = (filter_diff)/pre_filter_rows

    print('Removed rows w/ unwanted pitch types, filtered %d of %d rows at %f%%' % (filter_diff, pre_filter_rows, filter_pcnt))
    return pitch_data

def drop_columns(pitch_data):
    #
    # Drop unwanted dataset columns 
    # 

    # ID columns to drop
    id_cols_to_drop=['p1_pitch_id','p0_pitch_id','pitch_data_id','team_id','game_id',
                    'inning_id','half_inning_id','at_bat_id','gid','b1_id','b1_team_id',
                    'team_abbrev']
    pitch_data = utils.drop_columns_by_list(pitch_data,id_cols_to_drop)
    # Pitch data columns to drop
    pitch_cols_to_drop = ['p0_pitch_seqno', 'p1_pitch_seqno', 'p0_inning', 'result_type',
                          'type_confidence', 'p0_at_bat_o', 'p0_pitch_des', 'nasty']
    pitch_data = utils.drop_columns_by_list(pitch_data, pitch_cols_to_drop)

    # Optional pitchf/x data columns to drop
    #pitchfx_cols_to_drop = ['pitch_count_atbat', 'pitch_count_team', 'start_speed', 'spin_dir',
    #                        'x', 'y', 'sz_top', 'sz_bot', 'pfx_x', 'pfx_z', 'px', 'pz',
    #                        'x0', 'y0', 'z0', 'vx0', 'vy0', 'vz0', 'ax', 'ay', 'az', 'break_y']
    #pitch_data = utils.drop_columns_by_list(pitch_data, pitchfx_cols_to_drop)

    print("dropped cols")
    return pitch_data

def add_run_diff(pitch_data):
    #
    # Create new column of run differential
    #
    pitch_data['run_diff'] = pitch_data['runs_pitcher_team'] - pitch_data['runs_batter_team']
    cols_to_drop=['runs_pitcher_team','runs_batter_team']
    pitch_data = utils.drop_columns_by_list(pitch_data, cols_to_drop)
    print("added run diff")
    return pitch_data

def add_crunch_time(pitch_data):
    #
    # Create new column for crunch time (after 7th inning)
    #
    pitch_data['inning'] = pitch_data['inning'].astype(dtype='int64')
    pitch_data['inning'] = pitch_data['inning'].fillna(0)  # '0' is for unknown inning (Other values are 1-9)
    pitch_data['crunch_time'] = np.where(pitch_data['inning'] > 7, 1, 0)
    cols_to_drop=['inning']
    pitch_data = utils.drop_columns_by_list(pitch_data, cols_to_drop)
    print("added crunch time")
    return pitch_data

def replace_nans(pitch_data):
    #
    # Replace Nulls/NaN values that are left in the remaining object columns
    #
    #
    # Replace Nulls/NaN values that are left in the remaining object columns
    #
    pitch_data['p0_pitch_type'] = pitch_data['p0_pitch_type'].fillna('NP')  # 'NP' is for No Pitch

    pitch_data['result_type_simple'] = pitch_data['result_type_simple'].fillna('X')  # 'X' is for in play

    pitch_data['b1_game_position'] = pitch_data['b1_game_position'].fillna('Unknown')

    pitch_data['b1_bats'] = pitch_data['b1_bats'].fillna('R')  # 'R' is for right handed (Other values are L or S)

    pitch_data['throws'] = pitch_data['throws'].fillna('R')  # 'R' is for right handed (Other value is L)

    #pitch_data['inning'] = pitch_data['inning'].fillna('0')  # '0' is for unknown inning (Other values are 1-9)

    print('Current number of dataframe Null/NaN values: %d' % (pitch_data.isnull().sum().sum()))
    #
    # Fill the rest of Null/NaN values with zero in numeric columns
    #
    replace_dict = {'nasty': 0, 'x': 0, 'y': 0, 'sz_top': 0, 'sz_bot': 0, 'pfx_x': 0, 'pfx_z': 0,
                    'px': 0, 'pz': 0, 'x0': 0, 'y0': 0, 'z0': 0, 'vx0': 0, 'vy0': 0, 'vz0': 0,
                    'ax': 0, 'ay': 0, 'az': 0, 'break_y': 0, 'break_angle': 0, 'break_length': 0,
                    'start_speed': 0, 'end_speed': 0, 'zone': 0, 'outcome': 0, 'spin_rate': 0,
                    'spin_dir': 0, 'pitch_count_at_bat': 0, 'pitch_count_team': 0,
                    'wins': 0, 'losses': 0, 'b1_bat_order': 0}
    pitch_data = pitch_data.fillna(value=replace_dict)

    print('Current number of dataframe Null/NaN values: %d' % (pitch_data.isnull().sum().sum()))

    return pitch_data

def encode_object_data(pitch_data):
    print('Encoding pitch dataframe of shape {}...'.format(pitch_data.shape))

    # Split label column from rest of pitch dataframe then encode
    Y_all = pitch_data.loc[:, 'p1_pitch_type'].copy()
    Y_all = utils.encode_simple_pitch_types(Y_all)

    # Drop label colum from pitch dataframe, then one-hot-encode object columns
    pitch_data = pitch_data.drop('p1_pitch_type', axis=1)
    pitch_data = utils.one_hot_encode(pitch_data)

    # Insert label data back into pitch dataframe
    pitch_data['p1_pitch_type'] = Y_all.copy()

    print('Pitch dataframe encoding complete. New shape: {}'.format(pitch_data.shape))
    return pitch_data

def split_train_test(pitch_data):
    pd_train = pitch_data[pitch_data['season']!=2019].copy()
    pd_test = pitch_data[pitch_data['season']==2019].copy()

    print('Shape of ALL training data set is {}'.format(pd_train.shape))
    print('Shape of ALL test data set is {}'.format(pd_test.shape))

    return pd_train,pd_test

def get_pitcher_data(pd_train,pd_test,pitcher_id):
    pd_train_pitcher = pd_train[pd_train['pitcher_id']==pitcher_id].copy()
    pd_test_pitcher = pd_test[pd_test['pitcher_id']==pitcher_id].copy()
    return pd_train_pitcher,pd_test_pitcher


def drop_season_pitch_id_cols(pd_train,pd_test):
    cols_to_drop=['season','pitcher_id']
    pd_test = utils.drop_columns_by_list(pd_test, cols_to_drop)
    pd_train = utils.drop_columns_by_list(pd_train, cols_to_drop)
    return pd_train,pd_test

def get_X_Y(pitch_data,num_pitch_types):
    X = pitch_data.drop('p1_pitch_type',axis=1).copy()
    Y = pitch_data.loc[:,'p1_pitch_type'].copy()
    Y = keras.utils.to_categorical(Y ,num_classes=num_pitch_types)
    return X,Y



def main():
    pitch_data = get_pitch_data()
    pitch_data = filter_pitch_data(pitch_data)
    pitch_data = drop_pitch_types(pitch_data)
    pitch_data = drop_columns(pitch_data)
    pitch_data = add_run_diff(pitch_data)
    pitch_data = add_crunch_time(pitch_data)
    # Set intended data types of the remaining columns
    pitch_data = utils.set_dtypes(pitch_data)
    pitch_data['season'] = pitch_data['season'].astype(dtype='int64')
    pitch_data['pitcher_id'] = pitch_data['pitcher_id'].astype(dtype='int64')
    pitch_data = replace_nans(pitch_data)
    pitch_data = encode_object_data(pitch_data)
    pd_train,pd_test = split_train_test(pitch_data)
    # get the data for top 3 pitchers
    pd_train_verlander,pd_test_verlander = get_pitcher_data(pd_train,pd_test,434378)
    pd_train_scherzer,pd_test_scherzer= get_pitcher_data(pd_train,pd_test,453286)
    pd_train_porcello,pd_test_porcello= get_pitcher_data(pd_train,pd_test,519144)
    print('Verlander pitch data rows: train=%d, test=%d.' % (len(pd_train_verlander.index), len(pd_test_verlander.index)))
    print('Scherzer pitch data rows: train=%d, test=%d.' % (len(pd_train_scherzer.index), len(pd_test_scherzer.index)))
    print('Porcello pitch data rows: train=%d, test=%d.' % (len(pd_train_porcello.index), len(pd_test_porcello.index)))

    # Lastly drop season and pitch_id columns
    pd_train,pd_test = drop_season_pitch_id_cols(pd_train,pd_test)
    pd_train_verlander,pd_test_verlander = drop_season_pitch_id_cols(pd_train_verlander,pd_test_verlander)
    pd_train_scherzer,pd_test_scherzer = drop_season_pitch_id_cols(pd_train_scherzer,pd_test_scherzer)
    pd_train_porcello,pd_test_porcello = drop_season_pitch_id_cols(pd_train_porcello,pd_test_porcello)

    num_pitch_types = 16
    # get the NN data for Verlander
    X_test_verlander,Y_test_verlander = get_X_Y(pd_test_verlander,num_pitch_types)
    X_train_verlander,Y_train_verlander = get_X_Y(pd_train_verlander,num_pitch_types)
    num_cols = len(X_test_verlander.iloc[0,:])

    # train the model for verlander
    model_verlander = nn_model.get_multi_class_classifier_model(num_cols,num_pitch_types)
    score=nn_model.fit_multi_class_model(model_verlander,X_train_verlander,Y_train_verlander,X_test_verlander,Y_test_verlander,save_location='verlander.h5')

    # get the NN data for scherzer
    X_test_scherzer,Y_test_scherzer = get_X_Y(pd_test_scherzer,num_pitch_types)
    X_train_scherzer,Y_train_scherzer = get_X_Y(pd_train_scherzer,num_pitch_types)
    num_cols = len(X_test_scherzer.iloc[0,:])

    # train the model for scherzer 
    model_scherzer = nn_model.get_multi_class_classifier_model(num_cols,num_pitch_types)
    score=nn_model.fit_multi_class_model(model_scherzer,X_train_scherzer,Y_train_scherzer,X_test_scherzer,Y_test_scherzer,save_location='scherzer.h5')

    # get the NN data for porcello 
    X_test_porcello ,Y_test_porcello= get_X_Y(pd_test_porcello,num_pitch_types)
    X_train_porcello,Y_train_porcello= get_X_Y(pd_train_porcello,num_pitch_types)
    num_cols = len(X_test_porcello.iloc[0,:])

    # train the model for porcello 
    model_porcello = nn_model.get_multi_class_classifier_model(num_cols,num_pitch_types)
    score=nn_model.fit_multi_class_model(model_porcello,X_train_porcello,Y_train_porcello,X_test_porcello,Y_test_porcello,save_location='porcello.h5')




if __name__ == '__main__':
    main()
