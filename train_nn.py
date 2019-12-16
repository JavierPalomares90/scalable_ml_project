import utils.utils as utils
import models.nn_model as nn_model
import keras
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from imblearn.over_sampling import SMOTE
from sklearn.metrics import confusion_matrix 
from sklearn.metrics import multilabel_confusion_matrix 
from sklearn.metrics import accuracy_score 
from sklearn.metrics import classification_report 



def get_pitch_data():
    # Read csv files of saved pitch data from the MLB 2016-2019 seasons
    pitch_data = pd.read_csv('raw_pitch_data_all_base_v3.csv', index_col=0)
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
    pitch_data = utils.one_hot_encode(pitch_data,False)

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

def synthetically_balance_data(X, Y_cat, num_pitch_types):
    smote = SMOTE('not majority')
    print('Feature dataset shape pre-SMOTE: {}'.format(X.shape))
    Y = np.argmax(Y_cat, axis = 1)
    X_sm, Y_sm = smote.fit_sample(X, Y)
    Y_sm_cat = keras.utils.to_categorical(Y_sm ,num_classes=num_pitch_types)
    print('Feature dataset shape post-SMOTE: {}'.format(X_sm.shape))
    return X_sm, Y_sm_cat

def get_model_metrics(model,X_test,Y_test,num_pitch_types,pitcher_name):
    Y_pred = model.predict_classes(X_test, verbose=1)
    Y_pred_prob = model.predict(X_test, verbose=1)
    actual = np.argmax(Y_test, axis=1)
    pred = Y_pred
    print('Y_test unique values={}'.format(np.unique(actual)))
    print('Y_pred unique values={}'.format(np.unique(pred)))
    print('Y_pred_prob unique values={}'.format(np.unique(Y_pred_prob)))
    cm = confusion_matrix(actual, pred) 
    cm_ml = multilabel_confusion_matrix(actual, pred) 
    print('Multilabel Confusion Matrix :')
    print(cm_ml) 
    print('Confusion Matrix :')
    print(cm) 
    print('Report : ')
    print(classification_report(actual, pred))
    print('Accuracy Score :',accuracy_score(actual, pred)) 
    '''
    #
    # Create and save confusion matrix figure
    #
    normalize = True
    title='{} Confusion Matrix'.format(pitcher_name)
    label_names = [ 'UN','FF','FS','FC',
                    'SL','SC','CH','CB',
                    'KC','KN','FO','EP',
                    'GY','IN','AS','PO']
    print('Target names: {}'.format(label_names))
    
    cmap = plt.get_cmap('Blues')
    plt.figure(figsize=(8, 6))
    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(title)
    plt.colorbar()

    if target_names is not None:
        tick_marks = np.arange(len(target_names))
        plt.xticks(tick_marks, target_names, rotation=45)
        plt.yticks(tick_marks, target_names)

    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]

    thresh = cm.max() / 1.5 if normalize else cm.max() / 2
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        if normalize:
            plt.text(j, i, "{:0.4f}".format(cm[i, j]),
                     horizontalalignment="center",
                     color="white" if cm[i, j] > thresh else "black")
        else:
            plt.text(j, i, "{:,}".format(cm[i, j]),
                     horizontalalignment="center",
                     color="white" if cm[i, j] > thresh else "black")


    plt.tight_layout()
    plt.ylabel('True label')
    plt.xlabel('Predicted label\naccuracy={:0.4f}; misclass={:0.4f}'.format(accuracy, misclass))

    cm_name = 'ConfusionMatrix{}'.format(pitcher_name)
    plt.savefig(cn_name,format='png')
    '''
    return None

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
    #get unique pitch type of entire processed dataset
    unique_pitch_types = pitch_data['p1_pitch_type'].nunique()
    num_pitch_types = int(np.max(pitch_data['p1_pitch_type'].values)) + 1
    print('Entire pitch data set has {} unique pitches w/ max {}: '.format( 
        unique_pitch_types, (num_pitch_types-1)))
    print(pitch_data['p1_pitch_type'].value_counts(normalize=True).nlargest(n=num_pitch_types))
    pd_train,pd_test = split_train_test(pitch_data)
    # get the data for top 3 pitchers
    pd_train_verlander,pd_test_verlander = get_pitcher_data(pd_train,pd_test,434378)
    pd_train_scherzer,pd_test_scherzer= get_pitcher_data(pd_train,pd_test,453286)
    pd_train_porcello,pd_test_porcello= get_pitcher_data(pd_train,pd_test,519144)
    print('Verlander pitch data rows: train=%d, test=%d.' % 
            (len(pd_train_verlander.index), len(pd_test_verlander.index)))
    print('Verlander top training set pitch types: ')
    print(pd_train_verlander['p1_pitch_type'].value_counts(normalize=True).nlargest(n=num_pitch_types))
    print('Verlander top test set pitch types: ')
    print(pd_test_verlander['p1_pitch_type'].value_counts(normalize=True).nlargest(n=num_pitch_types))
    print()
    print('Scherzer pitch data rows: train=%d, test=%d.' % 
            (len(pd_train_scherzer.index), len(pd_test_scherzer.index)))
    print('Scherzer top training set pitch types: ')
    print(pd_train_scherzer['p1_pitch_type'].value_counts(normalize=True).nlargest(n=num_pitch_types))
    print('Scherzer top test set pitch types: ')
    print(pd_test_scherzer['p1_pitch_type'].value_counts(normalize=True).nlargest(n=num_pitch_types))
    print()
    print('Porcello pitch data rows: train=%d, test=%d.' % 
            (len(pd_train_porcello.index), len(pd_test_porcello.index)))
    print('Porcello top training set pitch types: ')
    print(pd_train_porcello['p1_pitch_type'].value_counts(normalize=True).nlargest(n=num_pitch_types))
    print('Porcello top test set pitch types: ')
    print(pd_test_porcello['p1_pitch_type'].value_counts(normalize=True).nlargest(n=num_pitch_types))
    print()

    # Lastly drop season and pitch_id columns
    pd_train,pd_test = drop_season_pitch_id_cols(pd_train,pd_test)
    pd_train_verlander,pd_test_verlander = drop_season_pitch_id_cols(pd_train_verlander,pd_test_verlander)
    pd_train_scherzer,pd_test_scherzer = drop_season_pitch_id_cols(pd_train_scherzer,pd_test_scherzer)
    pd_train_porcello,pd_test_porcello = drop_season_pitch_id_cols(pd_train_porcello,pd_test_porcello)

    # get the NN data for Verlander
    print('Justin Verlander (id=434378):')
    X_test_verlander,Y_test_verlander = get_X_Y(pd_test_verlander,num_pitch_types)
    X_train_verlander,Y_train_verlander = get_X_Y(pd_train_verlander,num_pitch_types)
    num_cols = len(X_test_verlander.iloc[0,:])

    # train the model for verlander
    model_verlander = nn_model.get_multi_class_classifier_model(num_cols,num_pitch_types)
    score,model_verlander=nn_model.fit_multi_class_model(model_verlander,
            X_train_verlander,Y_train_verlander,
            X_test_verlander,Y_test_verlander,
            save_location='verlander.h5')
    print()
    print('Verlander model training complete with score: {}'.format(score))
    print('Verlander model model metrics for predicting 2019 season data...')
    get_model_metrics(model_verlander,X_test_verlander,Y_test_verlander,num_pitch_types,'Verlander')
    print()

    model_verlander_cw = nn_model.get_multi_class_classifier_model(num_cols,num_pitch_types)
    score,model_verlander_cw=nn_model.fit_multi_class_model(model_verlander_cw,
            X_train_verlander,Y_train_verlander,
            X_test_verlander,Y_test_verlander,
            weight_classes=True, save_location='verlander_cw.h5')
    print()
    print('Verlander (cw) model training complete with score: {}'.format(score))
    print('Verlander (cw) model model metrics for predicting 2019 season data...')
    get_model_metrics(model_verlander_cw,X_test_verlander,Y_test_verlander,num_pitch_types,'Verlander')
    print()

    X_train_verlander_sm,Y_train_verlander_sm = synthetically_balance_data(
            X_train_verlander, Y_train_verlander, num_pitch_types)
    model_verlander_sm = nn_model.get_multi_class_classifier_model(num_cols,num_pitch_types)
    score,model_verlander_sm=nn_model.fit_multi_class_model(model_verlander_sm,
            X_train_verlander_sm,Y_train_verlander_sm,
            X_test_verlander,Y_test_verlander,
            weight_classes=True,save_location='verlander_sm.h5')
    print()
    print('Verlander (sm) model training complete with score: {}'.format(score))
    print('Verlander (sm) model model metrics for predicting 2019 season data...')
    get_model_metrics(model_verlander_sm,X_test_verlander,Y_test_verlander,num_pitch_types,'Verlander')
    print()

    # get the NN data for scherzer
    X_test_scherzer,Y_test_scherzer = get_X_Y(pd_test_scherzer,num_pitch_types)
    X_train_scherzer,Y_train_scherzer = get_X_Y(pd_train_scherzer,num_pitch_types)
    num_cols = len(X_test_scherzer.iloc[0,:])

    # train the model for scherzer 
    print('Max Scherzer (pitcher_id=453286):')
    model_scherzer = nn_model.get_multi_class_classifier_model(num_cols,num_pitch_types)
    score,model_scherzer=nn_model.fit_multi_class_model(model_scherzer,
            X_train_scherzer,Y_train_scherzer,
            X_test_scherzer,Y_test_scherzer,
            save_location='scherzer.h5')
    print()
    print('Scherzer model training complete with score: {}'.format(score))
    print('Scherzer model model metrics for predicting 2019 season data...')
    get_model_metrics(model_scherzer,X_test_scherzer,Y_test_scherzer,num_pitch_types,'Scherzer')
    print()

    model_scherzer_cw = nn_model.get_multi_class_classifier_model(num_cols,num_pitch_types)
    score,model_scherzer_cw=nn_model.fit_multi_class_model(model_scherzer_cw,
            X_train_scherzer,Y_train_scherzer,
            X_test_scherzer,Y_test_scherzer,
            weight_classes=True,save_location='scherzer_cw.h5')
    print()
    print('Scherzer (cw) model training complete with score: {}'.format(score))
    print('Scherzer (cw) model model metrics for predicting 2019 season data...')
    get_model_metrics(model_scherzer_cw,X_test_scherzer,Y_test_scherzer,num_pitch_types,'Scherzer')
    print()

    X_train_scherzer_sm,Y_train_scherzer_sm = synthetically_balance_data(
            X_train_scherzer, Y_train_scherzer, num_pitch_types)
    model_scherzer_sm = nn_model.get_multi_class_classifier_model(num_cols,num_pitch_types)
    score,model_scherzer_sm=nn_model.fit_multi_class_model(model_scherzer_sm,
            X_train_scherzer_sm,Y_train_scherzer_sm,
            X_test_scherzer,Y_test_scherzer,
            weight_classes=True,save_location='scherzer_sm.h5')
    print()
    print('Scherzer (sm) model training complete with score: {}'.format(score))
    print('Scherzer (sm) model model metrics for predicting 2019 season data...')
    get_model_metrics(model_scherzer_sm,X_test_scherzer,Y_test_scherzer,num_pitch_types,'Scherzer')
    print()

    # get the NN data for porcello 
    print('Rick Porcello (pitcher_id=519144):')
    X_test_porcello ,Y_test_porcello= get_X_Y(pd_test_porcello,num_pitch_types)
    X_train_porcello,Y_train_porcello= get_X_Y(pd_train_porcello,num_pitch_types)
    num_cols = len(X_test_porcello.iloc[0,:])

    # train the model for porcello 
    model_porcello = nn_model.get_multi_class_classifier_model(num_cols,num_pitch_types)
    score,model_porcello=nn_model.fit_multi_class_model(model_porcello,
            X_train_porcello,Y_train_porcello,
            X_test_porcello,Y_test_porcello,
            save_location='porcello.h5')
    print()
    print('Porcello model training complete with score: {}'.format(score))
    print('Porcello model model metrics for predicting 2019 season data...')
    get_model_metrics(model_porcello,X_test_porcello,Y_test_porcello,num_pitch_types,'Porcello')
    print()

    model_porcello_cw = nn_model.get_multi_class_classifier_model(num_cols,num_pitch_types)
    score,model_porcello_cw=nn_model.fit_multi_class_model(model_porcello_cw,
            X_train_porcello,Y_train_porcello,
            X_test_porcello,Y_test_porcello,
            weight_classes=True,save_location='porcello_cw.h5')
    print()
    print('Porcello (cw) model training complete with score: {}'.format(score))
    print('Porcello (cw) model model metrics for predicting 2019 season data...')
    get_model_metrics(model_porcello_cw,X_test_porcello,Y_test_porcello,num_pitch_types,'Porcello')
    print()

    X_train_porcello_sm,Y_train_porcello_sm = synthetically_balance_data(
            X_train_porcello, Y_train_porcello, num_pitch_types)
    model_porcello_sm = nn_model.get_multi_class_classifier_model(num_cols,num_pitch_types)
    score,model_porcello_sm=nn_model.fit_multi_class_model(model_porcello_sm,
            X_train_porcello_sm,Y_train_porcello_sm,
            X_test_porcello,Y_test_porcello,
            weight_classes=True,save_location='porcello_sm.h5')
    print()
    print('Porcello (sm) model training complete with score: {}'.format(score))
    print('Porcello (sm) model model metrics for predicting 2019 season data...')
    get_model_metrics(model_porcello_sm,X_test_porcello,Y_test_porcello,num_pitch_types,'Porcello')
    print()

    return 

    # get the NN data for all pitch data 
    X_test_all ,Y_test_all= get_X_Y(pd_test,num_pitch_types)
    X_train_all,Y_train_all= get_X_Y(pd_train,num_pitch_types)
    num_cols = len(X_test_all.iloc[0,:])

    # train the model for porcello 
    model_all = nn_model.get_multi_class_classifier_model(num_cols,num_pitch_types)
    score=nn_model.fit_multi_class_model(model_all,X_train_all,Y_train_all,X_test_all,Y_test_all,save_location='all_pd.h5')
    print('All Pitcher model training complete with score: {}'.format(score))

    print('All Pitcher model metrics for predicting Verlander 2019 season data..: ')
    get_model_metrics(model_all,X_test_verlander,Y_test_verlander,num_pitch_types,'AllPitcher')
    print()
    print('All Pitcher model metrics for predicting Scherzer 2019 season data..: ')
    get_model_metrics(model_all,X_test_scherzer,Y_test_scherzer,num_pitch_types,'AllPitcher')
    print()
    print('All Pitcher model metrics for predicting Porcello 2019 season data..: ')
    get_model_metrics(model_all,X_test_porcello,Y_test_porcello,num_pitch_types,'AllPitcher')
    print()

if __name__ == '__main__':
    main()
