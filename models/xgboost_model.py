import xgboost as xgb
from sqlalchemy.sql import text
import pandas as pd
from sklearn import metrics 
import xgboost as xgb
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score,f1_score,recall_score,precision_score
from sklearn.metrics import confusion_matrix


def get_binary_classifier_model():
    #TODO: Complete impl
    pass

def get_multi_class_classifier_model():
    #TODO: Complete impl
    pass

def load_xgboost_model():
    #TODO: Complete impl
    pass

def fit_multi_class_model(model, x_train, y_train,x_test,y_test,
             useTrainCV=False, cv_folds=5, early_stopping_rounds=50,save_location=None):
    
    if useTrainCV:
        xgb_param = model.get_xgb_params()
        xgtrain = xgb.DMatrix(x_train.values, label=y_train)
        cvresult = xgb.cv(xgb_param, xgtrain, 
                          num_boost_round=alg.get_params()['n_estimators'], nfold=cv_folds, early_stopping_rounds=early_stopping_rounds)
        alg.set_params(n_estimators=cvresult.shape[0])
    
    #Fit the algorithm on the data
    model.fit(x_train, y_train)
        
    #Predict training set:
    dtrain_predictions = model.predict(x_train)
    dtrain_predprob = model.predict_proba(x_train)[:,1]
    
    # Predic testing set:
    dtrain_predictions_test = model.predict(x_test)
    dtrain_predprob_test = model.predict_proba(x_test)[:,1]
    
    #Print model report:
    print("\nModel Report")
    print("Accuracy (Train) : {}".format(metrics.accuracy_score(y_train, dtrain_predictions)))
    print("Accuracy (Test) : {}".format(metrics.accuracy_score(y_test, dtrain_predictions_test)))
    
    if save_location is not None:
        # save the model to use later
        model._Booster.save_model(save_location)


def fit_binary_model(model, x_train, y_train,x_test,y_test,
             useTrainCV=True, cv_folds=5, early_stopping_rounds=50,save_location=None):
    
    if useTrainCV:
        xgb_param = model.get_xgb_params()
        xgtrain = xgb.DMatrix(x_train.values, label=y_train)
        cvresult = xgb.cv(xgb_param, xgtrain, 
                          num_boost_round=alg.get_params()['n_estimators'], nfold=cv_folds,
                          metrics='auc', early_stopping_rounds=early_stopping_rounds)
        alg.set_params(n_estimators=cvresult.shape[0])
    
    #Fit the algorithm on the data
    alg.fit(x_train, y_train,eval_metric='auc')
        
    #Predict training set:
    dtrain_predictions = model.predict(x_train)
    dtrain_predprob = model.predict_proba(x_train)[:,1]
    
    # Predic testing set:
    dtrain_predictions_test = model.predict(x_test)
    dtrain_predprob_test = model.predict_proba(x_test)[:,1]
    
    #Print model report:
    print("\nModel Report")
    print("Accuracy (Train) : {}".format(metrics.accuracy_score(y_train, dtrain_predictions)))
    print("AUC Score (Train): {}".format(metrics.roc_auc_score(y_train, dtrain_predprob)))
    print("Accuracy (Test) : {}".format(metrics.accuracy_score(y_test, dtrain_predictions_test)))
    print("AUC Score (Test): {}".format(metrics.roc_auc_score(y_test, dtrain_predprob_test)))
    
    if save_location is not None:
        # save the model to use later
        model._Booster.save_model(save_location)
