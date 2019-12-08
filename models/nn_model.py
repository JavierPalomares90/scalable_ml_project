import pandas
from keras.models import Sequential
from keras.layers import Dense
from keras.wrappers.scikit_learn import KerasClassifier
from keras.utils import np_utils
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
from sklearn.preprocessing import LabelEncoder
from sklearn.pipeline import Pipeline


def get_multi_class_classifier_model(num_inputs,num_outputs):
    # create model
    model = Sequential()
    model.add(Dense(2*num_inputs, input_dim=num_inputs, activation='relu'))
    model.add(Dense(num_outputs, activation='softmax'))
    # Compile model
    model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
    return model

def fit_multi_class_model(model,X,y,num_epochs=200,batch_sz=5,set_verbose=0,num_splits=10,set_shuffle=True):
    estimator =  KerasClassifier(model,epochs=num_epochs,batch_size=batch_sz,verbose=set_verbose)
    kfold = KFold(n_splits=num_splits,shuffle=set_shuffle)
    results = cross_val_score(estimator,X,y,cv=kfold)
    print("Baseline: %.2f%% (%.2f%%)" % (results.mean()*100, results.std()*100))