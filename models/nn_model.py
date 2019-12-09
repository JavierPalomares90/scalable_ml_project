import pandas
import keras
from keras.models import Sequential
from keras.layers import Dense, Dropout, Activation
from keras.optimizers import SGD
from keras.wrappers.scikit_learn import KerasClassifier
from keras.utils import np_utils
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
from sklearn.preprocessing import LabelEncoder
from sklearn.pipeline import Pipeline


def get_multi_class_classifier_model(num_inputs,num_outputs):
    # create model
    model = Sequential()
    # Dense(64) is a fully-connected layer with 64 hidden units.
    # in the first layer, you must specify the expected input data shape:
    model.add(Dense(128, activation='relu',input_shape=(num_inputs,)))
    model.add(Dropout(0.5))
    model.add(Dense(64, activation='relu'))
    model.add(Dropout(0.5))
    model.add(Dense(num_outputs, activation='softmax'))
    # Compile model
    sgd = SGD(lr=0.01, decay=1e-6, momentum=0.9, nesterov=True)
    model.compile(loss='categorical_crossentropy',
                optimizer=sgd,
                metrics=['accuracy'])

    return model

def fit_multi_class_model(model,x_train,y_train,x_test,y_test,num_epochs=20,batch_sz=128,set_verbose=0,num_splits=10,set_shuffle=True):
    #estimator =  KerasClassifier(model,epochs=num_epochs,batch_size=batch_sz,verbose=set_verbose)
    #kfold = KFold(n_splits=num_splits,shuffle=set_shuffle)
    #results = cross_val_score(estimator,X,y,cv=kfold)
    model.fit(x_train,y_train,epochs = num_epochs, batch_size = batch_sz)
    score = model.evaluate(x_test,y_test,batch_size=batch_sz)
    return score