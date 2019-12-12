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
    # Fully connected layers with 256 nodes.
    model.add(Dense(256, activation='relu',input_shape=(num_inputs,)))
    # Dropout of .5 to prevent overfitting
    model.add(Dropout(0.1))
    model.add(Dense(256, activation='relu'))
    model.add(Dropout(0.1))
    model.add(Dense(num_outputs, activation='softmax'))
    # Compile model
    sgd = SGD(lr=0.01, decay=1e-6, momentum=0.9, nesterov=True)
    model.compile(loss='categorical_crossentropy',
                optimizer=sgd,
                metrics=['accuracy'])

    return model

def fit_multi_class_model(model,x_train,y_train,x_test,y_test,num_epochs=200,batch_sz=1024,set_verbose=0,num_splits=10,set_shuffle=True,save_location=None):

    model.fit(x_train,y_train,epochs = num_epochs, batch_size = batch_sz)
    score = model.evaluate(x_test,y_test,batch_size=batch_sz)
    if(save_location is not None):
        model.save(save_location)
    return score