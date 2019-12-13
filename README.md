# MLB Pitch Prediction
![Verlander - Houston Chronicle](/verlander.jpg)
## Introduction
In the sport of baseball, America’s pastime, the game is won or lost based on the matchup of the pitcher and batter. A typical Major League Baseball (MLB) pitcher has numerous pitch types (like the fastball, curveball, cutter, sinker, etc.) and strike zone locations at their disposal to keep the batter guessing. For each batter, knowing the type of pitch thrown next by the pitcher would be a huge advantage as the hitter can appropriately time the setup of his swing and increase the likelihood of a statistical hit.
 
Using machine learning techniques learned in Dr. Alex Dimakis's Scalable Machine Learning course at UT Austin (follow @AlexGDimakis on Twitter!), we have built several different models that can predict the next pitch type of a particular MLB pitcher (multi-classification), unlike related works that only predict if the next pitch is a fastball or not (binary classification).  Additionally, our aim is to be able to make this prediciton based solely on in-game situational data and pitcher tendencies. 

## Data Collection and Analysis
We gather our data from the gameday statistical data published by MLB Advanced Media website at http://gd2.mlb.com/components/game/mlb/. In addition to standard baseball stats, the data includes detailed pitch tracking data collected from systems like PITCHf/x (camera) and TrackMan (radar) which provides speed and trajectory details of pitches. Standard baseball stats are available from as far back as the late 19th century. The more detailed pitch tracking data is available as far back as the 2008 MLB season.

For our project, we have collected all the available data for pitches thrown during the 2016-2019 seasons from data repository. The data was collected using a python package that parses the data from the repository and writes it to a database. The database maintains a relational model between players, innings, half-innings, games, at bats and pitches allowing us to easily lookup and relate pitches thrown by a specific pitcher. Our plan is to query the pitches thrown by a pitcher, in thrown order. We’ll use the result set as the training and testing datasets for our model.

## Feature Engineering

## Model Analysis
### NN
### XGBoost

## Conclusion

## Future Work
 
