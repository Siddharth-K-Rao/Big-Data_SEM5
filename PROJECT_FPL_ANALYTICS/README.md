# BIG DATA PROJECT
## FPL ANALYTICS

### Objectives
- The aim of this project is to analyse events occurring during football matches of the English Premier League (EPL).
- We developed a system that will process the streamed matches, events data and rate players using the data. 
- Chemistry coefficients, which signify how well a pair of players interact with each other during a game, is also calculated for all pairs of players who played the game.
- The system also maintains profiles of each player and details of each match.
- The system also uses clustering and regression for predicting player ratings and also the outcomes of matches played on a certain given date.

### Running the Code
1. Start the streaming program on port 6100.
2. Now while the data is ready to be streamed, run metrics.py using spark-submit which will ingest the data being streamed and will perform computations.
3. Run ui.py -> which will internally delete the non required intermediate data, will call metrics.py internally. If a model for clustering is already available then the computation for each of the requests will be quite fast otherwise, it may take some extra time.
