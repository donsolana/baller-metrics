## Baller-Metrics

### Introduction

If you ask me, one of the coolest appilications of Data Science is in sports, asides summary statistics, the field of Sport Analytics deals with the analysis of sport data to reveal insightful patterns that can improve in-game performance, reduce uncertainty in the outcome of games or even uncover overlooked talent. The techniques behind sport analytics are some of the most elegant implementations of mathematical modelling, however, the results are the stuff of headlines.

In this project you will journey with me as we curate a robust dataset that will allow for a number of sport analytics techniques. The dataset will be primarily based on game-by-game data for each active player. Information on each team and player attributes will be combined to this dataset to form a database.

The project follows the follow steps:
* Step 1: Scrape Data
* Step 2: Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: conclusions and recommendation

### Scrape Data

In the `baller_metrics_scrapper.py` script, we scrape items from the html table containing the dataset into a dictionary called `Player Stats`. We then convert the dictionary into a dataframe and finally read this dataframe into a csv file.

#### Description of Data Sources

| Data Set | Format | Description |
| ---      | ---    | ---         |
|[Game by game data for each player](https://www.basketball-reference.com/players/)| Web pages(html)| The dataset contains  game-by-game data for each active players, freely curated and hosted by ***basketball-reference.com***. Total amount of stats wold make up well over 1 million rows scattered across thousands of pages.|
|[Dataset of teams](https://github.com/swar/nba_api)| API | This dataset is from `nba-api` library. The library contains endpoints with updated data and simplied functions for retrieving them|
|[Dataset of players](https://github.com/swar/nba_api)| API| From Same end point above, but contains attribute information on tables.|


### Assess Data

Here we clean the data. To make the dataset sparkly we folow these steps:
1. To add column names
2. Remove rows where players did not play, and rows that only contain missing values
2. Provide a data dictionary outside this notebook. 


### Data Modelling 

Here we will use a datalake architecture with Amazon S3. With the schema below being defined on read. Data lakes are a secure, cost-effective and flexible approach to storing data. Though they do not inherently enforce constraints like relational databases, they make up for this through high availabity and flexible schemas.


1. Fact Table

**game_stats** - conatins unaggragated game level statistics

***columns -key, game_index, name, Date, Age, home_team_id , away_team_id, Form, GS, MP, FG, FGA, FG_pct,  three, threePA, three_pct, FT, FTA, FT_pct, ORB, DRB, TRB, AST, STL, BLK, TOV, PF, PTS, GmSc, plus_minus***

2. Dimensionals Tables

**Players** - contains player attributes

***full_name, first_name, last_name***


**Teams** - contains player attributes

***full_name, id, nickname, city, state, year_founded***


### Data Quality Checks
Here perform two data quality checks
1. We check if our files exist in the bucket
2. We extract the fact table and confirm rows and columns are present


### Data dictionary

##### **Player Stats**

1. ***key*** -   Unique identifier for each entry. Player name + date 
2. ***game_index*** - incremental count of games for each player per season
3.  ***name*** - Player's full name 
4.  ***Date*** - Date when game was played
5.  ***Age*** - Player's Age
6.  ***home_team_id*** - Home team id . Abbrevated name
7.  ***away_team_id*** - Away team id. Abbrevated name
8.  ***Form*** - Oppenent form
9.  ***GS*** - If player started game. Boolean, player started game =1 
10. ***MP*** - Minutes played
11. ***FG*** - Field goals i.e Non 3 point baskets made from open play
12. ***FGA*** - Field Goal Attempts
13. ***FG_pct*** - Percentage of field goals made
14. ***three*** - Three point
15. ***threePA*** - Three point attempts
16. ***three_pct*** - Percentage of three points made
17. ***FT*** - Free throw
18. ***FTA*** - Free throw attempt
19. ***FT_pct*** - Free throw percentage
20. ***ORB*** - Offensive rebounds
21. ***DRB*** - Defensive rebounds
22. ***TRB*** - Total rebounds
23. ***AST*** - Number of assists in game.
24. ***STL*** - Number of Steals made
25. ***BLK*** - Block attempts
26. ***TOV*** - Number of turnovers
27. ***PF***  - Personal Fouls
28. ***PTS*** - Total Points
29. ***GmSc***- Game Score
30. ***plus_minus*** - plus/minus score. A measure of how impactful a player is to his team.

#### **teams**
1. ***full_name*** - Team full name
2. ***id*** - Abbrevated name, foreing key to player stats table
3. ***nickname*** - Team Nickname 
4. ***city*** - Team's City
5. ***state*** - Team's state
6. ***year_founded*** - Founding year

#### **players**
1. ***full_name*** - Team full name
2. ***first_name*** - Team Nickname 
3. ***last_name*** - Team's City

### Conclusion and Recommendation


#### Tools and Technologies
1. `AWS S3` for data storage
2. AWS Boto3 to access s3 client
3. Pandas to manipulate data
4. `Apache Spark` supports S3 connections therefore spark can also be used if the dataset is extended. i.e We accomodate retired players.

#### Data Update Frequency
1. The player_stats table should be played weekly to accomodate for the last matches
2. Other tables can be updated annually to accomodate any unlikely changes in values

#### Future Designs
1. The data was increased by 100x.
	
	Pandas may not be able to handle a 100x increase in data can not process 100x data set, we could consider using a spark cluster hosted `Amazon EMR`.

2. The data populates a dashboard that must be updated on a daily basis by 7am every day.

	`Apache Airflow` can be used to schedule pipeline. `Airflow` supports service level agreements that can help ensure data is uploadex in time.

3. The database needed to be accessed by 100+ people.

	We can use `Amazon Redshift`. Redshift allows for over 500 connections.
    
#### To do:
1. Automate updates
2. Add WNBA and retired players.
3. It is very likely that the scrapper will time out before it scrapes all pages for best result we can manually batch the scrapper by adding an index to `url_list`
