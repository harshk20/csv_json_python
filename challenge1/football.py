import pandas as pd
import json
import requests
import sqlalchemy

print("Enter SQL connection string in format: user:password@sqldb:port/dbname")
sqlString = input()
connectionString = "mysql+pymysql://" + sqlString
## sql connection
engine = sqlalchemy.create_engine(connectionString)

## function to process the teams in chunk of 20s
def processTeams(teamdf, engine) :
    result = teamdf.to_json(orient = "records")
    teams = dict({'teams' : json.loads(result)})
    ## make a post call on pipedream
    print("sending data to pipedream ... ")
    requests.post('https://39b75a9a157c583460d73e90f4090e64.m.pipedream.net',json=teams)
    teamdf.to_sql("Team", engine, index = False, if_exists = 'append')

## function to process players in chunk of 20s
def processPlayers(playerdf, engine):
    result = playerdf.to_json(orient = "records")
    players = dict({'players' : json.loads(result)})
    ## make a post call on pipedream
    print("sending data to pipedream ... ")
    requests.post('https://39b75a9a157c583460d73e90f4090e64.m.pipedream.net',json=players)
    playerdf.to_sql("Player", engine, index = False, if_exists = 'append')

## read team csv file using pandas in chunks of 20s
with pd.read_csv('https://footystats.org/c-dl.php?type=teams&comp=1625', 
            header=0,
            index_col= False,
            iterator=True,
            chunksize=20,
            usecols=['team_name','common_name','season','country']
            ) as teamReader:
    for teamChunk in teamReader:
        processTeams(teamChunk, engine)

## read player csv file using pandas in chunks of 20s
with pd.read_csv('https://footystats.org/c-dl.php?type=players&comp=1625', 
            header=0,
            index_col=False,
            iterator=True,
            chunksize=20,
            usecols=['full_name','age','birthday','league','position','Current Club','nationality']
            ) as playerReader:
    for playerChunk in playerReader:
        processPlayers(playerChunk, engine)