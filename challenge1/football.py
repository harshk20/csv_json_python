import pandas as pd
import json
import requests
import pymysql as mysqlcon
import datetime
import getpass

print ("\nThis program takes sql server details as input" + \
      "\nReads csv data from https://footystats.org/"+ \
      "\nSends data in chunks to pipedream API"+ \
      "\nFinally saves the data in provided sql database")

print("\nProgram has started.")
print(" ---------- SQL Connection Details -----------")
host= input("host: ") 
user = input("user: ")
password = getpass.getpass("password :") 
db = input("Database name: ")

def createTables(cursor) :

    ## Create Team table
    cursor.execute(" CREATE TABLE IF NOT EXISTS `Team` ( " + \
                    " `id` int NOT NULL AUTO_INCREMENT, " + \
                    " `team_name` varchar(255) NOT NULL, " + \
                    " `common_name` varchar(255) NOT NULL UNIQUE, " + \
                    " `season` varchar(255) NOT NULL, " + \
                    " `country` varchar(255) NOT NULL, " + \
                    "  PRIMARY KEY (`id`)" + \
                    " ) ENGINE=InnoDB AUTO_INCREMENT=1" )

    cursor.execute(" CREATE TABLE IF NOT EXISTS `Player` ( " + \
                    " `id` int NOT NULL AUTO_INCREMENT, " + \
                    " `full_name` varchar(255) NOT NULL, " + \
                    " `age` int NOT NULL, " + \
                    " `birthday` varchar(255) NOT NULL, " + \
                    " `league` varchar(255) NOT NULL, " + \
                    " `position` varchar(255) NOT NULL, " + \
                    " `current_club` varchar(255) NOT NULL, " + \
                    " `current_club_id` int, " + \
                    " `nationality` varchar(255) NOT NULL, " + \
                    "  PRIMARY KEY (`id`)," + \
                    "  FOREIGN KEY (current_club_id) REFERENCES Team (id)" + \
                    " ) ENGINE=InnoDB AUTO_INCREMENT=1")

## sql connection
engine = mysqlcon.Connect(host=host, user = user, password = password, cursorclass = mysqlcon.cursors.DictCursor)

def insertInPlayerDB(cursor, data):
    query = 'INSERT INTO `Player` (`full_name`,`age`,`birthday`,`league`,`position`,`current_club`,`current_club_id`,`nationality`) ' + \
            'VALUES ("{full_name}",{age},"{birthday}","{league}","{position}","{Current Club}", (SELECT `id` from `Team` WHERE `common_name` = "{Current Club}") ,"{nationality}") '
    for rec in data['players'] :
        rec['birthday'] = datetime.datetime.fromtimestamp(rec['birthday'])
        cursor.execute(query.format(**rec))

def insertInTeamDB(cursor, data):
    query = "INSERT INTO `Team` (`team_name`,`common_name`,`season`,`country`) " + \
            "VALUES ('{team_name}','{common_name}','{season}','{country}') "
    for rec in data['teams'] :
        cursor.execute(query.format(**rec))

## function to process the teams in chunk of 20s
def processTeams(teamdf, engine) :
    result = teamdf.to_json(orient = "records")
    teams = dict({'teams' : json.loads(result)})
    ## make a post call on pipedream
    print("sending teams to pipedream ... ")
    r = requests.post('https://941368efe8592eb528671635aef75ec8.m.pipedream.net',json=teams)
    if r.status_code != 200 :
       print("rrequest failed, see if the endpoint is running!!")
    insertInTeamDB(cursor, teams)


## function to process players in chunk of 20s
def processPlayers(playerdf, engine):
    result = playerdf.to_json(orient = "records")
    players = dict({'players' : json.loads(result)})
    ## make a post call on pipedream
    print("sending players to pipedream ... ")
    r = requests.post('https://941368efe8592eb528671635aef75ec8.m.pipedream.net',json=players)
    if r.status_code != 200 :
       print("request failed, see if the endpoint is running!!")
    insertInPlayerDB(cursor, players)

with engine:
    with engine.cursor() as cursor :
        cursor.execute("CREATE DATABASE IF NOT EXISTS "+db)
        cursor.execute("USE "+db)
        createTables(cursor)
        # engine.commit()
        ## read team csv file using pandas in chunks of 20s
        with pd.read_csv('https://footystats.org/c-dl.php?type=teams&comp=1625', 
                        header=0,
                        index_col= False,
                        iterator=True,
                        chunksize=20,
                        usecols=['team_name','common_name','season','country']
                        ) as teamReader:
            for teamChunk in teamReader:
                processTeams(teamChunk, cursor)
                ## engine.commit()                    ## We can also keep commiting the chunks to DB

        ## read player csv file using pandas in chunks of 20s
        with pd.read_csv('https://footystats.org/c-dl.php?type=players&comp=1625', 
                        header=0,
                        index_col=False,
                        iterator=True,
                        chunksize=20,
                        usecols=['full_name','age','birthday','league','position','Current Club','nationality']
                        ) as playerReader:
            for playerChunk in playerReader:
                processPlayers(playerChunk, cursor)
                ##engine.commit()                     ## We can also keep commiting the chunks to DB

        ## commit pending changes
        engine.commit()
        print("Program has ended.")
