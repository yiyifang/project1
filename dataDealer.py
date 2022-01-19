# -*- coding: utf-8 -*-
"""
Created on Wed Dec  8 16:57:04 2021

@author: nick

"""

import configparser
import pandas as pd
import MySQLdb


cf = configparser.ConfigParser()
cf.read("config.ini")  
secs = cf.sections()
dbItems = cf.items("database")
csvItems = cf.items("csv")

print(dbItems)


def delTable(conn):
    cur = conn.cursor()
    cur.execute("drop table "+dbItems[5][1])
    cur.close() 
    conn.commit()  
    conn.close() 
    

def createTable(conn):
    cur = conn.cursor()
    cur.execute("create table  if not exists  "+dbItems[5][1]+"(`Rank` int(2), `Rating` float(10),\
                `Name` varchar(50), `Subtitle` varchar(500), `Year` int(2),\
                `MinPlayers` int(2), `MaxPlayers` int(2), `BestPlayers` varchar(10),\
                `MinPlayTime` int(2), `MaxPlayTime` int(2), `MinAge` int(2),\
                `Weight` float(10), `Type` varchar(50), PRIMARY KEY(`Rank`))")
    cur.close() 
    conn.commit()  




def createConnection():
    conn= MySQLdb.connect(
        host = dbItems[0][1],
        port = int(dbItems[1][1]),
        user = dbItems[2][1],
        passwd = dbItems[3][1],
        db = dbItems[4][1]
        )
    return conn



def csvToDB():
    df = pd.read_csv(csvItems[0][1], encoding= 'unicode_escape')
    conn = createConnection()
    
    createTable(conn)

    cur = conn.cursor()

    df['Subtitle']= df['Subtitle'].str.replace("'","\\'")
    df['Name']= df['Name'].str.replace("'","\\'")
    

    for i in range(len(df)):
        cur.execute("insert into "+dbItems[5][1]+" values("+str(df['Rank'][i])+", \
                    "+str(df['Rating'][i])+",'"+df['Name'][i]+"','"+str(df['Subtitle'][i])+"',\
                    "+str(df['Year'][i])+","+str(df['MinPlayers'][i])+","+str(df['MaxPlayers'][i])+",\
                    '"+str(df['BestPlayers'][i])+"',"+str(df['MinPlayTime'][i])+","+str(df['MaxPlayTime'][i])+",\
                    "+str(df['MinAge'][i])+","+str(df['Weight'][i])+",'"+df['Type'][i]+"')")  

    cur.close()
    conn.commit()
    conn.close()
    print("Write to MySQL successfully!")

    
def getCSV():
    return pd.read_csv(csvItems[0][1], encoding= 'unicode_escape')

def getMySQL():
    conn = createConnection()
    cur = conn.cursor()
    cur.execute("select * from "+dbItems[5][1])
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=["Rank","Rating","Name","Subtitle","Year","MinPlayers","MaxPlayers"\
                                     ,"BestPlayers","MinPlayTime","MaxPlayTime","MinAge","Weight","Type"])
    cur.close()
    conn.commit()
    conn.close()
    return df
    
    
def getData():
    if cf.get("source", "type") == "CSV":
        return getCSV()
        
    elif cf.get("source", "type") == "MySQL":  
        print("MySQL")
        return getMySQL()
        
    else: 
        return False
        
if __name__ == '__main__':
    csvToDB()
    print("Finish")



