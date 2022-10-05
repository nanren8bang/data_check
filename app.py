import mysql.connector
from mysql.connector import Error

import importlib
import os
import time
import configparser
import argparse
import concurrent.futures
import subprocess
from datetime import date
import uuid

def getConnection(myenv):
         
    configParser = configparser.RawConfigParser()
    configParser.read('%s/.my.cnf' % os.path.expanduser('~'))
    if myenv=='DEV':
        try:
           myUser = configParser.get('mysqlDEV', 'user')
           myPassword = configParser.get('mysqlDEV', 'password')
           myHost = configParser.get('mysqlDEV', 'host')
           myDatabase = configParser.get('mysqlDEV', 'database')
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError) as e:
           print('FAILED: Reading ini file for username/password - Reason: %s' % e)

    if myenv=='QA':
        try:
           myUser = configParser.get('mysqlQA', 'user')
           myPassword = configParser.get('mysqlQA', 'password')
           myHost = configParser.get('mysqlQA', 'host')
           myDatabase = configParser.get('mysqlQA', 'database')
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError) as e:
           print('FAILED: Reading ini file for username/password - Reason: %s' % e)
    if myenv=='PROD':
        try:
           myUser = configParser.get('mysqlPROD', 'user')
           myPassword = configParser.get('mysqlPROD', 'password')
           myHost = configParser.get('mysqlPROD', 'host')
           myDatabase = configParser.get('mysqlPROD', 'database')
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError) as e:
           print('FAILED: Reading ini file for username/password - Reason: %s' % e)
    connection = None

    try:
        connection=mysql.connector.connect(
        host   = myHost,
	user   = myUser,
	password = myPassword,
	database = myDatabase
	)
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        pass

    return connection


def runSelect(myenv,query):
    connection=None
    connection=getConnection(myenv)
    cursor = connection.cursor(dictionary=True)

    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print("Error:",e)     
    finally:
        connection.close()


def runSQL(query):
    connection=None
    connection=getConnection('QA')
    cursor = connection.cursor(dictionary=True)

    result = None
    try:
        cursor.execute(query)
        cursor.execute('commit')
        #result = cursor.fetchall()
        #return result
    except Error as e:
        print("Error:",e)     
    finally:
        connection.close()



def main():
    parser = argparse.ArgumentParser(description='Personal information')
    parser.add_argument('--env', dest='environment', type=str, help='DEv, QA or PROD')

    args = parser.parse_args()
    myenv=args.environment
    print('------In '+myenv+' environment-----')
    ##task1
    records=runSelect(myenv,'select count(*)  from variant_drug')[0]['count(*)']
    mysql=""" 
           INSERT INTO dataimportlog (environment,table_name,records,create_date) values ('%s',"variant_drug",%s,curdate()) 
          """%(myenv,records)
    runSQL(mysql)
    print("Totall records in table variant_drug                is:"+str(records))

    ##task2
    records=runSelect(myenv,'select count(*)  from variant_in_vivo')[0]['count(*)']
    mysql=""" 
           INSERT INTO dataimportlog (environment,table_name,records,create_date) values ('%s',"variant_in_vivo",%s,curdate()) 
          """%(myenv,records)
    runSQL(mysql)
    print("Totall records in table variant_in_vivo             is:"+str(records))

    ##task3
    records=runSelect(myenv,'select count(*)  from variant_linked_report')[0]['count(*)']
    mysql=""" 
           INSERT INTO dataimportlog (environment,table_name,records,create_date) values ('%s',"variant_linked_report",%s,curdate()) 
          """%(myenv,records)
    runSQL(mysql)
    print("Totall records in table variant_linked_report       is:"+str(records))

    ##task4
    records=runSelect(myenv,'select count(*)  from variant3')[0]['count(*)']
    mysql=""" 
           INSERT INTO dataimportlog (environment,table_name,records,create_date) values ('%s',"variant3",%s,curdate()) 
          """%(myenv,records)
    runSQL(mysql)
    print("Totall records in table variant3                    is:"+str(records))

    ##task5
    records=runSelect(myenv,'select count(*)  from variant_oos')[0]['count(*)']
    mysql=""" 
           INSERT INTO dataimportlog (environment,table_name,records,create_date) values ('%s',"variant_oos",%s,curdate()) 
          """%(myenv,records)
    runSQL(mysql) 
    print("Totall records in table variant_oos                 is:"+str(records))
    ##task6
    records=runSelect(myenv,'select count(*)  from viral_meta')[0]['count(*)']
    mysql=""" 
           INSERT INTO dataimportlog (environment,table_name,records,create_date) values ('%s',"viral_meta",%s,curdate()) 
          """%(myenv,records)
    runSQL(mysql)
    print("Totall records in table viral_meta                  is:"+str(records))

    ##task7
    records=runSelect(myenv,'select count(*)  from variant_related_resource')[0]['count(*)']
    mysql=""" 
           INSERT INTO dataimportlog (environment,table_name,records,create_date) values ('%s',"variant_related_resource",%s,curdate()) 
          """%(myenv,records)
    runSQL(mysql)
    print("Totall records in table variant_related_resource    is:"+str(records))

    ##-------------------
    print("Totall distinct report_number in table variant3         is:"+str(runSelect(myenv,'select count(distinct report_number) from variant3')[0]['count(distinct report_number)']))
    print("Totall distinct report_number in table variant3_dataset is:"+str(runSelect(myenv,'select count(report_number) from variant3_dataset')[0]['count(report_number)']))


    #====================================ALL DONE=================================
    print('\n---End ODP Data Import check---\n')
  
