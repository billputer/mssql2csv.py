#!/usr/bin/env python
# encoding: utf-8
"""
mssql2csv.py

Created by Bill Wiens on 2010-05-04.
"""

import sys, os, getopt
import logging
import csv
import pymssql

def main(argv):
  try:
    opts, args = getopt.getopt(argv, "h:d:t:r:u:p:U", ["help","host=","database=","tables=","result=","user=","password="])
  except getopt.GetoptError:
    usage()
    sys.exit(2)

  # Start values for variables
  database_host = "localhost"
  database_name = "master"
  database_user = "sa"
  database_pass = ""
  database_tables = ""
  dataLimit = 0
  
  for o, a in opts:
    if o == "--help":
      usage()
      sys.exit()
    if o in ("-h", "--host"):
      database_host = a
    if o in ("-d", "--database"):
      database_name = a
    if o in ("-u", "--user"):
      database_user = a
    if o in ("-p", "--password"):
      if len(a) >0:
        database_pass = a
      else:
        database_pass = getpass.getpass(database_user + "'s Password:")

    if o in ("-t","--tables"):
      database_tables = string.split(a, ",")
  dump_db(database_host, database_name, database_user, database_pass, database_tables)

def usage ():
  logging.getLogger().error(u"""
  Python script to dump a MSSQL Server Database to a SQL Script suitable for MySQL
  Requires the freetds library and the pymssql module
  
  mssql2mysql [options] [tables = tablename]
  
  OPTIONS:
  --host=<hostname> .- Specifies the hostname to connect to
  --db=<database_name>   .- Database name to use
  --user=<username>  .- Username with login privilegies
  --password=<password>.- Password for username
  --tables=<tables1>,<table2>.- List of tables that you want to dump
  """)


def dump_db(database_host, database_name, database_user, database_pass, database_tables):
  try:
    os.mkdir(database_name)
    os.chdir(database_name)
  except:
    logging.getLogger().error("Failed to make folder for CSV's: {0}".format(database_name))
    sys.exit(2)
  
  try:
    conn = pymssql.connect(user = database_user, password = database_pass, host = database_host, database = database_name)
    cursor = conn.cursor()
  except:
    logging.getLogger().error("Error: Can't connect to database")
    sys.exit(2)
  
  if len(database_tables) > 0:
    tables = database_tables
  else:
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='Base Table'")
    tables = [table[0] for table in cursor.fetchall()]

  for table_name in tables:
    dump_table(cursor, table_name)

  cursor.close()
  conn.close()
  

def dump_table(cursor, tablename):
  query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{0}'".format(tablename)
  cursor.execute(query)
  schema = cursor.fetchall()
  fieldnames =  [column[0] for column in schema]
  # casts 'ntext' to nvarchar
  selectnames = ["CAST ({0} as nvarchar(max))".format(name) if datatype == 'ntext' else name for name, datatype in schema]
  
  query = "SELECT {0} FROM {1}".format(", ".join(selectnames), tablename)
  cursor.execute(query)
  
  filename = "{0}.csv".format(tablename)
  with open(filename, "wb") as fp:
    writer = csv.writer(fp)
    writer.writerow(fieldnames)
    
    row = cursor.fetchone()
    while row:
      writer.writerow(row)
      row = cursor.fetchone()
        
if __name__ == '__main__':
  logging.basicConfig()
  logging.getLogger().setLevel(logging.DEBUG)
  
  try:
    main(sys.argv[1:])
  except KeyboardInterrupt:
    logging.getLogger().error("Cancelled by user")