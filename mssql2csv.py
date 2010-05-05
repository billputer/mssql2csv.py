#!/usr/bin/env python
# encoding: utf-8
"""
mssql2csv.py

Created by Bill Wiens on 2010-05-04.
"""

import sys, os, getopt, getpass
import optparse
import logging
import csv
import pymssql

def main():
  parser = optparse.OptionParser()
  parser.description="""Python script to dump a MSSQL Server Database to folder of CSV files.
	Requires the freetds library and the pymssql module"""
  parser.add_option("-H", "--host", dest="hostname", help="connect to HOSTNAME", metavar="HOSTNAME")
  parser.add_option("-d", "--database", dest="database", help="connect to DATABASE", metavar="DATABASE")
  parser.add_option("-u", "--user", dest="username", help="username to connect with", metavar="USERNAME")
  parser.add_option("-p", "--password", dest="password", help="password to connect with", metavar="PASSWORD")
  parser.add_option("-t", "--tables", dest="tables", help="Comma-separated list of tables to dump", metavar="TABLES")

  (options, args) = parser.parse_args()
  options = vars(options)
    
  if not options['password']:
    options['password'] = getpass.getpass("Enter password:")
  
  if options['tables']:
    options['tables'] = str.split(options['tables'], ",")
    
  dump_db(options['hostname'], options['database'], options['username'], options['password'], options['tables'])


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
    main()
  except KeyboardInterrupt:
    logging.getLogger().error("Cancelled by user")