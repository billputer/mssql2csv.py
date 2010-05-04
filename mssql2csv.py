#!/usr/bin/env python
# encoding: utf-8
"""
mssql2csv.py

Created by Bill Wiens on 2010-05-04.
"""

import sys, os, getopt
import csv
import pymssql

def main():
  conn = pymssql.connect(host='', user='', password='', database='')
  cursor = conn.cursor()
  dump_db(cursor, "Place20")

def dump_db(cursor, dbname):
  if os.path.exists(dbname):
    raise Error("file already exists: {0}".format(dbname))
  else:
    os.mkdir(dbname)
    os.chdir(dbname)
  
  query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='Base Table'"
  cursor.execute(query)
  for table in cursor.fetchall():
    dump_table(cursor, table[0])

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
  main()