"""pandastool.py: Functions to convert SQL queries to Pandas dataframe."""

#pylint: disable-msg=too-many-arguments
#pylint: disable-msg=too-many-locals
#pylint: disable-msg=too-many-statements
#pylint: disable-msg=too-many-branches

import sqlite3
import csv
import numpy as np
import pandas as pd

def escapestring(stng):
    """Returns an escaped string suitable for SQL commands."""
    return stng.replace("'", "''")

# dict_to_insert and dict_to_update take a Python dictionary and turn it into
# an SQL INSERT or UPDATE.
# Types on the values in the dictionary are assumed to be correct!
# I.e. numbers should be floats and ints, and strings should be strings
# (not numbers represented as strings or vice-versa or anything like that)
# This is useful because we can construct a Python dictionary once, then
# use it to either insert or update.
def dict_to_insert(tblname, insdct):
    """Converts a dictionary of fields to be inserted into an SQL INSERT."""
    fields = ''
    values = ''
    for fieldname in insdct:
        fields = fields + ', ' + fieldname
        if isinstance(insdct[fieldname], int):
            values = values + ', ' + str(insdct[fieldname])
        elif isinstance(insdct[fieldname], float):
            values = values + ', ' + str(insdct[fieldname])
        elif isinstance(insdct[fieldname], str):
            values = values + ", '" + escapestring(insdct[fieldname]) + "'"
        else:
            # this else should never happen
            tcba = type(insdct[fieldname])
            print('error: unrecognized type for:', tcba)
    sql = 'INSERT INTO ' + tblname + '(' + fields[2:] + ') VALUES (' + values[2:] + ');'
    return sql

def dict_to_update(tblname, updatedct, whereclause):
    """Converts a dictionary of fields to be updated into an SQL UPDATE."""
    setstmt = ''
    for fieldname in updatedct:
        setstmt = setstmt + ', ' + fieldname + ' = '
        if isinstance(updatedct[fieldname], int):
            setstmt = setstmt + str(updatedct[fieldname])
        elif isinstance(updatedct[fieldname], float):
            setstmt = setstmt + str(updatedct[fieldname])
        elif isinstance(updatedct[fieldname], str):
            setstmt = setstmt + "'" + escapestring(updatedct[fieldname]) + "'"
        else:
            # this else should never happen
            tcba = type(updatedct[fieldname])
            print('error: unrecognized type for:', tcba)
    sql = 'UPDATE ' + tblname + ' SET ' + setstmt[2:] + ' WHERE ' + whereclause + ';'
    return sql

# csv_to_database pulls a CSV file into a database.
# This function does NOT create the fields!
# It can't do that because it doesn't know the types for each column!
# YOU must create the table with the correct field names and types before calling this function.
# Column names much match field names in the CSV.
def csv_to_database(csv_file, tblname, rename_fields, dbcu):
    """Pulls a CSV file into a database table. The table needs to already be created."""
    fieldnames = []
    rownum = 0
    with open(csv_file, newline='') as csvfile:
        thereader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for rowdat in thereader:
            if rownum == 0:
                for info in rowdat:
                    if info in rename_fields:
                        fieldnames.append(rename_fields[info])
                    else:
                        fieldnames.append(info)
            else:
                insdict = {}
                colnum = 0
                for info in rowdat:
                    insdict[fieldnames[colnum]] = info
                    colnum = colnum + 1
                sql = dict_to_insert(tblname, insdict)
                dbcu.execute(sql)
            rownum = rownum + 1

# map_column creates a new column with a value from an existing column mapped through a function
# This is one thing that's easier to do in Pandas than SQL.
# In Pandas this functionality is built-in but here we have to do it ourselves.
def map_colum(dbcu, tblname, primarykey, whereclause, col1, col2, mapfunc):
    """Maps col1 to col 2 by calling mapfunc."""
    # BUGBUG: This function only works for INTEGER primary keys
    sql = 'SELECT ' + primarykey + ', ' + col1 + ' FROM ' + tblname + ' WHERE ' + whereclause + ';'
    stufftodo = []
    for row in dbcu.execute(sql):
        stufftodo.append(row)
    for item in stufftodo:
        newval = mapfunc(item[1])
        updaterec = {col2 : newval}
        subwhere = primarykey + ' = ' + str(item[0])
        sql = dict_to_update(tblname, updaterec, subwhere)
        dbcu.execute(sql)

# sql_to_dataframe is the primary function -- it transforms any SQL query into a Pandas DataFrame!
# Magic! But it only works if you set row_factory = sqlite3.Row on your connection BEFORE
# you create your cursor!
# Otherwise row.keys() in here will fail.
def sql_to_dataframe(dbcu, sql):
    """Takes an SQL query and returns a Pandas dataframe."""
    count = 0
    datlist = []
    for row in dbcu.execute(sql):
        if count == 0:
            columns = []
            for colname in row.keys():
                columns.append(colname)
                datlist.append([])
        position = 0
        for item in row:
            datlist[position].append(item)
            position = position + 1
        count = count + 1
    dfdct = {}
    position = 0
    for colname in columns:
        dfdct[colname] = datlist[position]
        position = position + 1
    return pd.DataFrame(dfdct)

# dbnameize is a helper function for fields that are
# unsuitable for use as DB column names.
def dbnameize(stng):
    """Returns a name suitable for use as a database field name."""
    # make suitable for db field name by making spaces underscores
    # replace dashes with underscores, too, ("father-son") because those mess up the db
    # and apostrophes ("love of one's life")
    # and periods ("U.S. President")
    # and parenthesis ("(MGM)")
    # and plus sign ("Canal+")
    # we also make lower case
    return (((((((stng.replace(' ', '_')).replace('-', '_')).replace("'", '')).replace(
        '.', '')).replace('(', '')).replace(')', '')).replace('+', '')).lower()

def get_field_names(dbcu, tblname):
    """Return list of fields from a DB table"""
    # Don't know any other way to get the field names except to do a SELECT *
    # Won't work if table is empty!
    sql = 'SELECT * FROM ' + tblname + ' WHERE 1 LIMIT 1;'
    columns = []
    for row in dbcu.execute(sql):
        for colname in row.keys():
            columns.append(colname)
    return columns

# This function is like sql_to_dataframe except it just gives us a single number back
# useful if you're just selecting a count of something or the ID number for something.
def sql_to_scalar(dbcu, sql):
    """Return a single value from an SQL query"""
    for row in dbcu.execute(sql):
        for item in row:
            result = item
    return result

def set_up_example_db(csv_file):
    """Pull in example database. Change this to make your code."""
    exampleconn = sqlite3.connect(':memory:')
    exampleconn.row_factory = sqlite3.Row
    examplecu = exampleconn.cursor()
    examplecu.execute('CREATE TABLE example (    \
                id INTEGER PRIMARY KEY,       \
            );')
    csv_to_database(csv_file, 'example', {'cast':'performers'}, examplecu)
    examplecu.execute('ALTER TABLE example ADD COLUMN revenue_adj_log REAL;')
    map_colum(examplecu, 'example', 'id', '1', 'revenue_adj', 'revenue_adj_log', mylogp1)
    exampleconn.commit()
    return exampleconn, examplecu

