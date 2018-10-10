# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 21:09:09 2018

@author: harsh
"""

import sqlite3
import json
from datetime import datetime
import time

data_period = '2015-05'
sql_files = []

connection = sqlite3.connect('{}.db'.format(data_period))
c = connection.cursor()

def format_data(data):
    data = data.replace("\n", "newLineChar").replace("\r", "newLineChar").replace('"', "'")
    return data

def get_parent(parent_ID):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_ID = '{}' LIMIT 1".format(parent_ID)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        return False

def get_parent_score(par_id):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_ID = '{}' LIMIT 1".format(par_id)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        return False
    
def acceptable_data(data):
    if len(data.split(' ')) > 1000 or len(data) <1:
        return False
    elif len(data) > 32000:
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True
                        
def initialize_table():
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_ID TEXT PRIMARY KEY, comment_ID TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")

def transaction_bldr(sql):
    global sql_files
    sql_files.append(sql)
    if len(sql_files) > 1000:
        c.execute('BEGIN TRANSACTION')
        for i in sql_files:
            try:
                c.execute(i)
            except:
                pass
        connection.commit()
        sql_files = []

def sql_insert_replace_comment(commentid, parentid, parent, comment, subreddit, time, score):
    try:
        sql = """UPDATE parent_reply SET parent_ID = ?, comment_ID = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_ID = ?;""".format(parentid, commentid, parent, comment, subreddit, time, score)
        transaction_bldr(sql)
        
    except Exception as e:
        print("replace_comment", str(e))

def sql_insert_has_parent(commentid, parentid, parent, comment, subreddit, time, score):
    try:
        sql = """INSERT INTO parent_reply (parent_ID, comment_ID, parent, comment, subreddit, unix, score) VALUES ("{}", "{}", "{}", "{}", "{}", "{}", "{}");""".format(parentid, commentid, parent, comment, subreddit, time, score)
        transaction_bldr(sql)
        
    except Exception as e:
        print("has parent_comment", str(e))
        
def sql_insert_no_parent(commentid, parentid, comment, subreddit, time, score):
    try:
        sql = """INSERT INTO parent_reply (parent_ID, comment_ID, comment, subreddit, unix, score) VALUES ("{}", "{}", "{}", "{}", "{}", "{}");""".format(parentid, commentid, comment, subreddit, time, score)
        transaction_bldr(sql)
        
    except Exception as e:
        print("no parent_comment", str(e))

    
if __name__ == "__main__":
        initialize_table()
        num_rows = 0
        pairs_of_rows = 0
        
        with open("RC_{}".format(data_period), buffering = 1000) as f:
            for row in f:
                num_rows += 1
                row = json.loads(row)
                parent_id = row['parent_id']
                body = format_data(row['body'])
                created_utc = row['created_utc']
                score = row['score']
                subreddit = row['subreddit']
                parent_data = get_parent(parent_id)
                comment_id = row['id']
                                 
                if score >= 2:
                    if acceptable_data(body):                        
                        existing_score = get_parent_score(parent_id)
                        
                        if existing_score:
                            
                            if score > existing_score:
                                sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                        else:
                            if parent_data:
                                sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                                pairs_of_rows +=1
                            else:
                                
                                sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)
                
                if num_rows % 100000 == 0:
                    print("Total rows read: {}, paired rows: {}, Time {}".format(num_rows, pairs_of_rows, str(datetime.now())))
                            
                                
                    

                   