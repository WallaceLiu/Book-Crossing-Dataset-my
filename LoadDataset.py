# -*- coding: utf-8 -*-

import mysql.connector
import codecs
import string
import os
import sys
import ConfigParser
from collections import OrderedDict
import re

class MysqlPythonFacotry(object):
    """
        Python Class for connecting  with MySQL server.
    """

    __instance = None
    __host = None
    __user = None
    __password = None
    __database = None
    __session = None
    __connection = None

    def __init__(self, host='localhost', user='root', password='', database=''):
        self.__host = host
        self.__user = user
        self.__password = password
        self.__database = database
    ## End def __init__

    def open(self):
        try:
            cnx = mysql.connector.connect(host=self.__host,\
                user= self.__user,\
                password= self.__password,\
                database= self.__database)
            self.__connection = cnx
            self.__session = cnx.cursor()
        except mysql.connector.Error as e:
            print('connect fails!{}'.format(e))
    ## End def open

    def close(self):
        self.__session.close()
        self.__connection.close()
    ## End def close

    def select(self, table, where=None, *args, **kwargs):
        result = None
        query = 'SELECT '
        keys = args
        values = tuple(kwargs.values())
        l = len(keys) - 1

        for i, key in enumerate(keys):
            query += "`" + key + "`"
            if i < l:
                query += ","
        ## End for keys

        query += 'FROM %s' % table

        if where:
            query += " WHERE %s" % where
        ## End if where

        self.__session.execute(query, values)
        number_rows = self.__session.rowcount
        number_columns = len(self.__session.description)
        result = self.__session.fetchall()

        return result
    ## End def select

    def update(self, table, where=None, *args, **kwargs):
        try:
            query = "UPDATE %s SET " % table
            keys = kwargs.keys()
            values = tuple(kwargs.values()) + tuple(args)
            l = len(keys) - 1
            for i, key in enumerate(keys):
                query += "`" + key + "` = %s"
                if i < l:
                    query += ","
                ## End if i less than 1
            ## End for keys
            query += " WHERE %s" % where

            self.__session.execute(query, values)
            self.__connection.commit()

            # Obtain rows affected
            update_rows = self.__session.rowcount

        except mysql.connector.Error as e:
            print(e.value)

        return update_rows
    ## End function update

    def insert(self, table, *args, **kwargs):
        values = None
        query = "INSERT INTO %s " % table
        if kwargs:
            keys = kwargs.keys()
            values = tuple(kwargs.values())
            query += "(" + ",".join(["`%s`"] * len(keys)) % tuple(keys) + ") VALUES (" + ",".join(["%s"] * len(values)) + ")"
        elif args:
            values = args
            query += " VALUES(" + ",".join(["%s"] * len(values)) + ")"

        self.__session.execute(query, values)
        self.__connection.commit()
        cnt = self.__session.rowcount
        return cnt
    ## End def insert

    def delete(self, table, where=None, *args):
        query = "DELETE FROM %s" % table
        if where:
            query += ' WHERE %s' % where

        values = tuple(args)

        self.__session.execute(query, values)
        self.__connection.commit()
        delete_rows = self.__session.rowcount
        return delete_rows
    ## End def delete

    def select_advanced(self, sql, *args):
        od = OrderedDict(args)
        query = sql
        values = tuple(od.values())
        self.__session.execute(query, values)
        number_rows = self.__session.rowcount
        number_columns = len(self.__session.description)
        result = self.__session.fetchall()
        return result
    ## End def select_advanced
## End class


class ErrorMyProgram(Exception):
    """
        My Exception Error Class
    """
    def __init__(self, value):
        self.value = value
    ##End def __init__
        
    def __str__(self):
        return repr(self.value)
    ##End def __str__
 ## End class ErrorMyProgram
    
    
class LoadAppConf(object):
    """
        Load app.conf Config File Class
    """
    __configFileName = "app.conf"

    def __init__(self):
        config = ConfigParser.ConfigParser()
        config.read(self.__configFileName)

        self.biz_db_host = config.get("biz_db","host") 
        self.biz_db_user = config.get("biz_db","user") 
        self.biz_db_password = config.get("biz_db","password")
        self.biz_db_database = config.get("biz_db","database")
    ## End def __init__
 ## End class LoadAppConf    
        
class Biz_Base(object):
    """
        biz base class
    """
    def __init__(self, db):
        self.db = db
    ## End def __init__
 ## End class Biz_Base
        

class Biz_bx_book_ratings(Biz_Base):
    """
        bx_book_ratings table
    """

    __tableName = "bx_book_ratings"

    def __init__(self, db):
        Biz_Base.__init__(self, db)
    ## End def __init__
        
    def insert(self, userid, isbn, bookrating):
        cnt = self.db.insert(self.__tableName,\
            userid = userid, \
            isbn = isbn,\
            bookrating = bookrating)
        return cnt > 0
    ## End def insert
 ## End class Biz_bx_book_ratings    


class Biz_bx_books(Biz_Base):
    """
        bx_books table
    """

    __tableName = "bx_books"

    def __init__(self, db):
         Biz_Base.__init__(self, db)
    ## End def __init__
         
    def insert(self, isbn, booktitle, bookauthor, yearofpublication, publisher, imageurls, imageurlm, imageurll):
        cnt = self.db.insert(self.__tableName,\
            isbn = isbn, \
            booktitle = booktitle, \
            bookauthor = bookauthor,\
            yearofpublication = yearofpublication, \
            publisher = publisher, \
            imageurls = imageurls, \
            imageurlm = imageurlm, \
            imageurll = imageurll)
        return cnt > 0
    ## End def insert
## End class Biz_bx_books 

class Biz_bx_users(Biz_Base):
    """
        bx_users table
    """

    __tableName = "bx_users"

    def __init__(self, db):
         Biz_Base.__init__(self, db)
    ## End def __init__
         
    def insert(self, userid, location, age):
        cnt = self.db.insert(self.__tableName,\
            userid = userid, \
            location = location,\
            age = age)
        return cnt > 0
    ## End def insert
## End class Biz_bx_users 

def regx(l):
    """
        split line by regex
    """
    p = re.compile(r'"[^"]*"')
    return p.findall(l)
## End def regx 

class LoadDataset(object):
    """
        bx_books table
    """
    
    __loadConf = None
    
    __users = None
    __books = None
    __book_ratings = None
    
    __bizDb = None    

    def __init__(self):
        self.__loadConf = LoadAppConf()
        
        self.__bizDb = MysqlPythonFacotry(self.__loadConf.biz_db_host,\
                 self.__loadConf.biz_db_user, \
                 self.__loadConf.biz_db_password,\
                 self.__loadConf.biz_db_database)

        self.__users = Biz_bx_users(self.__bizDb)
        self.__books = Biz_bx_books(self.__bizDb)
        self.__book_ratings = Biz_bx_book_ratings(self.__bizDb)
    
        self.__bizDb.open()
    ## End def __init__
        
    def toDB(self, path=''):
        """
            loads the BX book dataset. Path is where the BX files are
            located
        """
        
        self.data = {}
        i = 0
        j = 0
        try:
            #
            # First load book ratings into self.data
            #
            f = codecs.open(path + "BX-Book-Ratings.csv", 'r', 'utf8')
            for line in f:
                i += 1
                j += 1
                
                print(j)
                print(line)
                
                #separate line into fields
                fields = line.split(';')
                user = fields[0].strip('"')
                book = fields[1].strip('"')
                rating = int(fields[2].strip().strip('"'))

                self.__book_ratings.insert(user, book, rating)

            f.close()
            #
            # Now load books into self.productid2name
            # Books contains isbn, title, and author among other fields
            #
            j = 0
            f = codecs.open(path + "BX-Books.csv", 'r', 'utf8')
            for line in f:
                i += 1
                j += 1

                print(j)
                print(line)
                
                #separate line into fields
                fields = regx(line)
                isbn = fields[0].strip('"')
                title = fields[1].strip('"')
                author = fields[2].strip().strip('"')
                yearOfPublication = fields[3].strip().strip('"')
                publisher = fields[4].strip().strip('"')
                imageUrlS = fields[5].strip().strip('"')
                imageUrlM = fields[6].strip().strip('"')
                imageUrlL = fields[7].strip().strip('"')

                self.__books.insert(isbn, title, author, yearOfPublication, publisher, imageUrlS, imageUrlM, imageUrlL)
            f.close()
            #
            #  Now load user info into both self.userid2name and
            #  self.username2id
            #
            j = 0
            f = codecs.open(path + "BX-Users.csv", 'r', 'utf8')
            for line in f:
                i += 1
                j += 1
                
                print(j)
                print(line)
                
                #separate line into fields                
                fields = regx(line)
                userid = fields[0].strip('"')
                location = fields[1].strip('"')
                if len(fields) > 2:
                    age = fields[2].strip().strip('"')
                else:
                    age = None
                if age != None:
                    value = location + '  (age: ' + age + ')'
                else:
                    value = location

                if age == None:
                    age =0
   
                self.__users.insert(userid, location, age)
                                    
            f.close()
        except  ErrorMyProgram as e:
            print(e.value)
        finally:
            self.__bizDb.close()

        print(i)
    ## End def toDB
## End class LoadData     


