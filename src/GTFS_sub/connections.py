# -*- coding: utf-8 -*-
import psycopg2 as pg





class Connection():

    def __init__(self, env):
        self.env = env    
        
    def connect_get(self):
        if self.env == 'dev':
            connection = pg.connect("dbname='gtfs_quicko' user='mobupadm@dev-ucd-db' host='dev-ucd-db.postgres.database.azure.com' password='T0r0nt0S@' port='5432' sslmode='require'")
        if self.env == 'prod':
            connection = pg.connect("dbname='gtfs_prod' user='gtfs_prod@prd-quicko-db-1' host='prd-quicko-db-1.postgres.database.azure.com' password='r;3PQ[w=fvE!87Q$' port='5432' sslmode='require'")
        
        return connection
