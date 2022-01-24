
from GTFS_sub.connections import Connection
import random

class Backup_Table():
    
    def __init__(self, db, table_name):
        
        connection = Connection(db)
        self.conn = connection.connect_get()
        self.cur = self.conn.cursor()
        self.table_name = table_name
        
    def create_backup_pin(self):
        
        old_name = 'pin_' + self.table_name
        new_name = old_name + '_backup'
        #sql = 'CREATE TABLE {1} AS TABLE {0};'.format(old_name, new_name)
        sql = 'ALTER TABLE IF EXISTS public.{0} RENAME TO {1}'.format(old_name, new_name)

        self.cur.execute(sql)
        self.conn.commit()
        
    
    def create_backup_point(self):
        
        old_name = 'point_' + self.table_name
        new_name = old_name + '_backup'
        
        #sql = 'CREATE TABLE {1} AS TABLE {0};'.format(old_name, new_name)
        sql = 'ALTER TABLE IF EXISTS public.{0} RENAME TO {1}'.format(old_name, new_name)
        self.cur.execute(sql)
        self.conn.commit()

    
    def create_backup_timetable(self):
        
        old_name = 'timetable_' + self.table_name
        new_name = old_name + '_backup'
        
        #sql = 'CREATE TABLE {1} AS TABLE {0};'.format(old_name, new_name)
        sql = 'ALTER TABLE IF EXISTS public.{0} RENAME TO {1}'.format(old_name, new_name)
        self.cur.execute(sql)
        self.conn.commit()

    def create_backup_trip(self):
        
        old_name = 'trip_' + self.table_name
        new_name = old_name + '_backup'
        
        #sql = 'CREATE TABLE {1} AS TABLE {0};'.format(old_name, new_name)
        sql = 'ALTER TABLE IF EXISTS public.{0} RENAME TO {1}'.format(old_name, new_name)
        self.cur.execute(sql)
        self.conn.commit()
        
    def activate_pin(self):
        
        old_name = 'pin_' + self.table_name + "_"
        new_name = 'pin_' + self.table_name
        
        sql = 'ALTER TABLE IF EXISTS public.{0} RENAME TO {1}'.format(old_name, new_name)

        self.cur.execute(sql)
        self.conn.commit()
        
    
    def activate_point(self):
        
        old_name = 'point_' + self.table_name + "_"
        new_name = 'point_' + self.table_name
        
        sql = 'ALTER TABLE IF EXISTS public.{0} RENAME TO {1}'.format(old_name, new_name)
        self.cur.execute(sql)
        self.conn.commit()

    
    def activate_trip(self):
        
        old_name = 'trip_' + self.table_name + "_"
        new_name = 'trip_' + self.table_name
        
        sql = 'ALTER TABLE IF EXISTS public.{0} RENAME TO {1}'.format(old_name, new_name)
        self.cur.execute(sql)
        self.conn.commit()

    def activate_timetable(self):
        
        old_name = 'timetable_' + self.table_name + "_"
        new_name = 'timetable_' + self.table_name
        
        sql = 'ALTER TABLE IF EXISTS public.{0} RENAME TO {1}'.format(old_name, new_name)
        self.cur.execute(sql)
        self.conn.commit()
    
    def drop_backup_table_pin(self):
        
        delete_name = 'pin_' + self.table_name + "_backup"
        
        sql = 'DROP TABLE IF EXISTS public.{0}'.format(delete_name)

        self.cur.execute(sql)
        self.conn.commit()   

    def drop_backup_table_point(self):
        
        delete_name = 'point_' + self.table_name + "_backup"
        
        sql = 'DROP TABLE IF EXISTS public.{0}'.format(delete_name)

        self.cur.execute(sql)
        self.conn.commit() 

    def drop_backup_table_trip(self):
        
        delete_name = 'trip_' + self.table_name + "_backup"
        
        sql = 'DROP TABLE IF EXISTS public.{0}'.format(delete_name)

        self.cur.execute(sql)
        self.conn.commit()

    def drop_backup_table_timetable(self):
        
        delete_name = 'timetable_' + self.table_name + "_backup"
        
        sql = 'DROP TABLE IF EXISTS public.{0}'.format(delete_name)

        self.cur.execute(sql)
        self.conn.commit()

    def truncate_table_pin(self):

        name = 'pin_' + self.table_name

        sql = 'TRUNCATE {0}'.format(name)

        self.cur.execute(sql)
        self.conn.commit()

    def truncate_table_point(self):

        name = 'point_' + self.table_name

        sql = 'TRUNCATE {0}'.format(name)

        self.cur.execute(sql)
        self.conn.commit()    
    
    def truncate_table_trip(self):

        name = 'trip_' + self.table_name

        sql = 'TRUNCATE {0}'.format(name)

        self.cur.execute(sql)
        self.conn.commit()    
    
    def truncate_table_timetable(self):

        name = 'timetable_' + self.table_name

        sql = 'TRUNCATE {0}'.format(name)

        self.cur.execute(sql)
        self.conn.commit() 

    def create_temptable_pin(self):
        
        old_name = 'pin_' + self.table_name
        new_name = old_name + '_'
        #sql = 'CREATE TABLE {1} AS TABLE {0};'.format(old_name, new_name)
        sql = 'ALTER TABLE IF EXISTS public.{0} RENAME TO {1}'.format(old_name, new_name)

        self.cur.execute(sql)
        self.conn.commit()

    def create_temptable_point(self):
        
        old_name = 'point_' + self.table_name
        new_name = old_name + '_'
        #sql = 'CREATE TABLE {1} AS TABLE {0};'.format(old_name, new_name)
        sql = 'ALTER TABLE IF EXISTS public.{0} RENAME TO {1}'.format(old_name, new_name)

        self.cur.execute(sql)
        self.conn.commit()
       
    def create_temptable_trip(self):
        
        old_name = 'trip_' + self.table_name
        new_name = old_name + '_'
        #sql = 'CREATE TABLE {1} AS TABLE {0};'.format(old_name, new_name)
        sql = 'ALTER TABLE IF EXISTS public.{0} RENAME TO {1}'.format(old_name, new_name)

        self.cur.execute(sql)
        self.conn.commit()

    def create_temptable_timetable(self):
        
        old_name = 'timetable_' + self.table_name
        new_name = old_name + '_'
        #sql = 'CREATE TABLE {1} AS TABLE {0};'.format(old_name, new_name)
        sql = 'ALTER TABLE IF EXISTS public.{0} RENAME TO {1}'.format(old_name, new_name)

        self.cur.execute(sql)
        self.conn.commit()

    def truncate_temptable_pin(self):

        name = 'pin_' + self.table_name + '_'

        sql = 'TRUNCATE {0}'.format(name)

        self.cur.execute(sql)
        self.conn.commit()

    def truncate_temptable_point(self):

        name = 'point_' + self.table_name + '_'

        sql = 'TRUNCATE {0}'.format(name)

        self.cur.execute(sql)
        self.conn.commit()

    def truncate_temptable_trip(self):

        name = 'trip_' + self.table_name + '_'

        sql = 'TRUNCATE {0}'.format(name)

        self.cur.execute(sql)
        self.conn.commit()

    def truncate_temptable_timetable(self):

        name = 'timetable_' + self.table_name + '_'

        sql = 'TRUNCATE {0}'.format(name)

        self.cur.execute(sql)
        self.conn.commit()

    def create_pin_table(self): 
        index = '{0}{1}'.format('pin_index_',str(random.random()))
        name = 'pin_' + self.table_name
        sql_table =("""
                    CREATE TABLE {0} (
                                        id SERIAL PRIMARY KEY,
                                        stop_id character varying(255),
                                        stop_name character varying(255),
                                        stop_desc text,
                                        stop_lat double precision,
                                        stop_lon double precision,
                                        agency_id character varying(255),
                                        route_type integer)  
                    """.format(name))
    
        sql_index =("""
                    CREATE INDEX "{0}" ON {1} USING btree (stop_lat, stop_lon, route_type)  
                    """.format(index,name))
        
        #sql_pk = ("""
        #          ALTER TABLE ONLY {0}
        #          ADD CONSTRAINT {0}_pkey1 PRIMARY KEY (id)
        #          """.format(name))
        
        self.cur.execute(sql_table)
        #self.cur.execute(sql_pk)
        self.cur.execute(sql_index)
        self.conn.commit()

    def create_point_table(self): 
            index = '{0}{1}'.format('point_index_',str(random.random()))
            name = 'point_' + self.table_name
            sql_table =("""
                        CREATE TABLE {0} (
                                            id SERIAL PRIMARY KEY,
                                            stop_id character varying(255),
                                            stop_name character varying(255),
                                            stop_desc text,
                                            stop_lat double precision,
                                            stop_lon double precision,
                                            arrival_time character varying(50),
                                            trip_id character varying(255),
                                            route_id character varying(255),
                                            direction_id integer,
                                            trip_headsign character varying(255),
                                            agency_id character varying(255),
                                            route_short_name character varying(255),
                                            route_color character varying(20),
                                            route_type integer,
                                            route_long_name text)  
                        """.format(name))
        
            sql_index =("""
                            CREATE INDEX "{0}" ON {1} USING btree (stop_id)  
                        """.format(index,name))
            
            #sql_pk = ("""
            #            ALTER TABLE ONLY {0} ADD CONSTRAINT {0}_pkey PRIMARY KEY (id)
            #        """.format(name))
            
            self.cur.execute(sql_table)
            #self.cur.execute(sql_pk)
            self.cur.execute(sql_index)
            self.conn.commit()

    def create_trip_table(self): 
            index1 = '{0}{1}'.format('trip_index1_',str(random.random()))
            index2 = '{0}{1}'.format('trip_index2_',str(random.random()))
            name = 'trip_' + self.table_name
            sql_table =("""
                        CREATE TABLE {0} (
                                            id SERIAL PRIMARY KEY,
                                            stop_id character varying(255),
                                            trip_id character varying(255),
                                            arrival_time character varying(50),
                                            stop_name character varying(255),
                                            stop_desc text,
                                            stop_lat double precision,
                                            stop_lon double precision,
                                            route_id character varying(255),
                                            agency_id character varying(255),
                                            trip_headsign character varying(255),
                                            direction_id integer,
                                            shape_id character varying(255),
                                            route_color character varying(20),
                                            route_short_name character varying(255),
                                            route_type integer,
                                            route_long_name text,
                                            geometry text)  
                        """.format(name))

            #sql_pk = ("""
            #            ALTER TABLE ONLY {0} ADD CONSTRAINT {0}_pkey PRIMARY KEY (id)
            #        """.format(name))
        
            sql_index_1 =("""
                        CREATE INDEX "{0}" ON {1} USING btree (route_short_name, direction_id)  
                        """.format(index1,name))
            sql_index_2 =("""
                        CREATE INDEX "{0}" ON {1} USING btree (stop_id, route_short_name, direction_id)  
                        """.format(index2,name))
            
            self.cur.execute(sql_table)
            #self.cur.execute(sql_pk)
            self.cur.execute(sql_index_1)
            self.cur.execute(sql_index_2)
            self.conn.commit()

    def create_timetable_table(self): 
            index = '{0}{1}'.format('timetable_index_',str(random.random()))
            name = 'timetable_' + self.table_name
            sql_table =("""
                        CREATE TABLE {0} (
                                            id SERIAL PRIMARY KEY,
                                            service_id character varying(255),
                                            route_short_name character varying(255),
                                            direction_id integer,
                                            trip_headsign character varying(255),
                                            monday integer,
                                            tuesday integer,
                                            wednesday integer,
                                            thursday integer,
                                            friday integer,
                                            saturday integer,
                                            sunday integer,
                                            start_date character varying(20),
                                            end_date character varying(20),
                                            arrival_time character varying(20),
                                            stop_id character varying(255),
                                            stop_name character varying(255),
                                            agency_id character varying(255),
                                            route_type integer,
                                            route_long_name text)  
                        """.format(name))
        
            sql_index =("""
                        CREATE INDEX "{0}" ON {1} USING btree (stop_id, route_short_name, direction_id)  
                        """.format(index,name))

            #sel_pk = ("""
            #            ALTER TABLE ONLY {0} ADD CONSTRAINT {0}_pkey PRIMARY KEY (id)
            #""".format(name))
            
            self.cur.execute(sql_table)
            #self.cur.execute(sel_pk)
            self.cur.execute(sql_index)
            self.conn.commit()

    def close(self):
        
        self.cur.close()
        self.conn.close()