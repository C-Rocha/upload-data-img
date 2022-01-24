# -*- coding: utf-8 -*-

import io
from GTFS_sub.connections import Connection

class updateData():
    
    def __init__(self, db, table, table_name):
        
        connection = Connection(db)
        self.conn = connection.connect_get()
        self.cur = self.conn.cursor()
        self.table = table
        self.table_name = table_name
    
    def upload(self):
        
        nlin = len(self.table)
        lout = nlin % 50000
        n_in = nlin - lout
        p=0
        for i in range(0,nlin, 50000):
            if i != 0:
                table_temp = self.table[p:i]
                output = io.StringIO()
                table_temp.to_csv(output, sep='*', header=False, index=False)
                output.seek(0)
                self.cur.copy_from(output, self.table_name, sep='*', null="", columns=table_temp.columns)
                self.conn.commit()
                p = i
                
        table_temp = self.table[n_in:nlin]       
        output = io.StringIO()
        table_temp.to_csv(output, sep='*', header=False, index=False)
        output.seek(0)
        self.cur.copy_from(output, self.table_name, sep='*', null="", columns=table_temp.columns)
        self.cur.close()
        self.conn.commit()
        self.conn.close()


