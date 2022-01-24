# -*- coding: utf-8 -*-


class Validate():
    
    def __init__(self, feed):
        self.feed = feed
        
    
    def validate_fields(self):
        
        stops = ['stop_id','stop_name','stop_desc','stop_lat','stop_lon']
        routes = ['agency_id','route_type','route_id','route_short_name','route_color','route_long_name']
        trips = ['trip_id','direction_id','trip_headsign','shape_id', 'service_id','route_id']
        stop_times = ['arrival_time','stop_id','trip_id']
        calendar = ['monday','tuesday','wednesday','thursday','friday','saturday','sunday','start_date','end_date']
        result_dict={}
        
        result_dict['stop_times'] = [i for i in stop_times if i not in self.feed.stop_times]
        result_dict['stops'] = [i for i in stops if i  not in self.feed.stops]
        result_dict['routes'] = [i for i in routes if i  not in self.feed.routes]
        result_dict['trips'] = [i for i in trips if i  not in self.feed.trips]
        result_dict['calendar'] = [i for i in calendar if i  not in self.feed.calendar]
        
        return result_dict