# -*- coding: utf-8 -*-
import pandas as pd
import gtfs_kit as gk
import geopandas as gpd
import flexpolyline as fp

class Create_GTFS_sub():
    
    
    def __init__(self, feed):
        
        self.feed = feed
        
        
    def pin(self):
        
        pin = pd.merge_ordered(self.feed.stops[['stop_id','stop_name','stop_desc','stop_lat','stop_lon']],self.feed.stop_times[['stop_id','trip_id']], on='stop_id', how='inner')
        pin = pin.drop_duplicates(subset=['stop_id']).copy()
        pin = pd.merge(pin,self.feed.trips[['trip_id','route_id']], on='trip_id', how='inner').copy()
        pin = pd.merge(pin,self.feed.routes[['route_id','agency_id','route_type']], on='route_id', how='inner').copy()
        
        pin = pin[['stop_id','stop_name','stop_desc','stop_lat','stop_lon','agency_id','route_type']]
        
        return pin
        
    def point(self):
        
        if isinstance(self.feed.trips.direction_id.loc[0], float):
            self.feed.trips['direction_id'] = self.feed.trips['direction_id'].fillna(0)
            self.feed.trips['direction_id'] = self.feed.trips['direction_id'].astype(int)
        
        point = pd.merge(self.feed.stops,self.feed.stop_times[['stop_id','arrival_time','trip_id']], on='stop_id', how='inner')
        point = pd.merge(point,self.feed.trips[['trip_id','route_id','direction_id','trip_headsign']], on='trip_id',how='inner').copy()
        point = pd.merge(point,self.feed.routes[['route_id', 'agency_id','route_short_name','route_color','route_type', 'route_long_name']], on='route_id', how='inner').copy()
        
        point = point[['stop_id','stop_name','stop_desc','stop_lat','stop_lon','arrival_time','trip_id','route_id','direction_id','trip_headsign','agency_id','route_short_name','route_color','route_type', 'route_long_name']]
        
        return point
        
    def trip(self):
        
        rows=[]
        df_trip_id_unique = pd.DataFrame()
        
        if isinstance(self.feed.trips.direction_id.loc[0], float):
            self.feed.trips['direction_id'] = self.feed.trips['direction_id'].fillna(0)
            self.feed.trips['direction_id'] = self.feed.trips['direction_id'].astype(int)
        
        ftrip = pd.merge(self.feed.stop_times[['stop_id', 'trip_id','arrival_time']], self.feed.stops, on='stop_id', how='inner')
        ftrip = pd.merge(ftrip,self.feed.trips[['trip_id','route_id','trip_headsign','direction_id','shape_id']], on='trip_id',how='inner').copy()
        ftrip = pd.merge(ftrip,self.feed.routes[['route_id','route_color','route_short_name','agency_id','route_type', 'route_long_name']], on='route_id', how='inner').copy()
        
        geometry_by_shape = dict(gk.geometrize_shapes_0(self.feed.shapes, use_utm=False))
        gdf = gpd.GeoDataFrame(geometry_by_shape, crs="EPSG:4326")
        ftrip_f = pd.merge(ftrip, gdf[['shape_id','geometry']],on='shape_id',how='inner')
        
        for index, geom in gdf[["geometry"]].itertuples(index=True):
            new_rows = [[index, (lon, lat)] for i, (lon, lat) in enumerate(geom.coords)]
            rows.extend(new_rows)
        
        df = gpd.GeoDataFrame(rows,columns=['id','geometry'])
        df1 = df.drop_duplicates(subset='id').copy()
        df1.drop(columns='geometry', inplace=True)
        df1.reset_index(drop=True)
        df1_id=df1['id'].values.tolist()
        
        for i in df1_id:
            d = df[df['id'] == i]['geometry']
            gdf.loc[i,'hashxy'] = [fp.encode(d)]
            
        gdf_final = gdf.drop(columns='geometry')
        df_trip_id = ftrip_f.drop_duplicates(subset=['route_id', 'direction_id']).copy()
        df_trip_id['id'] = df_trip_id.index
        
        df_trip_id_unique = df_trip_id[['id', 'shape_id']]
        final = pd.merge(df_trip_id_unique,gdf_final,on ='shape_id', how='inner')
        
        ftrip_f1 = ftrip_f.drop(columns='geometry')
        ftrip_f1['geometry'] = ''
        
        for i in range(0,len(final)):
            ftrip_f1.loc[final.loc[i,'id'],'geometry'] = final.loc[i,'hashxy']
        
        ftrip = ftrip_f1[['stop_id','trip_id','arrival_time','stop_name','stop_desc','stop_lat','stop_lon','route_id','agency_id','trip_headsign','direction_id','shape_id','route_color','route_short_name','route_type', 'route_long_name','geometry']]

        return ftrip
        
        
    def timetable(self):
        
        if isinstance(self.feed.trips.direction_id.loc[0], float):
            self.feed.trips['direction_id'] = self.feed.trips['direction_id'].fillna(0)
            self.feed.trips['direction_id'] = self.feed.trips['direction_id'].astype(int)
        
        grade = pd.merge(self.feed.trips[['service_id', 'trip_id','route_id','direction_id','trip_headsign']], self.feed.calendar, on='service_id', how='inner')
        grade = pd.merge(grade, self.feed.stop_times[['trip_id','arrival_time','stop_id']], on='trip_id', how='inner').copy()
        grade = pd.merge(grade, self.feed.stops[['stop_id','stop_name']], on='stop_id', how='inner').copy()
        gradef = pd.merge(grade, self.feed.routes[['route_id', 'route_short_name', 'agency_id','route_type', 'route_long_name']], on='route_id', how='inner')
        gradef = gradef.drop(columns=['trip_id', 'route_id'])
        
        gradef = gradef[['service_id', 'route_short_name', 'direction_id','trip_headsign','monday','tuesday','wednesday','thursday','friday','saturday','sunday','start_date','end_date','arrival_time','stop_id', 'stop_name', 'agency_id','route_type', 'route_long_name']]
        
        return gradef
        
        


