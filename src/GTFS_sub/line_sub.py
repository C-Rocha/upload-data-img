import pandas as pd
import io
import os
import requests

class Line():
    
    def __init__(self, feed, uf, path, city_name, ibge_code):
        
        self.feed = feed
        self.uf = uf
        self.path = path
        self.city_name = city_name
        self.ibge_code = ibge_code

    def create(self):
        
        if isinstance(self.feed.trips.direction_id.loc[0], float):
            self.feed.trips['direction_id'] = self.feed.trips['direction_id'].fillna(0)
            self.feed.trips['direction_id'] = self.feed.trips['direction_id'].astype(int)
        
        line = pd.merge(self.feed.routes[['route_id', 'route_short_name', 'route_color','route_type']], self.feed.trips[['route_id','trip_id','trip_headsign','direction_id']], on='route_id', how='inner')
        line['trip_id'] = line.apply(lambda x : '{0}-{1}'.format(x['route_short_name'],str(x['direction_id'])), axis=1)
        line_f = line.drop_duplicates(subset=['trip_id']).copy()
        
        for index, row in line_f.iterrows():
            line_f.loc[index,row.index[2]] = '#{0}'.format(line_f.loc[index,row.index[2]]) 
        
        code = pd.read_excel(self.ibge_code)
        code_city = dict(code.values)
        
        line_f.insert(len(line_f.columns), 'codeIBGE', code_city[self.city_name])
        path_name = os.path.join(self.path, 'busca_' + self.city_name + "_" + self.uf + '.csv')
        line_f.to_csv(path_name ,index=False)
    
    def upload(self):
        
        path_name = os.path.join(self.path, 'busca_' + self.uf + '.csv')
        
        headers = {
                    'Accept': 'application/json',
                    'SUBSCRIPTION-KEY': '96e824a566484860887af7a0cc21701f',
                    'QUICKO-MOBILEID':'c5c942da86c14aba'
                    }
        
        files = {'file': open(path_name, 'rb')}
        response = requests.post("https://dev-mobup-api.azure-api.net/search-api/lines",  headers=headers, files=files)
        
        if response.ok == True:
            return response.content
        else:
            return False 
