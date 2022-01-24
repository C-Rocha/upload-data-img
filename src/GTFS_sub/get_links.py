import pandas as pd

class Links():
    
    def __init__(self, link):
        table = pd.read_excel(open(link, 'rb'), sheet_name='Planilha1')
        self.url = table['GTFS Estático'].to_list()
        self.city_info = table.Cidade.to_list()
        self.source = table.Fonte
        self.rm = table.RM

    
    def list_link(self):
        uf=[]
        city=[]
        for i in self.city_info:
            uf.append(i.split(",")[1])
            city.append(i.split(",")[0])
        return([self.url,uf,city, self.source, self.rm])

