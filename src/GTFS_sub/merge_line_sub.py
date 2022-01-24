# -*- coding: utf-8 -*-

import os
import pandas as pd


class MergeLines():
    
    def __init__(self, path):
        
        self.path = path


    def merge(self):
        
        lista =[]

        
        for diretorio, subpastas, arquivos in os.walk(self.path):
            for arquivo in arquivos:
                try:
                    name = arquivo.split("_")[2]
                    lista.append(arquivo)
                except:
                    continue
                
        if len(lista) > 1:
            data = pd.read_csv(os.path.join(diretorio, lista[0]))
            for s in range(1,len(lista)):
                data = pd.concat([data, pd.read_csv(os.path.join(diretorio, lista[s]))])
                data.to_csv(os.path.join(diretorio, 'busca_' + name), index=False)
        else:
            data = pd.read_csv(os.path.join(diretorio, lista[0]))
            data.to_csv(os.path.join(diretorio, 'busca_' + name), index=False)