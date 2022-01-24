import os
import sys
from sys import path
dname = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0,dname)
from GTFS_sub.line_sub import Line
from GTFS_sub.get_links import Links
import gtfs_kit as gk
import datetime
from GTFS_sub.validate import Validate
from GTFS_sub.create_gtfs_sub import Create_GTFS_sub
from GTFS_sub.backup_table import Backup_Table
from GTFS_sub.update_data import updateData
from GTFS_sub.merge_line_sub import MergeLines

def main():
    print(datetime.datetime.now().strftime("%H:%M:%S"))
    # ENTRADA DE PARAMETROS
    # =============================================================================
    dname = os.path.dirname(os.path.abspath(__file__))
    dir_name = os.path.join(dname, 'busca')
    ibge_code = os.path.join(dname, 'IBGE', 'cod_IBGE.xls')
    link_GTFS = os.path.join(dname, 'Carga_GTFS_table', 'SaoPaulo.xls')
    db = 'dev'
    # =============================================================================
    #LISTAR LINKS PARA DOWNLOAD
    # =============================================================================
    try:
        links = Links(link_GTFS)
        list_links = links.list_link()
    except:
        print("Não foi possível acessar a tabela de links")
    # =============================================================================
    for i in range(0, len(list_links[0])):
        
        #CRIAR VARIÁVEL DE NOME
    # =============================================================================    
        url = list_links[0][i]
        uf = list_links[1][i].strip().lower()
        city_name = list_links[2][i]
        source = list_links[3][i]
        rm = list_links[4][i].lower()
        
        if rm != 'capital':
            table_name = rm + "_" + uf
        else:
            table_name = uf
        
        name = city_name + "_" + uf + '.zip'
        
        print("RM: "+table_name)
        print(city_name)
    # =============================================================================
        # INSTANCIAR OBJETOS #    
    # =============================================================================          
        # FLUXO #
    # =============================================================================
        #AJUSTE DE HORÁRIO E VALIDAÇÃO DE COLUNAS
    # =============================================================================
        y=[]
        feed = gk.read_feed(url, dist_units='km')
        validate = Validate(feed)
        val_result = validate.validate_fields()
        print(val_result)
        def adjust_time(x):
            z = x['arrival_time']
            if not isinstance(z, float):
                if z >= '24:00:00':
                    y = z.split(":")
                    t = int(y[0]) - 24
                    if t > 0:
                        z = '0'+str(t)+':'+y[1]+':'+y[2]
                    else:
                        z = str(t)+'0:'+y[1]+':'+y[2]
            return z
        try:
            feed.stop_times['arrival_time'] = feed.stop_times.apply(adjust_time, axis=1)
        except:
            print(list_links[2][i])
            print('Não foi possível ajustar todas as linhas:' + name)
            continue
            
    # ============================================================================= 
        # CRIA ARQUIVOS DE BUSCA DE LINHAS
        
        try:
            line = Line(feed, uf, dir_name, city_name, ibge_code)
            line.create()
        except:
            print(list_links[2][i])
            print('Não foi possível criar os arquivos de busca de linhas')  
    # =============================================================================  
        # CRIA OS DATAFRAMES DE SUB
        try:
            create_sub = Create_GTFS_sub(feed)
        except:
            print(city_name)
            print('Não foi possível criar as tabelas sub')

    # =============================================================================
    # =============================================================================
        if i == 0:
    # CRIA AS TABELAS DE BACKUP
    # =============================================================================
            if db != 'prod':
                backup = Backup_Table(db,table_name)
                
                try:
                    backup.drop_backup_table_pin()
                    backup.drop_backup_table_point()
                    backup.drop_backup_table_trip()
                    backup.drop_backup_table_timetable()
                    
                    print("Drop tables feito!")
                    
                    backup.create_backup_pin()
                    backup.create_backup_point()
                    backup.create_backup_trip()
                    backup.create_backup_timetable()
                    
                    print("Backup tables feito!")
                    
                    backup.create_pin_table()
                    backup.create_point_table()
                    backup.create_trip_table()
                    backup.create_timetable_table()
                    
                    print("Create tables feito!")
                    
                    backup.close()
                except:
                    backup.close()
                    print('Erro ao construir as tabelas de backup')
        # =============================================================================
            #MONTA OS NOMES DAS TABELAS
        # =============================================================================    
                name_pin = 'pin_'+ table_name
                name_point = 'point_'+ table_name
                name_timetable = 'timetable_'+ table_name
                name_trip = 'trip_'+ table_name
        # =============================================================================   
            # FAZ A CARGA DOS DADOS NAS TABELAS  
        # =============================================================================
                try:
                    update = updateData(db, create_sub.pin(), name_pin).upload()
                    print("Update pin feito!")
                    update = updateData(db, create_sub.point(), name_point).upload()
                    print("Update point feito!")
                    update = updateData(db, create_sub.timetable(), name_timetable).upload()
                    print("Update timetable feito!")
                    update = updateData(db, create_sub.trip(), name_trip).upload()
                    print("Update trip feito!")
                except:
                    print('Erro ao realizar a carga dos arquivos')       
        # =============================================================================            
    # =============================================================================
    # JUNTAR TABELAS DE BUSCA DE LINHAS POR ESTADO    
    # ============================================================================= 
    #INSTANCIAR MERGE LINES       
    merge = MergeLines(dir_name)
    #try:
    merge.merge()
    #except:
    #   print('Erro a juntar tabelas de busca de linhas')

    # =============================================================================
    # ATUALIZA ARQUIVOS DE BUSCA DE LINHAS NA API
    #try:
    update_line =  line.upload()
    print(update_line)
    #except:
    #    print('Não foi possível fazer o upload do arquivo de busca de linha')            
    # ============================================================================= 
    print("Fluxo concluido")
    print(datetime.datetime.now().strftime("%H:%M:%S"))
main()