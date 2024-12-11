import camelot
import os
import csv
import pandas as pd

#Variables
pdf_path = "/home/debian/pdf-procesamiento/sla.pdf" #Modificar Nombre del PDF
output_csv = "/home/debian/pdf-procesamiento/all_tables/"  #ruta para alojar cada tabla dentro del pdf
down_path = '/home/debian/pdf-procesamiento/final_table.csv' #ruta para alojar el csv final del pdf

#Funcion 1 - Extrae cada tabla en el pdf usando camelot y son exportadas en csv para post procesammiento
def extract_tables_to_csv(pdf_path, output_csv):
    try:
        tables = camelot.read_pdf(pdf_path, pages="all", flavor="lattice",line_scale=35)
        if not tables:
            print("No se detectaron tablas en el PDF.")
            return        
        name_tables = []
        for i, table in enumerate(tables):
            table.to_csv(f"{output_csv}tabla_{i+1}.csv")
            name_tables.append(f"tabla_{i+1}.csv")
        print(f"Tablas CSVs exportadas exitosamente.")
        return name_tables
    except Exception as e:
        print(f"Error al procesar el PDF: {e}")

#Funcion 2 - Lectura de los csv de segmentos de tablas para union de todo los csvs en un array
def concat_tables(name_tables, files_path, down_path):
    table= []
    for t in name_tables:
        if os.path.exists(f'{files_path}/{t}'):
            try:
                with open(f'{files_path}/{t}', mode='r') as file:
                    csv_reader = csv.reader(file)
                    for row in csv_reader:
                        table.append(row)
            except Exception as error:
                print(error, t)
        else:
            print(f"El archivo {files_path} no existe.")
#uniendo filas y eliminando filas del array
    a = 1
    for i, e in enumerate(table):
        if not e[0]:
            table[i-a][1]+= e[1]
            a += 1
        else:
            a = 1
    table = [e for e in table if e[0]] #Tabla Filtrada con todas las tablas ordenadas

#Obtener indices de cada tablas dentro de una lista  ordenados
    index_tables = []
    for i, e in enumerate(table):
        if "Proceso:" in e[0]:
            index_tables.append(i)

#Separar array en tablas Ordenadas utilizando como identificado la primera fila de cada tabla (Proceso)
    dfs_list = []
    dfs_names = []
    n = 0
    for i in range(len(index_tables)):
        start_index = index_tables[i]
        end_index = index_tables[i + 1] if i + 1 < len(index_tables) else len(table)
        table1 = table[start_index:end_index]
#aca podemos agregar algun procesaiento ya que cuando no reconoce las columna de una tabla el contenido de esa tabla que si se reconoce se agrega a la ultima tabla reconocida, y lo agrega como columnas extras que podemos borrar aca o mas adelante

        #Cambiar posicion de la primera fila futuras (columnas)
        table1[0][0], table1[0][1] = table1[0][1], table1[0][0]
        table1[0][0] = 'Nombre Proceso'

        #Cambiar Eje de cada tabla
        columnas = [e[0] for e in table1]
        valores = [e[1] for e in table1]
        df = pd.DataFrame([valores], columns=columnas)
        dfs_list.append(df)
        dfs_names.append(f'/home/debian/pdf-procesamiento/csvs_tables/{n}.csv')
        print(df.head())
        df.to_csv(f'/home/debian/pdf-procesamiento/csvs_tables/{n}.csv') # si no se exportan los csvs no permite concatenar, tube problemas trabajandolos en memoria
        n += 1
    dfs_names = [pd.read_csv(e) for e in dfs_names]
    df_concatenado = pd.concat(dfs_names, axis=0, ignore_index=True)
    df_concatenado.to_csv(down_path)

name_tables = extract_tables_to_csv(pdf_path, output_csv)
concat_tables(name_tables, output_csv, down_path)


