#Autor: Juan Sebastian Burgos.
#Fecha de entrega: Diciembre 19 de 2022
#Descripcion: Script elaborado como la parte 1 de un ejercicio tecnico de Hunty

#Seccion para importar las librerias
from google.cloud import bigquery
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

#Declaracion de constantes
BUCKET_NAME = 'pruebahunty'
SA_KEY = "C:\\Users\\juanj\\OneDrive\\Escritorio\\PruebaTecnicaHunty\\key\\strong-eon-372023-2ea17e4d2cf1.json"
SCOPE = ['https://www.googleapis.com/auth/spreadsheets',
        "https://www.googleapis.com/auth/drive"]


#Autenticacion hacia google sheet mendiante el servicio API de GCP
sheet_credentials = ServiceAccountCredentials.from_json_keyfile_name(SA_KEY, SCOPE) 
sheet_client = gspread.authorize(sheet_credentials)

#Autenticación hacia el bucket del servicio Cloud Storage de GCP
storage_client = bigquery.Client.from_service_account_json(SA_KEY)
bucket = storage_client.get_bucket(BUCKET_NAME)
filename = list(bucket.list_blobs(prefix=''))

#Validacion de la existencia de un sheet en el servicio
#Si la funcion retorna 0 es porque el documento NO existe en el servicio.
#Si el documento retorna 1 es porque existe.
def validaSheet(nombre: str):
    sheet_docs = sheet_client.list_spreadsheet_files()
    if len(sheet_docs) == 0:
        return 0
    elif len(sheet_docs) > 0:
        for element in sheet_docs:
            if element['name'] == str(nombre):
                return 1            
    return 0
            

#Funcion para insertar los datos en el google sheet creado
def insertarSheet(dato: str, name: str):
    
    print(type(dato))
    df = pd.read_json(dato)    
    sheet = sheet_client.open(name).sheet1
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    print(df)
    

#Script que utiliza los metodos y validaciones creados.
for name in filename:
    if validaSheet(name.name.lower()) == 0:
        if '.json' in name.name.lower():
            sheet = sheet_client.create(name.name)
            sheet.share('juan.jsbo@gmail.com', perm_type='user', role='writer')
    blop = bucket.blob(name.name)
    data = blop.download_as_string()
    insertarSheet(data, name.name.lower())


        
       


#sheet_docs = sheet_client.list_spreadsheet_files()
#for element in sheet_docs:
    #sheet_client.del_spreadsheet(element['id'])
    #print(element['name'])
    #print(element)


        
        #sheet_client.create(name.name)
        #print(name.name)
        #sheet.share('juan.jsbo@gmail.com', perm_type='user', role='writer')
        #blop = bucket.blob(name.name)
        #data = blop.download_as_string()
        #for elemento in name.name:
            #print(elemento['name'])
            #sheet = sheet_client.create(name.name)
            #if elemento['name'].lower() != name.name.lower() or elemento['name'].lower() == None:
                #sheet = sheet_client.create(name.name)
                #print(elemento['name'])
                #sheet.share('juan.jsbo@gmail.com', perm_type='user', role='writer')
                #blop = bucket.blob(name.name)
                #print(elemento['name'])        
                #sheet_client.del_spreadsheet(elemento['id'])





            