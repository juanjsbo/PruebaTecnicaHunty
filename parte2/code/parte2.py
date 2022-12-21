from google.cloud import bigquery
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from pandas.io import gbq
import json

#Autenticacion a los servicios de Drive
SA_KEY = "C:\\Users\\juanj\\OneDrive\\Escritorio\\PruebaTecnicaHunty\\key\\strong-eon-372023-2ea17e4d2cf1.json"
SCOPE = ['https://www.googleapis.com/auth/spreadsheets',
        "https://www.googleapis.com/auth/drive"]


sheet_credentials = ServiceAccountCredentials.from_json_keyfile_name(SA_KEY, SCOPE)
sheet_client = gspread.authorize(sheet_credentials)

#Lectura de Google Sheet compartido para el ejercicio
sheet = sheet_client.open_by_url('https://docs.google.com/spreadsheets/d/1yGPAiAhRz36LrVHXg3MHB7UkIjPtDDiZFJUgvP5_tY0/edit#gid=471493750')
ws_main_user_info = sheet.worksheet('main_user_info')
ws_user_extra_info = sheet.worksheet('user_extra_info')

#Segmento donde se inserta la informacion en un dataset
df_main_user_info = pd.DataFrame(ws_main_user_info.get_all_records())
df_user_extra_info = pd.DataFrame(ws_user_extra_info.get_all_records())


####Transformaciones#####
list_recurrencia = []
list_years = []
list_months = []

for element in df_user_extra_info['location_change_city_ids'].values:
    list_recurrencia.append(element.count('item'))
    
for element in df_user_extra_info['years_experience'].values:
    jsonExperience = json.loads(element.replace('\'', ('\"')))
    
    if str(jsonExperience["months"]) == '12':
        list_months.append('0')
        list_years.append(str(int(str(jsonExperience["years"]).strip()) + 1))
        
    else:
        list_months.append(jsonExperience["months"])
        list_years.append(jsonExperience["years"])

df_user_extra_info.insert(4, "recurrencia_item", list_recurrencia)
df_user_extra_info.insert(8, "years", list_years)
df_user_extra_info.insert(9, "months", list_months)

df_user_extra_info = df_user_extra_info.astype({"user_id": str, 
    "vacancy_area_id": str,
    "location_change_city_ids": str,
    "available_time_week_id": str,
    "vacancy_area_custom": str,
    "change_city": str,
    "years_experience": str,
    "employment_status": str,
    "recurrencia_item": int,
    "months": int,
    "years": int})

df_main_user_info = df_main_user_info.astype({"user_id": str, 
    "first_name": str,
    "last_name": str,
    "Phone": str,
    "load_date": str})

##Carga de la informacion transformada a la capa Raw de Bigquery, se debe autorizar la libreria para hacer la carga.
df_main_user_info.to_gbq(
    destination_table='raw_test_hunty.raw_main_user_info',
    project_id='strong-eon-372023',
    if_exists='replace')

df_user_extra_info.to_gbq(
    destination_table='raw_test_hunty.raw_user_extra_info',
    project_id='strong-eon-372023',
    if_exists='replace')
