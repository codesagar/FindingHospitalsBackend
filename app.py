import os
from flask import Flask
import flask
import sys
from flask_cors import CORS
from pymongo import MongoClient
import pandas as pd
import numpy as np
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
from math import cos, asin, sqrt, pi
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import requests
from bs4 import BeautifulSoup
import pytz
from urllib.parse import urljoin
from googlemaps import Client as GoogleMaps
gmaps = GoogleMaps(os.getenv('GMAPS_API'))

hyd_hospitals = pd.read_excel("https://coronacheck.blob.core.windows.net/$web/Hyderabad%20Hospitals.xlsx", engine="openpyxl")
hyd_hospitals.drop(['Hospital_Type','CONTACT'],axis=1, inplace=True)
vad_hospitals = pd.read_excel("https://coronacheck.blob.core.windows.net/$web/Vadodara%20Hospitals.xlsx", engine='openpyxl')


base_URL = "http://164.100.112.24/SpringMVC/"

p_page = requests.post(
    urljoin(base_URL, "getHospital_Beds_Status_Citizen.htm"),
    data={"hospital": "P"},
    stream=True,
)
g_page = requests.post(
    urljoin(base_URL, "getHospital_Beds_Status_Citizen.htm"),
    data={"hospital": "G"},
    stream=True,
)


def clean_hospital(hospital_name):
    return "".join(hospital_name.split(".")[1:]).strip()

def get_hyd_data(page):
    soup = BeautifulSoup(page.text, 'html.parser')
    tables = soup.find_all("table")
    column_head = ['DISTRICT','HOSPITAL','CONTACT','Total-GEN','Occupied-GEN','Vacant-GEN','Total-O2','Occupied-O2','Vacant-O2','Total-ICU','Occupied-ICU','Vacant-ICU','Total-TOTAL','Occupied-TOTAL','Vacant-TOTAL','LAST DATE','LAST TIME','LOL']
    final_head = column_head[:-1]
    table = tables[0]
    tab_data = [[cell.text.strip() for cell in row.find_all(["th","td"])]
                            for row in table.find_all("tr")]
    df = pd.DataFrame(tab_data, columns=column_head)
    district = ""
    data_list = []
    for idx,row in df.iterrows():
        if '.' not in row['DISTRICT']:
            district = row['HOSPITAL']
            temp = [district]
            temp.extend(row.values[2:])
            data_list.append(temp)
        else:
            temp = [district]
            temp.extend(row.values[:-2])
            data_list.append(temp)
            
    df = pd.DataFrame(data_list, columns=final_head)
    df['HOSPITAL'] = df['HOSPITAL'].apply(lambda x: clean_hospital(x))
    return df[3:]


private_hospital = get_hyd_data(p_page)
# Adding hospital type private/government
private_hospital = private_hospital.assign(Hospital_Type="Private")
government_hosptal = get_hyd_data(g_page)
government_hosptal = government_hosptal.assign(Hospital_Type="Government")
## Combining both
all_hospitals = pd.concat([private_hospital, government_hosptal])

hyd_final = all_hospitals.merge(hyd_hospitals,how="left", on=['DISTRICT', 'HOSPITAL'])
hyd_final.drop(['Total-GEN', 'Occupied-GEN','Total-O2', 'Occupied-O2','Total-ICU','Occupied-ICU','Total-TOTAL', 'Occupied-TOTAL', 'Vacant-TOTAL'], axis=1, inplace=True)
## removing summary rows
hyd_final = hyd_final[hyd_final['HOSPITAL'].str.len() > 0]

def get_full_data(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    tables = soup.find_all("table")
    result = pd.DataFrame()
    for table in tables:
        tab_data = [[cell.text for cell in row.find_all(["th","td"])] for row in table.find_all("tr")]
        temp = pd.DataFrame(tab_data)
        temp.rename(columns=temp.iloc[0], inplace=True)
        temp = temp[['Hospital Name', 'Hospital Address', 'Nodal Officer Name', 'Nodal Officer Mobile No', 'Cluster Name']]
        result = result.append(temp[1:])
    return result

def get_table_by_url(url, url_type):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    tables = soup.find_all("table")
    result = pd.DataFrame()
    for table in tables:
        tab_data = [[cell.text for cell in row.find_all(["th","td"])] for row in table.find_all("tr")]
        temp = pd.DataFrame(tab_data)
        temp.rename(columns=temp.iloc[0], inplace=True)
        temp = temp[['Hospital Name', 'Hospital Address', 'Vacant']]
        temp.rename({"Vacant":"Vacant-"+url_type}, axis=1, inplace=True)
        result = result.append(temp[1:])
    return result

o2 = get_table_by_url("https://vmc.gov.in/Covid19VadodaraApp/HospitalBedsDetails.aspx?tid=43","O2")
icu = get_table_by_url("https://vmc.gov.in/Covid19VadodaraApp/HospitalBedsDetails.aspx?tid=63","ICU")
gen = get_table_by_url("https://vmc.gov.in/Covid19VadodaraApp/HospitalBedsDetails.aspx?tid=53","GEN")

merged = icu.merge(o2, how='outer', left_on=['Hospital Name','Hospital Address'], right_on=['Hospital Name','Hospital Address'])
merged = merged.merge(gen, how="outer", left_on=['Hospital Name','Hospital Address'], right_on=['Hospital Name','Hospital Address'])
vad_final = merged.merge(vad_hospitals, how='left', left_on=['Hospital Name','Hospital Address'], right_on=['Hospital Name','Hospital Address'])

hyd_dict = hyd_final.to_dict('records')
vad_dict = vad_final.to_dict('records')
all_dict = hyd_dict + vad_dict
final_df = pd.DataFrame(all_dict)

for idx, row in final_df.iterrows():
    if pd.isnull(row['Hospital Name']):
        final_df.loc[idx,'Hospital Name'] = str(row['HOSPITAL']) + " - " + str(row['DISTRICT'])
        
final_df.drop(['DISTRICT','HOSPITAL'], axis=1, inplace=True)

final_df['Vacant-ICU'] = pd.to_numeric(final_df['Vacant-ICU'])
final_df['Vacant-O2'] = pd.to_numeric(final_df['Vacant-O2'])
final_df['Vacant-GEN'] = pd.to_numeric(final_df['Vacant-GEN'])

# Last Update logging
tz_IN = pytz.timezone('Asia/Kolkata') 
datetime_IN = datetime.now(tz_IN)
last_updated = datetime_IN.strftime("%d %b %I:%M %p")

def get_distance(lat1, lon1, lat2, lon2):
    p = pi/180
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p) * cos(lat2*p) * (1-cos((lon2-lon1)*p))/2
    return round(12742 * asin(sqrt(a)),2) #2*R*asin...

## App instantiation and cross origin requests setup
app = Flask(__name__)
CORS(app)

## MAIL Config
sender_address = os.getenv("GMAIL_SRC")
sender_pass = os.getenv("GMAIL_PASS")
receiver_address = os.getenv("GMAIL_DEST")

## Home
@app.route("/")
def hello():
	msg = "Hi there, we're breathing just fine. We're trying to save lives. Hope you go easy on us."
	response = msg + "<br><br>ICU available = " + str(beds_status['Vacant-ICU'].sum()) + "<br>O2 available = "+str(beds_status['Vacant-O2'].sum())+"<br>GEN available = "+str(beds_status['Vacant-GEN'].sum())+"<br><br>Last Updated = "+last_updated
	return response


# Prediction API endpoint
@app.route("/hospitals", methods=["POST"])
def predict():
	req_json = flask.request.json
	print('Request JSON', req_json)
	q_lat = float(req_json['lat'])
	q_lon = float(req_json['lon'])
	hospital_type = req_json['type']
	# if hospital_type == "ICU":
	# 	result = final_df[final_df['Vacant-ICU']>0]
	# elif hospital_type == "O2":
	# 	result = final_df[final_df['Vacant-O2']>0]
	# elif hospital_type == "GEN":
	# 	result = final_df[final_df['Vacant-GEN']>0]

	final_df['Distance'] = final_df.apply(lambda x: get_distance(x.Lat, x.Lon,q_lat,q_lon), axis=1)
	if sum(final_df['Distance']<20) > 10:
		dist_filtered = final_df[final_df['Distance']<20]
	else:
		dist_filtered = final_df.copy()
	print(dist_filtered.shape)
	# dist_filtered['Vacant-ICU'] = pd.to_numeric(dist_filtered['Vacant-ICU'])
	# dist_filtered['Vacant-O2'] = pd.to_numeric(dist_filtered['Vacant-O2'])
	# dist_filtered['Vacant-GEN'] = pd.to_numeric(dist_filtered['Vacant-GEN'])
	
	if hospital_type == "ICU":
		dist_filtered = dist_filtered.sort_values(['Vacant-ICU'],ascending=False)
	elif hospital_type == "O2":
		dist_filtered = dist_filtered.sort_values(['Vacant-O2'],ascending=False)
	elif hospital_type == "GEN":
		dist_filtered = dist_filtered.sort_values(['Vacant-GEN'],ascending=False)

	json_str = dist_filtered.iloc[:15].to_json(orient='records')


	response = {"success": True, "result":json.loads(json_str), "last_updated":last_updated}
	# Log request
	try:
		user_request.insert_one(req_json)
	except:
		pass

	return flask.jsonify(response)


## Email
@app.route("/message", methods=["POST"])
def email():
	req_json = flask.request.json
	mail_content = req_json['message'] + "\n\n" + req_json['name'] + "\n" + str(req_json['contact'])
	message = MIMEMultipart()
	message['From'] = sender_address
	message['To'] = receiver_address
	message['Subject'] = req_json['name'] + ' for Finding Hospitals'
	message.attach(MIMEText(mail_content, 'plain'))
	#Create SMTP session for sending the mail
	try:
		session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
		session.starttls() #enable security
		session.login(sender_address, sender_pass) #login with mail_id and password
		text = message.as_string()
		session.sendmail(sender_address, receiver_address, text)
		session.quit()
		return flask.jsonify({"success": True})
	except:
		e = sys.exc_info()[0]
		return flask.jsonify({"success": False, "error":str(e)})