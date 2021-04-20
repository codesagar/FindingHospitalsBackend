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
from googlemaps import Client as GoogleMaps
gmaps = GoogleMaps(os.getenv('GMAPS_API'))


beds_status = pd.read_excel("https://coronacheck.blob.core.windows.net/$web/Vadodara%20Hospitals%20Complete.xlsx", engine='openpyxl')
hospitals = pd.read_excel("https://coronacheck.blob.core.windows.net/$web/Vadodara%20Hospitals.xlsx", engine='openpyxl')

beds_status['Vacant-ICU'] = pd.to_numeric(beds_status['Vacant-ICU'])
beds_status['Vacant-O2'] = pd.to_numeric(beds_status['Vacant-O2'])
beds_status['Vacant-GEN'] = pd.to_numeric(beds_status['Vacant-GEN'])

tz_IN = pytz.timezone('Asia/Kolkata') 
datetime_IN = datetime.now(tz_IN)
last_updated = datetime_IN.strftime("%d %b %I:%M %p")

# You need to set MongoDB connection strings as environment variables before proceeding
client = MongoClient(os.getenv("MONGOURL"))
db = client.corona    #Select the database
db.authenticate(name=os.getenv("MONGO_USERNAME"),password=os.getenv("MONGO_PASSWORD"))
user_request = db.hospital_request

## Helper functions
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

def add_google_data(df):
    gmaps = GoogleMaps(os.getenv('GMAPS_API'))
    df['Lookup'] =  df['Hospital Name'] + ", " + df['Hospital Address']
    df['Lat'] = ""
    df['Lon'] = ""
    df['Place ID'] = ""
    df['Map Link'] = ""
    df['Contact'] = ""
    for idx, row in df.iterrows():
        try:
            geocode_result = gmaps.geocode(row['Lookup'])
            df['Lat'][idx] = geocode_result[0]['geometry']['location']['lat']
            df['Lon'][idx] = geocode_result[0]['geometry']['location']['lng']
            df['Place ID'][idx] = geocode_result[0]['place_id']
            df['Map Link'][idx] = "https://www.google.com/maps/search/?api=1&query="+row['Hospital Name'].replace(" ","+")+"&query_place_id="+geocode_result[0]['place_id']
            query = "https://maps.googleapis.com/maps/api/place/details/json?place_id="+geocode_result[0]['place_id']+"&fields=formatted_phone_number&key=AIzaSyA6rztAURUGFVYTPZLYHw_oU8uXILDRhhc"
            r = requests.get(query)
            if 'formatted_phone_number' in r.json()['result'].keys(): 
                df['Contact'][idx] = r.json()['result']['formatted_phone_number']
        except:
            pass
    return df

## Data updation
def update_data():
	global hospitals
	global beds_status
	global last_updated
	o2 = get_table_by_url("https://vmc.gov.in/Covid19VadodaraApp/HospitalBedsDetails.aspx?tid=43","O2")
	icu = get_table_by_url("https://vmc.gov.in/Covid19VadodaraApp/HospitalBedsDetails.aspx?tid=63","ICU")
	gen = get_table_by_url("https://vmc.gov.in/Covid19VadodaraApp/HospitalBedsDetails.aspx?tid=53","GEN")
	all_hospitals = get_full_data("https://vmc.gov.in/Covid19VadodaraApp/HospitalBedsDetails.aspx?tid=13")
	merged = icu.merge(o2, how='outer', left_on=['Hospital Name','Hospital Address'], right_on=['Hospital Name','Hospital Address'])
	merged = merged.merge(gen, how="outer", left_on=['Hospital Name','Hospital Address'], right_on=['Hospital Name','Hospital Address'])
	beds = merged.merge(hospitals, how='left', left_on=['Hospital Name','Hospital Address'], right_on=['Hospital Name','Hospital Address'])

	if sum([False if h in hospitals['Hospital Name'].values else True for h in beds['Hospital Name']])>0:
		print("Adding new hospitals data")
		new_hospitals = beds[[False if h in hospitals['Hospital Name'].values else True for h in beds['Hospital Name']]]
		new_hospitals = new_hospitals[['Hospital Name','Hospital Address']].merge(all_hospitals, how='left', left_on=['Hospital Name','Hospital Address'], right_on=['Hospital Name','Hospital Address'] )
		new_hospitals = add_google_data(new_hospitals)
		hospitals = hospitals.append(new_hospitals)
		beds = merged.merge(hospitals, how='left', left_on=['Hospital Name','Hospital Address'], right_on=['Hospital Name','Hospital Address'])
		hospitals.to_excel("Vadodara Hospitals.xlsx",engine='openpyxl',index=False)
		beds.to_excel("Vadodara Hospitals Complete.xlsx", engine='openpyxl', index=False)
	beds_status = beds.copy()
	beds_status['Vacant-ICU'] = pd.to_numeric(beds_status['Vacant-ICU'])
	beds_status['Vacant-O2'] = pd.to_numeric(beds_status['Vacant-O2'])
	beds_status['Vacant-GEN'] = pd.to_numeric(beds_status['Vacant-GEN'])
	datetime_IN = datetime.now(tz_IN)
	last_updated = datetime_IN.strftime("%d %b %I:%M %p")


def print_status():
	global beds_status
	print(beds_status['Vacant-ICU'].sum(), beds_status['Vacant-O2'].sum(), beds_status['Vacant-GEN'].sum()) 

## Scheduler
sched = BackgroundScheduler(daemon=True)
sched.start()
sched.add_job(update_data,'interval',minutes=5, next_run_time=datetime.now())


## App instantiation and cross origin requests setup
app = Flask(__name__)
CORS(app)

## MAIL Config
sender_address = os.getenv("GMAIL_SRC")
sender_pass = os.getenv("GMAIL_PASS")
receiver_address = os.getenv("GMAIL_DEST")


def get_distance(lat1, lon1, lat2, lon2):
    p = pi/180
    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p) * cos(lat2*p) * (1-cos((lon2-lon1)*p))/2
    return round(12742 * asin(sqrt(a)),2) #2*R*asin...

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

	hospital_type = req_json['type']
	if hospital_type == "ICU":
		result = beds_status[beds_status['Vacant-ICU']>0]
	elif hospital_type == "O2":
		result = beds_status[beds_status['Vacant-O2']>0]
	elif hospital_type == "GEN":
		result = beds_status[beds_status['Vacant-GEN']>0]

	result.assign(Distance = 100)
	
	for idx,row in result.iterrows():
		result.loc[idx,'Distance'] = get_distance(row['Lat'],row['Lon'],float(req_json['lat']),float(req_json['lon']))
	
	json_str = result.sort_values(['Distance'])[:10].to_json(orient='records')
	total_available = {
		"Total ICU available": beds_status['Vacant-ICU'].sum(),
		"Total O2 available": beds_status['Vacant-O2'].sum(),
		"Total GEN available": beds_status['Vacant-GEN'].sum()
	}

	response = {"success": True, "result":json.loads(json_str), "last_updated":last_updated, "availability status":total_available}
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