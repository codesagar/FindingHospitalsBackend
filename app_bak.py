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

beds_status = pd.read_excel("https://coronacheck.blob.core.windows.net/$web/Vadodara%20Hospitals%20Complete.xlsx", engine='openpyxl')

beds_status['Vacant-ICU'] = pd.to_numeric(beds_status['Vacant-ICU'])
beds_status['Vacant-O2'] = pd.to_numeric(beds_status['Vacant-O2'])
beds_status['Vacant-GEN'] = pd.to_numeric(beds_status['Vacant-GEN'])

print(beds_status.dtypes)
# You need to set MongoDB connection strings as environment variables before proceeding
client = MongoClient(os.getenv("MONGOURL"))
db = client.corona    #Select the database
db.authenticate(name=os.getenv("MONGO_USERNAME"),password=os.getenv("MONGO_PASSWORD"))
user_request = db.hospital_request


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
    return "Hi there, we're breathing just fine. We're trying to save lives. Hope you go easy on us."


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

	result['Distance'] = 100
	
	for idx,row in result.iterrows():
		result['Distance'].loc[idx] = get_distance(row['Lat'],row['Lon'],float(req_json['lat']),float(req_json['lon']))
	
	json_str = result.sort_values(['Distance'])[:10].to_json(orient='records')

	response = {"success": True, "result":json.loads(json_str)}
	
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