#!/usr/bin/env python3

from SnowflakeConsolidatedReport import heavylifting
import concurrent.futures
import pandas as pd
import logging
import getopt
from time import strftime
import logging.handlers as handlers
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from snowflake.connector.errors import ProgrammingError
import smtplib
from email.message import EmailMessage
import pandas as pd
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
import socket
from threading import Lock
from datetime import date, timedelta, datetime
import sys
import configparser
import ast

def sendEmail(emailFrom, emailTo, emailSubject, emailBody, fileToSend):

  msg = MIMEMultipart()
  msg["From"] = emailFrom
  msg["To"] = emailTo
  msg["Subject"] = emailSubject
  msg.preamble = emailSubject

  ctype, encoding = mimetypes.guess_type(fileToSend)
  if ctype is None or encoding is not None:
    ctype = "application/octet-stream"

  maintype, subtype = ctype.split("/", 1)

  if maintype == "text":
    fp = open(fileToSend)
    attachment = MIMEText(fp.read(), _subtype=subtype)
    fp.close()
  elif maintype == "image":
    fp = open(fileToSend, "rb")
    attachment = MIMEImage(fp.read(), _subtype=subtype)
    fp.close()
  elif maintype == "audio":
    fp = open(fileToSend, "rb")
    attachment = MIMEAudio(fp.read(), _subtype=subtype)
    fp.close()
  else:
    fp = open(fileToSend, "rb")
    attachment = MIMEBase(maintype, subtype)
    attachment.set_payload(fp.read())
    fp.close()
    encoders.encode_base64(attachment)

  attachment.add_header("Content-Disposition", "attachment", filename=fileToSend.split('/')[-1])
  msg.attach(attachment)

  part1 = MIMEText(emailBody, 'plain')
  msg.attach(part1)

  try:
    smtpObj = smtplib.SMTP('<placeholder>', 25)
    smtpObj.sendmail(emailFrom, emailTo.split(','), msg.as_string())
    smtpObj.quit()
  except Exception as e:
    print("error sending an email"+e)

def getProps(propfile):
  separator = "="
  common = {}

  with open(propfile) as f:
    for line in f:
        if separator in line:
            name, value = line.split(separator, 1)
            common[name.strip()] = value.strip()
  return common

sf_home_folder = sys.argv[1]
sf_loglevel = sys.argv[2]

file_path = os.path.join(sf_home_folder,"data")
log_path = os.path.join(sf_home_folder, "logs")
script_path = os.path.join(sf_home_folder, "scripts")
usage_wh_file_name = file_path+'/SnowflakeConsolidatedReportCollection.csv'
propfile=script_path+'/SnowflakeMergeCommon.ini'

numeric_level = getattr(logging, sf_loglevel, None)
if not isinstance(numeric_level, int):
    print("Set right numeric level")

logger = logging.getLogger('')
logger.setLevel(numeric_level)

  ## Here we define our formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logHandler = handlers.TimedRotatingFileHandler(log_path + "/SnowflakeConsolidatedReportCollection.log",'midnight',1)
logHandler.setLevel(numeric_level)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

if os.path.isfile(propfile):
  propExists = True
else:
  propExists = False

if propExists:
  logger.info("\nRead properties from Properties file (" + propfile + ")...")
  #props = getProps(propfile)
  config = configparser.ConfigParser()
  config.read(propfile)
  config.optionxform=lambda option: option
  common = config['common']
  #sf_cmddata = common["sf_account"]
  #logger.info("sf_account = " + sf_cmddata)
  sf_databasename = config["sf_databasename"]
  #logger.info("sf_databasename = " + sf_databasename)
  #sf_account = common["sf_account"]
  #logger.info("sf_account = " + sf_account)
  sf_ismail = common["sf_ismail"]
  logger.info("sf_ismail = " + sf_ismail)
  #sf_dbratiocache = common["sf_dbratiocache"]
  #logger.info("sf_dbratiocache = " + sf_dbratiocache)
  sf_emailfrom = common["sf_emailfrom"]
  logger.info("sf_emailfrom = " + sf_emailfrom)
  sf_emailto = common["sf_emailto"]
  logger.info("sf_emailto = " + sf_emailto)
  sf_reportperiod = common["sf_reportperiod"]
  logger.info("sf_reportperiod = " + sf_reportperiod)
  sf_datefrom = common["sf_datefrom"]
  logger.info("sf_datefrom = " + sf_datefrom)
  sf_dateto = common["sf_dateto"]
  logger.info("sf_dateto = " + sf_dateto)
  sf_alldatabase = ast.literal_eval(common["sf_alldatabase"])
  #logger.info("sf_alldatabase = " + sf_alldatabase)

else:
  logger.info("\nProperties file (" + propfile + ") does not exist. Can not proceed...")
  exit(1)

if (sf_reportperiod == 'W'):
  logger.info("Report for last week...")
  start_date_prev = date.today() + timedelta(-date.weekday(date.today()), weeks=-1)
  end_date_prev = date.today() + timedelta(-date.weekday(date.today()) - 1)
elif (sf_reportperiod == 'M'):
  logger.info("Report for last month...")
  end_date_prev = date.today().replace(day=1) - timedelta(days=1)
  start_date_prev = date.today().replace(day=1) - timedelta(days=end_date_prev.day)
else:
  logger.info("Report for date range in properties file...")
  start_date_prev = datetime.strptime(sf_datefrom, '%Y-%m-%d') # from properties file
  end_date_prev = datetime.strptime(sf_dateto, '%Y-%m-%d') # from properties file

no_of_days = end_date_prev.day - start_date_prev.day + 1
prev_first = start_date_prev.strftime('%Y-%m-%d')
prev_last = end_date_prev.strftime('%Y-%m-%d')

sf_cmddata = []
for k,v in sf_databasename.items():

    print(k,v)
    if sf_alldatabase:
       sf_cmddata.append([sf_home_folder, k.split('-')[0].capitalize(), k.split('-')[1].capitalize(), sf_loglevel, ast.literal_eval(v), sf_alldatabase, prev_first, prev_last, no_of_days])
    elif ast.literal_eval(v) != []:
       sf_cmddata.append([sf_home_folder, k.split('-')[0].capitalize(), k.split('-')[1].capitalize(), sf_loglevel, ast.literal_eval(v), sf_alldatabase, prev_first, prev_last, no_of_days])

#sf_cmddata = [[sf_home_folder, "","","", sf_loglevel, sf_alldatabase, prev_first, prev_last, no_of_days], []]

lock = Lock()
print(sf_cmddata)
with concurrent.futures.ThreadPoolExecutor() as executor:

    futures = [executor.submit(heavylifting, param, lock) for param in sf_cmddata]
    result = [f.result() for f in futures]
""" Parent Thread code for merging results, save to csv and mail below """

try:

  with open(usage_wh_file_name,'w+') as f:

    result = pd.concat(result)
    result.to_csv(f)

except Exception as e:
  logger.info("Error writing result to csv file")
  raise

f.close()
account = ''
for _ in sf_cmddata:
  account += ''.join(_[1:3])
  account += ','

emailSubject = 'Report - Snowflake Database - ( ' +account+ ' ) Size Usage'
curdate = pd.Timestamp("today").strftime("%m/%d/%Y %H:%M")
hostname = socket.gethostname()
emailBody = "Report Execution Date     : "+curdate+"\nReport Range                    : "+prev_first+" TO "+prev_last+ "\nExecuted on instance        : "+hostname

try:
    sendEmail(sf_emailfrom, sf_emailto, emailSubject, emailBody, usage_wh_file_name)
    logger.info("Successfully sent email")
except Exception:
    logger.info("Error: unable to send email")
    raise
