#!/usr/bin/env python3

from decimal import Decimal
import snowflake.connector
import logging
import getopt
from time import strftime
import logging.handlers as handlers
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from snowflake.connector.errors import ProgrammingError, DatabaseError
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
from datetime import date, timedelta
import json
from threading import Lock

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
    # Note: we should handle calculating the charset
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

  attachment.add_header("Content-Disposition", "attachment", filename=fileToSend)
  msg.attach(attachment)

  part1 = MIMEText(emailBody, 'plain')
  msg.attach(part1)

  try:

    smtpObj = smtplib.SMTP('<placeholder>', 25)
    smtpObj.sendmail(emailFrom, emailTo.split(','), msg.as_string())
    smtpObj.quit()
  except Exception as e:
     print("Error:", e)

def getProps(propfile):
  separator = "="
  props = {}

  with open(propfile) as f:
    for line in f:
        if separator in line:

            name, value = line.split(separator, 1)
            props[name.strip()] = value.strip()
  return props

def warehouse_spm_map(database_name, dic):
  wh = '_'.join(database_name.split('_')[:-1])+'_SPM_'+''.join(database_name.split('_')[-1:])
  return dic.get(wh, 0)

def warehouse_int_map(database_name, dic):
  wh = '_'.join(database_name.split('_')[:-1])+'_INT_'+''.join(database_name.split('_')[-1:])
  return dic.get(wh, 0)

def heavylifting(al,lock):

  sf_home_folder = al[0]
  sf_actype = al[1]
  sf_region = al[2]
  databaselist = al[4]
  sf_alldatabases = al[5]
  prev_first = al[6]
  prev_last = al[7]
  no_of_days = al[8]

  home_folder = sf_home_folder
  print("home_folder = " + home_folder)
  key_path = os.path.join(home_folder, "keys")
  log_path = os.path.join(home_folder, "logs")
  file_path = os.path.join(home_folder, "data")
  script_path = os.path.join(home_folder, "scripts")
  propfile = script_path+'/SnowflakeReportUsage'+sf_actype+sf_region+'.properties'

  numeric_level = getattr(logging, al[3], None)
  if not isinstance(numeric_level, int):
    print("Set right numeric level")
  logger = logging.getLogger('')
  logger.setLevel(numeric_level)

  ## Here we define our formatter
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

  logHandler = handlers.TimedRotatingFileHandler(log_path + "/SnowflakeConsolidatedReport"+sf_actype+sf_region+".log",'midnight',1)
  logHandler.setLevel(numeric_level)
  logHandler.setFormatter(formatter)

  logger.addHandler(logHandler)

  logger.debug("Debug message...")
  logger.info("Informative message")
  logger.error("Error message")

  if os.path.isfile(propfile):
    propExists = True
  else:
    propExists = False

  if propExists:
    logger.info("\nRead properties from Properties file (" + propfile + ")...")
    props = getProps(propfile)

    sf_account = props["sf_account"]
    logger.info("sf_account = " + sf_account)
    sf_admin_user = props["sf_admin_user"]
    logger.info("sf_admin_user = " + sf_admin_user)
    sf_private_key = props["sf_private_key"]
    logger.info("sf_private_key = " + sf_private_key)
    sf_ismail = props["sf_ismail"]
    logger.info("sf_ismail = " + sf_ismail)
    sf_emailfrom = props["sf_emailfrom"]
    logger.info("sf_emailfrom = " + sf_emailfrom)
    sf_emailto = props["sf_emailto"]
    logger.info("sf_emailto = " + sf_emailto)
  else:
    logger.info("\nProperties file (" + propfile + ") does not exist. Can NOT proceed...")
    exit(1)

  logger.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
  logger.info("%%%%% Start processing for Snowflake Account: "+sf_account+" %%%%%")
  logger.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
  logger.info("")

  logger.info("prev day "+ prev_first)
  logger.info("prev last  "+ prev_last)

  keyfile = key_path+"/"+sf_actype+"_snowflake_spm_key.p8"
  logger.info("Read key file: "+keyfile)
  with open(keyfile, "rb") as key:
      p_key= serialization.load_pem_private_key(
          key.read(),
          password=str(sf_private_key).encode(),
          backend=default_backend()
      )

  pkb = p_key.private_bytes(
      encoding=serialization.Encoding.DER,
      format=serialization.PrivateFormat.PKCS8,
      encryption_algorithm=serialization.NoEncryption())

  logger.info("Going to create a Connection to Snowflake using the Key...")
  try:
    conn=snowflake.connector.connect(
        user=str(sf_admin_user),
        account=str(sf_account),
        private_key=pkb,
        database=str(sf_admin_user)
    )
  except Exception as ex:
    print(f"DB Connection error: {ex}")
    raise

  logger.info("")
  logger.info("******************************************")
  logger.info("***** Created a Snowflake Connection *****")
  logger.info("******************************************")
  logger.info("")

  try:
  #create cursor
    curs=conn.cursor()
    logger.info("")
    logger.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    logger.info("+++++ Created a Cursor using the Snowflake Connection +++++")
    logger.info("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    logger.info("")
  #execute SnowSQL to get Current Date
    curs.execute("select current_date")
    sfResults = curs.fetchall()
    logger.debug(sfResults[0][0])
  
    try:
      with open(script_path + '/dbratiofile'+sf_actype+'_'+sf_region, 'r') as f:
        ratio = json.loads(f.read())
    except FileNotFoundError:
      logger.info("dbratiofile will be written from scratch as dbratiofile can't be found")
      ratio = {}

    d ={}
    q1="select DATABASE_NAME from SNOWFLAKE.ACCOUNT_USAGE.DATABASES where DATABASE_NAME not in ('<placeholder>')"
    q2="select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME from %s.INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA != 'INFORMATION_SCHEMA'"
    q3="select csv/sample_rows*total_rows from (select sum(length(csv_line)-1)/1024/1024/1024 csv, count(*) sample_rows from (select replace(array_construct(a.*)::string, 'undefined', '') csv_line from %s.%s.%s a limit 1000))a join (select count(*) total_rows from %s.%s.%s) b"
    q4="select current_date() as date, current_account() as snowaccount, DATABASE_NAME, round(sum(average_database_bytes+average_failsafe_bytes)/1024/1024/1024,2) COMPRESSED_STORAGE_USED_GB from SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY where usage_date = current_date() group by DATABASE_NAME"
    q5="select DATABASE_NAME, round(((sum(average_database_bytes+average_failsafe_bytes))/1024/1024/1024)/%s,2) avg_daily_storage_used_GB from table(snowflake.information_schema.database_storage_usage_history('"+prev_first+"', '"+prev_last+"')) group by DATABASE_NAME"
    q6="select warehouse_name warehouse_name, round(sum(credits_used),2) credits_used from table(snowflake.information_schema.warehouse_metering_history('"+prev_first+"', '"+prev_last+"')) group by warehouse_name, len(warehouse_name) order by substring(warehouse_name,(len(warehouse_name)-3))"
    try:
      curs.execute(q4)
      q4res = curs.fetchall()
      colnames = [desc[0] for desc in curs.description]
      df = pd.DataFrame(q4res)
      df.columns = colnames
    except ValueError as ex:
      print(f"Bad day! - {ex}")
      return pd.DataFrame()

    curs.execute(q5 % (no_of_days))
    q5res = curs.fetchall()
    colnames = [desc[0] for desc in curs.description]
    df0 = pd.DataFrame(q5res)
    df0.columns = colnames

    curs.execute(q6)
    q6res = curs.fetchall()
    colnames = [desc[0] for desc in curs.description]
    df2 = pd.DataFrame(q6res)
    df2.columns = colnames
    dic = dict(zip(df2['WAREHOUSE_NAME'], df2['CREDITS_USED']))

    if sf_alldatabases:
      curs.execute(q1)
      q1res = curs.fetchall()
      databaselist = q1res
      #print(databaselist)

    for i in databaselist:
        unpack = i[0]
        d[unpack] = []

    for i in list(d):
        try:

          curs.execute(q2 % (i))
          q2res = curs.fetchall()
          d[i] = q2res
          #print(d[i])

        except Exception as e:
          print("unable to fetch table details for - " + i)
          print(f"exception {e}")
          del d[i]
          continue

    for k,v in d.items():
      if sf_actype+'_'+sf_region+'_'+k not in ratio:
        flag = 0
        size = 0
        for j in v:
            try:
              catalog, schema, table = j
            except ValueError:
              print("Oops! Give right permissions to the database - " + k)
              break
            try:
              curs.execute(q3 % (catalog, schema, table, catalog, schema, table))
              q3res = curs.fetchall()
             #print(q3res)
            except Exception as e:
              print(f"exception {e}")
              flag = 1
              break
            for i in q3res:
                unpack = i[0]
                if unpack is None:
                  pass
                else:
                  size+=unpack
        if flag == 1:
          continue
        ratio[k] = round(float(size),2)
        #print(ratio[k])
        df_index = df.set_index('DATABASE_NAME')
        try:
          ratio[k] = round(float(ratio[k] / float(df_index.loc[k][2])),2)
        except ZeroDivisionError as e:
          pass
        except KeyError as e:
          pass
        with lock:
          with open(script_path + '/dbratiofile'+sf_actype+'_'+sf_region, 'w') as f:
            ratio[sf_actype+'_'+sf_region+'_'+k] = ratio.pop(k)
            f.write(json.dumps(ratio))

    df_merge = pd.merge(df, df0, on='DATABASE_NAME')

    ratio_series = pd.Series(ratio)
    df_ratio = pd.DataFrame({'DATABASE_NAME':ratio_series.index,'ratio':ratio_series.values})
    df_ratio['DATABASE_NAME'] = df_ratio['DATABASE_NAME'].apply(lambda row: "_".join(row.split('_')[2:]))

    df_merge = pd.merge(df_merge, df_ratio, on='DATABASE_NAME')
    df_merge['AVG_DAILY_UNCOMPRESSED_STORAGE']=df_merge.apply(lambda row: round(row.ratio * float(row.AVG_DAILY_STORAGE_USED_GB),2), axis=1)

    df_merge['CUSTOMER'] = df_merge.apply(lambda row: row.DATABASE_NAME.split('_')[-1], axis=1)
    df_merge['ACCOUNT_TYPE'] = df_merge.apply(lambda row: row.DATABASE_NAME.split('_')[-2], axis=1)

    df_merge['WH_SPM_CREDITS_LAST_MONTH']=df_merge.apply(lambda row: dic.get('_'.join(row.DATABASE_NAME.split('_')[:-1]+['SPM']+row.DATABASE_NAME.split('_')[-1:]),0),axis=1)
    df_merge['WH_INT_CREDITS_LAST_MONTH']=df_merge.apply(lambda row: dic.get('_'.join(row.DATABASE_NAME.split('_')[:-1]+['INT']+row.DATABASE_NAME.split('_')[-1:]),0),axis=1)

    df_merge['MONTHLY_SNOWFLAKE_COST_$$']=df_merge.apply(lambda row: round((float(row.WH_SPM_CREDITS_LAST_MONTH) + float(row.WH_INT_CREDITS_LAST_MONTH)) * 3.52 + (float(row.AVG_DAILY_STORAGE_USED_GB)/1000) * 23,2), axis = 1)

    df_merge.pop('COMPRESSED_STORAGE_USED_GB')
    df_merge.pop('ratio')

    #df_merge = df_merge.reset_index(drop=True)
    df_merge = df_merge[['DATE','SNOWACCOUNT','CUSTOMER','ACCOUNT_TYPE','DATABASE_NAME','AVG_DAILY_UNCOMPRESSED_STORAGE','AVG_DAILY_STORAGE_USED_GB','WH_SPM_CREDITS_LAST_MONTH','WH_INT_CREDITS_LAST_MONTH','MONTHLY_SNOWFLAKE_COST_$$']]

  except ProgrammingError as db_ex:
    logger.info(f"Programming error: {db_ex}")
    print(f"Fix the following; meanwhile this will return an empty dataframe for this snow account - {db_ex}")
    return pd.DataFrame()
    #raise
  except DatabaseError as db_ex1:
    logger.info(f"Database Related error: {db_ex1}")
    raise

  finally:
    curs.close()
    logger.info("")
    logger.info("+++++++++++++++++++++++++++++")
    logger.info("+++++ Closed the Cursor +++++")
    logger.info("+++++++++++++++++++++++++++++")
    logger.info("")

  conn.close()
  logger.info("")
  logger.info("*******************************************")
  logger.info("***** Closed the Snowflake Connection *****")
  logger.info("*******************************************")
  logger.info("")

  return df_merge

if __name__=="__main__":
    heavylifting()
