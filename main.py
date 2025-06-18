import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import paramiko
import gc 
import random
import numpy as np
from datetime import datetime, timedelta 
import boto3
from io import StringIO
import time

def tictoc(func):
        def wrapper(*args, **kwargs):
                t1 = time.time()
                result = func(*args, **kwargs)
                t2 = time.time()-t1
                print(f'Took {t2:.5f} seconds')
        return wrapper

@tictoc
def to_sql_table(df,engine,table_name,sch):
    df.to_sql(table_name, engine, if_exists='replace', index=False,schema=sch)

@tictoc
def to_sql_table_append(df,engine,table_name,sch):
    df.to_sql(table_name, engine, if_exists='append', index=False,schema=sch)    

hostname = '192.168.18.61'
username = 'batman'
password = 'batman'

remote_backup = '/home/batman/Downloads/pihole.db.backup'
local_copy = '/home/kratos/dockers/pihole.db.backup'

backup_cmd = 'sudo sqlite3 /etc/pihole/pihole-FTL.db ".backup /home/batman/Downloads/pihole.db.backup"'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(hostname, username=username, password=password)

stdin, stdout, stderr = ssh.exec_command(backup_cmd)
print("COMMAND OUTPUT:\n", stdout.read().decode())
print("COMMAND ERRORS:\n", stderr.read().decode())

sftp = ssh.open_sftp()
sftp.get(remote_backup, local_copy)
sftp.close()
ssh.close()

print("✅ Backup file copied successfully.")

#### Complete DB creation in PostgreSQL

#sudo sqlite3 /etc/pihole/pihole-FTL.db ".backup /home/batman/Downloads/pihole.db.backup"

cred = 'postgresql://ashwany:limit7841@192.168.18.61:5432/pi-hole-bi'
engine = create_engine(cred)
db = sqlite3.connect(r'/home/kratos/dockers/')
db_cur= db.cursor()

file = r'/home/kratos/dockers/Pihole_DB.xlsx'

def read_excel_file(file_path, sheet):
    xls = pd.read_excel(file_path, sheet_name=sheet)
    return xls

query_type = read_excel_file(file,'query_type')

try:
    query_type.to_sql('pi_hole_query_type', engine, if_exists='fail', index=False,schema='pi')
    print('Query Type appended successfully!')
except ValueError as e:
    print(f"Query_Type table is already created hence skipping this update.{e}")
    # Handle the error as needed
    
status_type = read_excel_file(file,'status_type')
try:
    status_type.to_sql('pi_hole_status_type', engine, if_exists='fail', index=False,schema='pi')
    print('Status Type appended successfully!')
except ValueError as e:
    print(f"Status_Type table is already created hence skipping this update.{e}")

reply_type = read_excel_file(file,'reply_type')

try:
    reply_type.to_sql('pi_hole_reply_type', engine, if_exists='fail', index=False,schema='pi')
    print('Reply Type appended successfully!')
except ValueError as e:
    print(f"Reply_Type table is already created hence skipping this update.{e}")

dnssec = read_excel_file(file,'dnssec')
try:
    dnssec.to_sql('pi_hole_dnssec', engine, if_exists='fail', index=False,schema='pi')
    print('DNSSEC appended successfully!')
except ValueError as e:
    print(f"DNSSEC table is already created hence skipping this update.{e}")

main = []
for row in db_cur.execute('SELECT * FROM queries'):
    df1= row
    main.append(df1)

main = pd.DataFrame(main)
main = main.rename(columns={0:'id', 1:'timestamp',2: 'type',3:'status',4:'domain',5:'client',6:'forward',7:'additional_info',8:'reply_type',9:'reply_time',10:'dnssec',11:'regex_id',12:'key_id'})
main['timestamp'] = pd.to_datetime(main['timestamp'], unit='s')
main['timestamp']= main['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
main['timestamp'] = pd.to_datetime(main['timestamp'])
#main['timestamp'] = main['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
main['reply_time'].fillna((main['reply_time'].mean()), inplace=True)
main['reply_time']=(main['reply_time']*1000000)
main['dnssec'] = np.random.randint(0,6, size=len(main))
main['forward'].fillna("-", inplace=True)
main['additional_info'].fillna(1, inplace=True)
main['regex_id'].fillna(0, inplace=True)


domain_by_id = []
for row in db_cur.execute('SELECT * FROM domain_by_id'):
    df1= row
    domain_by_id.append(df1)
domain_by_id = pd.DataFrame(domain_by_id)
domain_by_id =  domain_by_id.rename(columns={0:'id', 1:'domain'})

url = URL.create(
    drivername="postgresql+psycopg2",
    username="ashwany",
    password="limit7841",
    host="192.168.18.61",
    port=5432,
    database="pi-hole-bi"
)

engine1 = create_engine(url)

from sqlalchemy import text

qid = text("SELECT m.id, 1 as done FROM pi.pi_hole_domains m")
with engine1.connect() as conn:
    result = conn.execute(qid)
    id = pd.DataFrame(result.fetchall(), columns=result.keys())

id['id'].value_counts()

domain_by_id = domain_by_id.merge(id, on='id', how='left')
domain_by_id['done'] = domain_by_id['done'].fillna(0)
domain_by_id = domain_by_id[domain_by_id['done'] == 0]
domain_by_id = domain_by_id.drop(columns=['done'])


client_by_id = []
for row in db_cur.execute('SELECT * FROM client_by_id'):
    df1= row
    client_by_id.append(df1)
client_by_id = pd.DataFrame(client_by_id)
client_by_id =  client_by_id.rename(columns={0:'id', 1:'client'})
client_by_id = client_by_id.drop(2, axis=1)
clients = pd.read_csv(r'/home/kratos/dockers/client.csv')
if len(client_by_id) == len(clients):
    print('Client IDs are same.')
else:
    print('New Client IDs found.')

client_by_id.to_csv(r'/home/kratos/dockers/client1.csv', index=False)
client_by_id = client_by_id.merge(clients, on='id', how='left')
client_by_id.drop_duplicates('client', inplace=True,keep='last')

qid = text("SELECT m.id, 1 as done FROM pi.pi_hole_clients m")
with engine1.connect() as conn:
    result = conn.execute(qid)
    id = pd.DataFrame(result.fetchall(), columns=result.keys())

id['id'].value_counts()

client_by_id = client_by_id.merge(id, on='id', how='left')
client_by_id['done'] = client_by_id['done'].fillna(0)
client_by_id = client_by_id[client_by_id['done'] == 0]
client_by_id = client_by_id.drop(columns=['done'])


addinfo_by_id = []
for row in db_cur.execute('SELECT * FROM addinfo_by_id '):
    df1= row
    addinfo_by_id.append(df1)
addinfo_by_id = pd.DataFrame(addinfo_by_id)

addinfo_by_id =  addinfo_by_id.rename(columns={0:'id', 1:'type',2:'content'})

addinfo_by_id['id'] = addinfo_by_id['id'].astype(str)


qid = text("SELECT m.id, 1 as done FROM pi.pi_hole_add_info m")
with engine1.connect() as conn:
    result = conn.execute(qid)
    id = pd.DataFrame(result.fetchall(), columns=result.keys())

id['id'].value_counts()

addinfo_by_id = addinfo_by_id.merge(id, on='id', how='left')
addinfo_by_id['done'] = addinfo_by_id['done'].fillna(0)
addinfo_by_id = addinfo_by_id[addinfo_by_id['done'] == 0]
addinfo_by_id = addinfo_by_id.drop(columns=['done'])

qid = text("SELECT m.id, 1 as done FROM pi.pi_hole_main m")
with engine1.connect() as conn:
    result = conn.execute(qid)
    id = pd.DataFrame(result.fetchall(), columns=result.keys())

id['id'].value_counts()

main = main.merge(id, on='id', how='left')
main['done'] = main['done'].fillna(0)
main = main[main['done'] == 0]
main = main.drop(columns=['done'])

to_sql_table_append(domain_by_id,engine,'pi_hole_domains','pi')
to_sql_table_append(client_by_id,engine,'pi_hole_clients','pi')
to_sql_table_append(addinfo_by_id,engine,'pi_hole_add_info','pi')
to_sql_table_append(main,engine,'pi_hole_main','pi')

print('Add_info appended successfully!')
print("Main Data appended successfully!")
print('Domains appended successfully!')
print('Clients appended successfully!')
print('Everything appended successfully! DB is created.')
