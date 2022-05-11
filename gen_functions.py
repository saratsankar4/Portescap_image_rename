from config import config as cfg
from db_connection import read_from_db, save_to_db
import os, os.path
import numpy as np
import pandas as pd
from datetime import datetime, time
import schedule
import time
import pysftp
import ftplib

cnopts = pysftp.CnOpts()

table_details = cfg('table_details')

db_details = cfg('db')

f_path = cfg('folder_path')

source_path = f_path['source_path']

ftp = cfg('sftp')


def copy_local_remote(src_file_name, dest_file_name, src_path, dest_file_path):

    host = ftp['host']
    user = ftp['user']
    passwrd = ftp['pass']

    src_file_path = src_path + '/' + src_file_name
    print(src_path)

    ftp_server = ftplib.FTP(host, user, passwrd)
    ftp_server.cwd(dest_file_path)

    file = open(src_file_path, 'rb')  # file to send
    ftp_server.storbinary('STOR %s' % src_file_name, file)
    print('file_copied')
    ftp.rename(src_file_name, dest_file_name)
    print('file_renamed:' + src_file_name + ' to ' + dest_file_name)
    file.close()


# to get modified date of file
def get_mod_time(a):
    folder_path = f_path['source_path']
    modificationtime = time.strftime('%Y-%m-%d %H:%M:%S',
                                     time.localtime(os.path.getmtime(os.path.join(folder_path, a))))
    return modificationtime


def get_last_time(file_name):
    records = read_from_db("select max(last_modified) from "+table_details['folder_data'] +
                           " where source_file_name = " + "'" + str(file_name) + "'", cfg('db'))

    print(records['max'][0], file_name)
    return records['max'][0]


def get_station(a):
    station_id = a[5:7]
    return station_id


def get_timestamp(a):
    year = a[8:12]
    month = a[13:15]
    day = a[16:18]
    hr = a[19:21]
    mm = a[22:24]
    ss = a[25:27]
    ts = year + '-' + month+'-'+day+' ' + hr + ':' + mm + ':' + ss
    time_stamp = pd.to_datetime(ts, format='%Y-%m-%d %H:%M:%S')
    time_stamp = time_stamp.strftime('%Y-%m-%d %H:%M:%S')
    print(time_stamp)
    return time_stamp


def get_abs_path(file_name):
    folder_path = f_path['source_path']
    fil_path = os.path.abspath(os.path.join(folder_path, file_name))
    return fil_path


def get_gir_partnum(tim):
    gir_part = read_from_db('''select distinct gm.item, gm.gir from gir_master gm 
                              inner join iqa_measurement_details imd
                                      on gm.id=imd.gir_master_id 
                                   where imd.creation_date in
                                         ( select max(creation_date) from iqa_measurement_details iq
                                           where creation_date >= cast ((''' + "'" + tim + "'" + ''') as timestamp)
                                             and imd.gir_master_id = iq.gir_master_id) ''', cfg('db'))

    return gir_part


def get_wo_artnum(tim):
    wo_art = read_from_db('''select distinct wo.work_order, wo.article 
                                   from  top_qa_lot tqt, 
                                         production.work_order wo  
                                   where tqt.id_work_order=wo.id 
                                     and tqt.lot_creation_date in
                                         ( select max(lot_creation_date) from top_qa_lot  tql
                                           where lot_creation_date >= cast ((''' + "'" + tim + "'" + ''') as timestamp)
                                             and tql.id = tqt.id) ''', cfg('db'))

    return wo_art


def insert_table(i):
    df = pd.DataFrame()
    src_file_name = i
    modificationtime = get_mod_time(src_file_name)
    time_stamp = get_timestamp(i)
    src_path = get_abs_path(i)
    station_id = get_station(i)
    if station_id == '08':
        df_station = get_gir_partnum(time_stamp)
        gir_num = df_station['gir'][0]
        part_num = df_station['item'][0]
    elif station_id == '09':
        df_station = get_wo_artnum(time_stamp)
        gir_num = df_station['work_order'][0]
        part_num = df_station['article'][0]

    else:
        gir_num = 0
        part_num = 0

    dest_file_name = str(gir_num) + '_' + str(part_num) + '_' + i[28:]
    dest_file_path = f_path['destination_path']
    # ct = datetime.datetime.now()
    # time_copied = ct.strftime('%Y-%m-%d %H:%M:%S')

    end_date = '31/12/2999'

    df = df.append(
        {'station_id': int(station_id), 'source_file_name': src_file_name, 'source_path': src_path,
         'file_time_stamp': time_stamp, 'gir': gir_num, 'part_number': part_num, 'dest_file_name': dest_file_name,
         'dest_file_path': dest_file_path, 'last_modified': modificationtime, 'end_date': end_date},
        ignore_index=True)

    copy_local_remote(src_file_name, dest_file_name, src_path, dest_file_path)

    save_to_db(table_details['folder_data'], "append", cfg('db'), df)


def update_table(file_name, temp_dte):
    df = pd.DataFrame()

    df = df.append({'source_file_name': file_name, 'last_modified': temp_dte}, ignore_index=True)

    save_to_db(table_details['folder_data'], "update", cfg('db'), df)


def update_check(old_data, new_files):
    for i in new_files:
        if i.endswith(".jpg") or i.endswith(".JPG"):

            modificationtime = get_mod_time(i)
            if i in list(old_data['source_file_name']):
                temp_dte = get_last_time(i)
                # print(temp_dte, type(temp_dte), modificationtime, type(modificationtime))
                if str(temp_dte) != modificationtime:
                    update_table(i, temp_dte)
                    print('updated')
                    insert_table(i)
                    print('debug: 1 inserted')
            else:
                if not i.startswith('~'):
                    # copy_remote_local(source_path, i)
                    insert_table(i, source_path)
                print('debug: 2: inserted')


def insert_all(files):
    for i in files:
        if not i.startswith('~') and (i.endswith(".jpg") or i.endswith(".JPG")):
            insert_table(i)
            print('debug: 3: inserted')


def job():
    folder_path = f_path['source_path']
    files = os.listdir(str(folder_path))
    f_info = read_from_db("select * from " + table_details['folder_data'], cfg('db'))

    # insert_all(files)
    if f_info.shape[0] != 0:
        print('1')
        update_check(f_info, files)

    else:
        print('2')
        insert_all(files)


schedule.every(300).seconds.do(job)

while True:
    schedule.run_pending()
    time.sleep(300-1)
