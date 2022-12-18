import os
import pandas as pd
from py_scripts import helpers

def printLoading(filePath):
    print(helpers.bcolors.OKGREEN + 'Loading ' + filePath + '...'+ helpers.bcolors.ENDC)
def sql_load(con, filePath):
    printLoading(filePath)
    cursor = con.cursor()
    sql_file = open(filePath)
    sql_as_string = sql_file.read()
    cursor.executescript(sql_as_string)
    con.commit()
def csv_load(con, filePath, tableName):
    printLoading(filePath)
    df = pd.read_csv(filePath, sep=';')
    df.to_sql(tableName, con=con, if_exists='replace', index=False)
def excel_load(con, filePath, tableName):
    printLoading(filePath)
    df = pd.read_excel(filePath)
    df.to_sql(tableName, con=con, if_exists='replace', index=False)
def bank(con):
    sql_load(con, 'ddl_dml.sql')  # create accounts, cards, clients tables
    cursor = con.cursor()
    cursor.execute('''
        insert into DWH_DIM_CARDS (
            card_num, account_num, create_dt, update_dt
        ) select
            card_num, account, create_dt, update_dt from cards;
    ''')
    cursor.execute('''
        insert into DWH_DIM_ACCOUNTS (
            account_num, valid_to, client, create_dt, update_dt
        ) select
            account, valid_to, client, create_dt, update_dt from accounts;
    ''')
    cursor.execute('''
        insert into DWH_DIM_CLIENTS (
            client_id, last_name, first_name, patronymic, date_of_birth,
            passport_num, passport_valid_to, phone, create_dt, update_dt
        ) select
            client_id, last_name, first_name, patronymic, date_of_birth,
            passport_num, passport_valid_to, phone, create_dt,
            update_dt from clients;
    ''')
    cursor.execute('drop table if exists cards')
    cursor.execute('drop table if exists accounts')
    cursor.execute('drop table if exists clients')
    con.commit()
def transactions(con, date):
    source_file = "transactions_" + date + ".txt"
    csv_load(con, source_file, 'STG_TRANSACTIONS')
    cursor = con.cursor()
    cursor.execute('''
        insert into DWH_FACT_TRANSACTIONS (
            trans_id, trans_date, amt, card_num, oper_type, oper_result,
            terminal
        ) select
            transaction_id, transaction_date, amount, card_num, oper_type,
            oper_result, terminal from STG_TRANSACTIONS;
    ''')
    cursor.execute('drop table if exists STG_TRANSACTIONS')
    con.commit()
    backup_file = os.path.join("archive", "transactions_" + date + ".txt.backup")
    os.rename(source_file, backup_file)
    return date
def passport_blacklist(con, date):
    source_file = "passport_blacklist_" + date + ".xlsx"
    excel_load(con, source_file, 'STG_PASSPORT_BLACKLIST')
    cursor = con.cursor()
    cursor.execute('''
        insert into DWH_FACT_PASSPORT_BLACKLIST (
            passport_num, entry_dt
        ) select
            passport, date from STG_PASSPORT_BLACKLIST;
    ''')
    cursor.execute('drop table if exists STG_PASSPORT_BLACKLIST')
    backup_file = os.path.join("archive", "passport_blacklist_" + date + ".xlsx.backup")
    os.rename(source_file, backup_file)
    con.commit()