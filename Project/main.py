import sqlite3
import sys
from py_scripts import helpers, loaddb1, load1db_terminals, fraud
if __name__ == "__main__":
    con = sqlite3.connect('sber.db')
    loaddb1.sql_load(con, './sql_scripts/init.sql')
    loaddb1.bank(con)
    date = ''
    try:
        date = helpers.getFileDate()
    except Exception as e:
        print('Error: ' + str(e))
        sys.exit()
    if date != '':
        loaddb1.transactions(con, date)
        loaddb1.passport_blacklist(con, date)
        load1db_terminals.incremental_load(con, date)
        fraud.passport(con, date)
        fraud.account(con, date)
        fraud.city(con)
        fraud.sum_guessing(con)
    helpers.showData(con, 'REP_FRAUD')
