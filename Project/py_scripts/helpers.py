import os
import sys
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
def countData(con, objName):
    cursor = con.cursor()
    cursor.execute(f'select count(*) from {objName}')
    result = cursor.fetchone()
    print(f'{result[0]} rows\n')
def showData(con, objName):
    cursor = con.cursor()
    print(bcolors.OKBLUE + '-_'*20 + '\n' + objName + '\n' + '-_'*20 + '\n'
            + bcolors.ENDC)
    cursor.execute(f'select * from {objName}')
    title = [i[0] for i in cursor.description]
    print(title)
    # for row in cursor.fetchall()[0:10]:
    for row in cursor.fetchall():
        print(row)
    countData(con, objName)
def printAllTables(con):
    showData(con, 'DWH_DIM_CARDS')
    showData(con, 'DWH_DIM_ACCOUNTS')
    showData(con, 'DWH_DIM_CLIENTS')
    showData(con, 'DWH_FACT_PASSPORT_BLACKLIST')
    showData(con, 'DWH_DIM_TERMINALS_HIST')
    showData(con, 'DWH_FACT_TRANSACTIONS')

def getFileDate():
    date = ''
    lst = os.listdir('.')
    lst.sort()
    for fname in lst:
        if fname.startswith('transactions'):
            date = fname.split("_", 1)[1].split(".", 1)[0]
            break
    if date == '':
        raise Exception('Files not found')
    if not os.path.isfile('transactions_' + date + '.txt'):
        raise Exception('transactions file not found')
    if not os.path.isfile('passport_blacklist_' + date + '.xlsx'):
        raise Exception('passport_blacklist file not found')
    if not os.path.isfile('terminals_' + date + '.xlsx'):
        raise Exception('terminals file not found')
    return date
