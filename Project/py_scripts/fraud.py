import re
from py_scripts import helpers
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
def passport(con, date):
    print(bcolors.OKGREEN + 'Searching passport fraud...' + bcolors.ENDC)
    date = re.sub(r"(\d\d)(\d\d)(\d{4})", r'\3-\2-\1', date)
    cursor = con.cursor()
    cursor.execute('''
        create table if not exists STG_CLIENT_NOT_VALID as
            select
                client_id, passport_num,
                last_name || ' ' || first_name || ' ' || patronymic as fio,
                phone,
                passport_valid_to
            from DWH_DIM_CLIENTS
            where
                ? > passport_valid_to
                or passport_num in (select passport_num from
                DWH_FACT_PASSPORT_BLACKLIST);
    ''', [date])
    # helpers.showData(con, 'STG_CLIENT_NOT_VALID')
    cursor.execute('''
        create view if not exists STG_ACCOUNT_NOT_VALID as
            select
                t1.account_num,
                t2.passport_num,
                t2.fio,
                t2.phone
            from DWH_DIM_ACCOUNTS t1
            inner join STG_CLIENT_NOT_VALID t2
            on t1.client = t2.client_id
            where t2.passport_valid_to is not null;
    ''')
    cursor.execute('''
        create view if not exists STG_CARDS_NOT_VALID as
            select
                t1.card_num,
                t2.passport_num,
                t2.fio,
                t2.phone
            from DWH_DIM_CARDS t1
            inner join STG_ACCOUNT_NOT_VALID t2
            on t1.account_num = t2.account_num;
    ''')
    # helpers.showData(con, 'STG_CARDS_NOT_VALID')
    cursor.execute('''
        create view if not exists STG_PASSPORT_FRAUD_VIEW as
            select
               t2.fio,
               t2.passport_num as passport,
               t2.phone,
               t1.trans_date as event_dt
            from DWH_FACT_TRANSACTIONS t1
            inner join STG_CARDS_NOT_VALID t2
            on t1.card_num = t2.card_num
    ''')
    # helpers.showData(con, 'STG_PASSPORT_FRAUD_VIEW')
    cursor.execute('''
        insert into REP_FRAUD (
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) select
           event_dt,
           passport,
           fio,
           phone,
           'passport',
           datetime('now')
        from STG_PASSPORT_FRAUD_VIEW
        where (event_dt, passport) in (
            select min(event_dt), passport
            from STG_PASSPORT_FRAUD_VIEW
            group by passport
            );
    ''')
    cursor.execute('drop table if exists STG_CLIENT_NOT_VALID')
    cursor.execute('drop view if exists STG_ACCOUNT_NOT_VALID')
    cursor.execute('drop view if exists STG_CARDS_NOT_VALID')
    cursor.execute('drop view if exists STG_PASSPORT_FRAUD_VIEW')
    con.commit()
def account(con, date):
    print(bcolors.OKGREEN + 'Searching account fraud...' + bcolors.ENDC)
    date = re.sub(r"(\d\d)(\d\d)(\d{4})", r'\3-\2-\1', date)
    cursor = con.cursor()
    cursor.execute('''
        create table if not exists STG_ACCOUNT_NOT_VALID as
            select
                t1.account_num, t2.passport_num, t2.phone,
                t2.last_name || ' ' || t2.first_name
                || ' ' || t2.patronymic as fio, t1.valid_to
            from DWH_DIM_ACCOUNTS t1
            left join DWH_DIM_CLIENTS t2 on t1.client = t2.client_id
            where ? > t1.valid_to;
    ''', [date])
    cursor.execute('''
        create view if not exists STG_CARDS_NOT_VALID as
            select
                t1.card_num, t2.passport_num, t2.fio, t2.phone
            from DWH_DIM_CARDS t1
            inner join STG_ACCOUNT_NOT_VALID t2 on t1.account_num =
            t2.account_num;
    ''')
    cursor.execute('''
        create view if not exists STG_PASSPORT_FRAUD_VIEW as
            select
               t2.fio, t2.passport_num as passport, t2.phone,
               t1.trans_date as event_dt
            from DWH_FACT_TRANSACTIONS t1
            inner join STG_CARDS_NOT_VALID t2 on t1.card_num = t2.card_num;
    ''')
    cursor.execute('''
        insert into REP_FRAUD (
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) select
           event_dt,
           passport,
           fio,
           phone,
           'account',
           datetime('now')
        from STG_PASSPORT_FRAUD_VIEW
        where (event_dt, passport) in (
            select min(event_dt), passport
            from STG_PASSPORT_FRAUD_VIEW
            group by passport
            );
    ''')
    cursor.execute('drop table if exists STG_ACCOUNT_NOT_VALID')
    cursor.execute('drop view if exists STG_CARDS_NOT_VALID')
    cursor.execute('drop view if exists STG_PASSPORT_FRAUD_VIEW')
    con.commit()

def city(con):
    print(bcolors.OKGREEN + 'Searching city fraud...' + bcolors.ENDC)
    cursor = con.cursor()
    cursor.execute('''
        create view if not exists STG_TRANSACTIONS_DIFFERENT_CITY as
            select
                card_num,
                count(distinct t2.terminal_city) as cnt_city
            from DWH_FACT_TRANSACTIONS t1
            left join DWH_DIM_TERMINALS_HIST t2 on t1.terminal = t2.terminal_id
            group by t1.card_num
            having count(distinct t2.terminal_city) > 1
    ''')
    # helpers.showData(con, 'STG_TRANSACTIONS_DIFFERENT_CITY')
    cursor.execute('''
        create view if not exists STG_TRANSACTIONS_PER_CITY as
            select
                t1.card_num,
                t1.trans_date,
                t3.terminal_city
            from DWH_FACT_TRANSACTIONS t1
            inner join STG_TRANSACTIONS_DIFFERENT_CITY t2 on t1.card_num = t2.card_num
            left join DWH_DIM_TERMINALS_HIST t3 on t1.terminal = t3.terminal_id
    ''')
    # helpers.showData(con, 'STG_TRANSACTIONS_PER_CITY')
    cursor.execute('''
        create view if not exists STG_TRANSACTIONS_LAST_AND_CURRENT_CITY as
            select
                card_num,
                terminal_city as current_city,
                trans_date as current_city_date,
                lag(terminal_city) over(partition by card_num order by
                trans_date) as last_city,
                lag(trans_date) over(partition by card_num order by
                trans_date) as last_city_date
            from STG_TRANSACTIONS_PER_CITY
    ''')
    # helpers.showData(con, 'STG_TRANSACTIONS_LAST_AND_CURRENT_CITY')
    cursor.execute('''
        create view if not exists STG_FRAUD_TRANSACTIONS_CITY as
            select
                card_num,
                min(current_city_date) as trans_date
            from STG_TRANSACTIONS_LAST_AND_CURRENT_CITY
            where cast((JulianDay(current_city_date) - JulianDay(last_city_date)) *
            24 * 60 as integer) < 60
            and last_city != current_city
            group by card_num
    ''')
    # helpers.showData(con, 'STG_FRAUD_TRANSACTIONS_CITY')
    cursor.execute('''
        create view if not exists STG_FRAUD_TRANSACTIONS_CITY_00 as
            select
                t1.trans_date as event_dt,
                t4.passport_num as passport,
                t4.last_name || ' ' || t4.first_name || ' ' || t4.patronymic as fio,
                t4.phone
            from STG_FRAUD_TRANSACTIONS_CITY t1
            left join DWH_DIM_CARDS t2 on t1.card_num = t2.card_num
            left join DWH_DIM_ACCOUNTS t3 on t2.account_num = t3.account_num
            left join DWH_DIM_CLIENTS t4 on t3.client = t4.client_id
    ''')
    # helpers.showData(con, 'STG_FRAUD_TRANSACTIONS_CITY_00')
    cursor.execute('''
        insert into REP_FRAUD (
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) select
           event_dt,
           passport,
           fio,
           phone,
           'city',
           datetime('now')
        from STG_FRAUD_TRANSACTIONS_CITY_00;
    ''')
    cursor.execute('drop view if exists STG_TRANSACTIONS_DIFFERENT_CITY')
    cursor.execute('drop view if exists STG_TRANSACTIONS_LAST_AND_CURRENT_CITY')
    cursor.execute('drop view if exists STG_TRANSACTIONS_PER_CITY')
    cursor.execute('drop view if exists STG_FRAUD_TRANSACTIONS_CITY')
    cursor.execute('drop view if exists STG_FRAUD_TRANSACTIONS_CITY_00')
    con.commit()

def sum_guessing(con):
    print(bcolors.OKGREEN + 'Searching sum fraud...' + bcolors.ENDC)
    cursor = con.cursor()
    cursor.execute('''
        create view if not exists STG_OPER_SUCCESS as
            select
                card_num,
                trans_date,
                oper_result,
                oper_type,
                LAG(oper_result, 1) over (partition by card_num order by
                trans_date) as previous_result_1,
                LAG(oper_result, 2) over (partition by card_num order by
                trans_date) as previous_result_2,
                LAG(oper_result, 3) over (partition by card_num order by
                trans_date) as previous_result_3,
                LAG(trans_date, 3) over (partition by card_num order by
                trans_date) as previous_date_3
            from DWH_FACT_TRANSACTIONS
    ''')
    # helpers.showData(con, 'STG_OPER_SUCCESS')
    cursor.execute('''
        create view if not exists STG_SUM_GUESSING as
            select
                card_num,
                trans_date,
                oper_type,
                oper_result,
                previous_result_1,
                previous_result_2,
                previous_result_3,
                cast((JulianDay(trans_date) - JulianDay(previous_date_3)) *
                24 * 60 as integer) as diff_minutes_3
            from STG_OPER_SUCCESS
            where
                oper_result = 'SUCCESS'
                and previous_result_1 = 'REJECT'
                and previous_result_2 = 'REJECT'
                and previous_result_3 = 'REJECT'
                and cast((JulianDay(trans_date) - JulianDay(previous_date_3)) *
                24 * 60 as integer) < 20
    ''')
    # helpers.showData(con, 'STG_SUM_GUESSING')
    cursor.execute('''
        create view if not exists STG_SUM_GUESSING_00 as
            select
                t1.trans_date as event_dt,
                t4.passport_num as passport,
                t4.last_name || ' ' || t4.first_name || ' ' || t4.patronymic as fio,
                t4.phone
            from STG_SUM_GUESSING t1
            left join DWH_DIM_CARDS t2 on t1.card_num = t2.card_num
            left join DWH_DIM_ACCOUNTS t3 on t2.account_num = t3.account_num
            left join DWH_DIM_CLIENTS t4 on t3.client = t4.client_id
    ''')
    cursor.execute('''
        insert into REP_FRAUD (
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) select
           event_dt,
           passport,
           fio,
           phone,
           'sum',
           datetime('now')
        from STG_SUM_GUESSING_00;
    ''')
    cursor.execute('drop view if exists STG_OPER_SUCCESS')
    cursor.execute('drop view if exists STG_SUM_GUESSING')
    cursor.execute('drop view if exists STG_SUM_GUESSING_00')
    con.commit()
