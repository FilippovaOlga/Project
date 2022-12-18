import os
from py_scripts import helpers, loaddb1


def init(cursor):
    cursor.execute('''
        create table if not exists DWH_DIM_TERMINALS_HIST(
            id integer primary key autoincrement,
            terminal_id varchar(128),
            terminal_type varchar(128),
            terminal_city varchar(128),
            terminal_address varchar(128),
            effective_from date default current_timestamp,
            effective_to date default (datetime('2999-12-31 23:59:59')),
            deleted_flg default 0
        );
    ''')
    cursor.execute('''
        create view if not exists STG_TERMINALS_VIEW as
            select
                terminal_id, terminal_type, terminal_city, terminal_address
            from DWH_DIM_TERMINALS_HIST
            where current_timestamp between effective_from and effective_to;
    ''')
def createTableNewRows(cursor):
    cursor.execute('''
        create table if not exists STG_TERMINALS_NEW as
            select
                t1.*
            from STG_TERMINALS as t1
            left join STG_TERMINALS_VIEW as t2
            on t1.terminal_id == t2.terminal_id
            where t2.terminal_id is null;
    ''')
def createTableDeleteRows(cursor):
    cursor.execute('''
        create table if not exists STG_TERMINALS_DELETE as
            select
                t1.*
            from
            STG_TERMINALS_VIEW as t1
            left join STG_TERMINALS as t2
            on t1.terminal_id == t2.terminal_id
            where t2.terminal_id is null;
    ''')

def createTableChangedRows(cursor):
    cursor.execute('''
        create table if not exists STG_TERMINALS_CHANGED as
            select
                t1.*
            from STG_TERMINALS as t1
            inner join STG_TERMINALS_VIEW as t2
            on t1.terminal_id == t2.terminal_id
            and
                (
                    t1.terminal_address <> t2.terminal_address or
                    t1.terminal_type <> t2.terminal_type or
                    t1.terminal_city <> t2.terminal_city
                );
    ''')
def updateAutoHist(con):
    cursor = con.cursor()
    # deleted records
    cursor.execute('''
        update DWH_DIM_TERMINALS_HIST
            set effective_to = datetime('now', '-1 second'),
            deleted_flg = 1
            where terminal_id in (select terminal_id from STG_TERMINALS_DELETE)
            and effective_to = datetime('2999-12-31 23:59:59');
    ''')
    # new records
    cursor.execute('''
        insert into DWH_DIM_TERMINALS_HIST(
            terminal_id,
            terminal_city,
            terminal_type,
            terminal_address
        ) select
            terminal_id,
            terminal_city,
            terminal_type,
            terminal_address
        from STG_TERMINALS_NEW;
    ''')
    # modified records
    cursor.execute('''
        update DWH_DIM_TERMINALS_HIST
            set effective_to = datetime('now', '-1 second')
            where terminal_id in (select terminal_id from STG_TERMINALS_CHANGED)
            and effective_to = datetime('2999-12-31 23:59:59');
    ''')
    cursor.execute('''
        insert into DWH_DIM_TERMINALS_HIST(
            terminal_id,
            terminal_city,
            terminal_type,
            terminal_address
        ) select
            terminal_id,
            terminal_city,
            terminal_type,
            terminal_address
        from STG_TERMINALS_CHANGED;
    ''')
    con.commit()
def deleteTmpTables(con):
    cursor = con.cursor()
    cursor.execute('drop table if exists STG_TERMINALS')
    cursor.execute('drop table if exists STG_TERMINALS_NEW')
    cursor.execute('drop table if exists STG_TERMINALS_DELETE')
    cursor.execute('drop table if exists STG_TERMINALS_CHANGED')
    cursor.execute('drop view if exists STG_TERMINALS_VIEW')
    con.commit()
def incremental_load(con, date):
    source_file = "terminals_" + date + ".xlsx"
    loaddb1.excel_load(con, source_file, 'STG_TERMINALS')
    cursor = con.cursor()
    init(cursor)
    createTableNewRows(cursor)
    createTableDeleteRows(cursor)
    createTableChangedRows(cursor)
    updateAutoHist(con)
    # utils.showData(con, 'STG_TERMINALS')
    # utils.showData(con, 'STG_TERMINALS_NEW')
    # utils.showData(con, 'STG_TERMINALS_DELETE')
    # utils.showData(con, 'STG_TERMINALS_CHANGED')
    # utils.showData(con, 'DWH_DIM_TERMINALS_HIST')
    deleteTmpTables(con)
    backup_file = os.path.join("archive", "terminals_" + date + ".xlsx.backup")
    os.rename(source_file, backup_file)
