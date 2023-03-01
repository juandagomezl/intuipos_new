import json
import pyodbc
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import requests


def con_redshift():

    return psycopg2.connect(
        dbname='data-prod-co',
        host='data-prod.cem7ltlisydy.us-east-1.redshift.amazonaws.com',
        port='5439',
        user='awsuser',
        password='c5ZM27wulOXhM95nM8Ce')


def con_sql_server():

    return pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                          "Server=209.105.239.28,6706;"
                          "Database=DBINTUIPOS49400;"
                          "UID=userBIintuipos49400;"
                          "PWD=biPower2030$;")


def drop_tables_stg():
    with con_redshift() as conn:
        with conn.cursor() as cursor:
            cursor.execute("drop table intuipos_mexico.t_transaction__stg ")

            cursor.execute(
                "drop table intuipos_mexico.t_transactiondetail__stg ")

            cursor.execute(
                "drop table intuipos_mexico.view_trxdetails__stg ")

        conn.commit()


def insert_data(df, table_name):

    with con_redshift() as conn:
        with conn.cursor() as cursor:

            insert = f"INSERT INTO {table_name} ("
            insert_query_temp = ''

            for column in df.columns:

                insert_query_temp = insert_query_temp + column + ', '

            insert_query = insert + insert_query_temp[:-2] + ") VALUES"
            insert_query_t = insert_query
            numero_commit = 0

            for registro in df.iterrows():

                insert_query_temp = ''

                for column in df.columns:

                    dato = registro[1][column]

                    dato = str(dato).replace("'", "")
                    dato = "'" + dato + "'"
                    insert_query_temp = insert_query_temp + dato + ', '

                insert_query_t = insert_query_t + \
                    "(" + insert_query_temp[:-2] + "),"

                numero_commit = numero_commit + 1
                if numero_commit == 3000:
                    # print(1)
                    insert_query_t = insert_query_t[:-1] + ";"

                    cursor.execute(insert_query_t)
                    conn.commit()
                    numero_commit = 0
                    insert_query_t = insert_query

            if numero_commit != 0:
                insert_query_t = insert_query_t[:-1] + ";"

                cursor.execute(insert_query_t)
                conn.commit()


def t_transaction():
    print("T_TRANSACTION")
    with con_redshift() as conn:

        query = """SELECT max(dtlastupdate)::datetime
                    FROM intuipos_mexico.t_transaction"""

        fecha_max = str(pd.read_sql(query, conn)['max'][0])
        # fecha_max = str(fecha_max['max'][0]

    print(fecha_max)

    with con_sql_server() as conn_server:
        query = f"""select *
                    from dbo.T_Transaction vt
                    where dtLastUpdate > '{fecha_max}' """

        df_transaction = pd.read_sql(query, conn_server)

    df_transaction.columns = map(str.lower, df_transaction.columns)

    columns_numeric = ['bitransactionid', 'titransactiontypeid', 'tistatusid',
                       'tideliverytypeid', 'titableid', 'biemployeeid', 'bidocumentid', 'ipax',
                       'biclientid', 'iwarehouseid', 'tireplicationstatusid',
                       'ipuntodeventaid', 'ipuntodeventaid_persona',
                       'isupplierid', 'ipuntodeventaid_supplier',
                       'ipuntodeventaid_client', 'ipuntodeventaid_terminal', 'ipuntodeventaid_furniture',
                       'isubpuntodeventaid', 'tiplatformid']

    for column in columns_numeric:

        df_transaction[column] = df_transaction[column].replace('NaN', '0')
        df_transaction[column] = df_transaction[column].replace("'nan'", '0')
        df_transaction[column] = df_transaction[column].replace('nan', '0')
        df_transaction[column] = df_transaction[column].replace("", '0')
        df_transaction[column] = df_transaction[column].replace(' ', '0')
        df_transaction[column].fillna(0, inplace=True)
        df_transaction[column] = df_transaction[column].astype(int)

    df_transaction['mtotalamount'].fillna(0, inplace=True)
    df_transaction['mtotalamount'] = df_transaction['mtotalamount'].astype(
        float)

    print('Inicio del Insert')

    print(df_transaction.columns)
    print(len(df_transaction))
    df_transaction['fecha_carga'] = datetime.now()

    create_table_stg = open(
        'source/create/create_t_transaction.sql', 'r').read()

    with con_redshift() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_stg)
        conn.commit()

    insert_data(df_transaction, "intuipos_mexico.t_transaction__stg")

    update = open('source/update/update_t_transaction.sql', 'r').read()
    insert = open('source/insert/insert_t_transaction.sql', 'r').read()

    with con_redshift() as conn:
        with conn.cursor() as cursor:
            cursor.execute(update)
            cursor.execute(insert)
        conn.commit()

    with con_redshift() as conn:
        with conn.cursor() as cursor:
            cursor.execute("drop table intuipos_mexico.t_transaction__stg ")
        conn.commit()


def t_transaction_details():
    print("T_TRANSACTIONDETAILS")
    with con_redshift() as conn:

        query = """SELECT max(dtlastupdate)::datetime
                    FROM intuipos_mexico.t_transactiondetail"""

        fecha_max = pd.read_sql(query, conn)['max'][0]

    create_table_stg = open(
        'source/create/create_t_transactiondetail.sql', 'r').read()

    with con_redshift() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_stg)
        conn.commit()

    with con_sql_server() as conn_server:
        query = f"""select *
                from dbo.T_TransactionDetail vt
                where dtLastUpdate > '{fecha_max}' """

        df_transaction = pd.read_sql(query, conn_server)

    df_transaction.columns = map(str.lower, df_transaction.columns)

    columns_numeric = ['bitransactiondetailid', 'iitemid', 'bitransactionid',
                       'bifatherid', 'iwarehouseid', 'tireplicationstatusid', 'timeasureunitid', 'ipuntodeventaid_item',
                       'iperiodo', 'tidetailstatusid', 'itaxid',
                       'itaxid2', 'isupplierid', 'ipuntodeventaid_supplier',
                       'ipuntodeventaid']

    for column in columns_numeric:

        df_transaction[column] = df_transaction[column].replace('NaN', '0')
        df_transaction[column] = df_transaction[column].replace(
            "'nan'", '0')
        df_transaction[column] = df_transaction[column].replace('nan', '0')
        df_transaction[column] = df_transaction[column].replace("", '0')
        df_transaction[column] = df_transaction[column].replace(' ', '0')
        df_transaction[column].fillna(0, inplace=True)
        df_transaction[column] = df_transaction[column].astype(int)

    column_float = ['mitemprice', 'fdiscountpercentage',
                    'dtaxpercentage', 'dquantity', 'dtaxpercentage2']

    for column in column_float:
        df_transaction[column].fillna(0, inplace=True)
        df_transaction[column] = df_transaction[column].astype(float)

    insert_data(df_transaction, "intuipos_mexico.t_transactiondetail__stg")

    print("INIT DELETE AND INSERT")
    update = open('source/update/update_t_transactiondetail.sql', 'r').read()
    insert = open('source/insert/insert_t_transactiondetail.sql', 'r').read()

    with con_redshift() as conn:
        with conn.cursor() as cursor:
            cursor.execute(update)
            cursor.execute(insert)
        conn.commit()

    with con_redshift() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "drop table intuipos_mexico.t_transactiondetail__stg ")
        conn.commit()


def view_trxdetails():

    print("VIEW_TRXDETAILS")
    with con_redshift() as conn:

        query = """SELECT max(t_tranx_dtlastupdate)::datetime t_tranx_dtlastupdate, 
                            max(t_trand_dtlastupdate)::datetime t_trand_dtlastupdate
                    FROM intuipos_mexico.view_trxdetails"""

        fechas = pd.read_sql(query, conn)

    t_tranx_dtlastupdate = fechas['t_tranx_dtlastupdate'][0]
    t_trand_dtlastupdate = fechas['t_trand_dtlastupdate'][0]
    # t_tranx_dtlastupdate = '2023-02-01'
    # t_trand_dtlastupdate = '2023-02-01'
    # print(t_tranx_dtlastupdate)
    # print(t_trand_dtlastupdate)

    create_table_stg = open('source/create/create_trx_details.sql', 'r').read()

    with con_redshift() as conn:
        with conn.cursor() as cursor:
            cursor.execute(create_table_stg)
        conn.commit()

    select_view_trxdetails = open('source/select/select _view_trxdetails.sql', 'r').read().format(
        t_tranx_dtlastupdate=t_tranx_dtlastupdate,
        t_trand_dtlastupdate=t_trand_dtlastupdate
    )

    print(select_view_trxdetails)

    with con_redshift() as conn:

        df_view = pd.read_sql(select_view_trxdetails, conn)

    insert_data(df_view, "intuipos_mexico.view_trxdetails__stg")

    update = open('source/update/update_view_trxdetails.sql', 'r').read()
    insert = open('source/insert/insert_view_trxdetails.sql', 'r').read()

    with con_redshift() as conn:
        with conn.cursor() as cursor:
            cursor.execute(update)
            cursor.execute(insert)
        conn.commit()

    with con_redshift() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "drop table intuipos_mexico.view_trxdetails__stg ")
        conn.commit()


if __name__ == '__main__':
    # dias_actualizacion = 35
    drop_tables_stg()

    t_transaction()
    t_transaction_details()
    view_trxdetails()
