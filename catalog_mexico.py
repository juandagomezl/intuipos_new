import json
import pyodbc
import pandas as pd
import psycopg2
from datetime import datetime
import requests


class Intuipos():

    def __init__(self):

        self.conn = psycopg2.connect(
            dbname='data-prod-co',
            host='data-prod.cem7ltlisydy.us-east-1.redshift.amazonaws.com',
            port='5439',
            user='awsuser',
            password='c5ZM27wulOXhM95nM8Ce')

        self.esquema = 'marketing'
        self.dbname = 'data-prod-co'

        self.cnxn_str = ("Driver={ODBC Driver 17 for SQL Server};"
                         "Server=209.105.239.28,6706;"
                         "Database=DBINTUIPOS49400;"
                         "UID=userBIintuipos49400;"
                         "PWD=biPower2030$;")


def lambda_handler():

    intu = Intuipos()
    conn_server = pyodbc.connect(intu.cnxn_str)
    color_rojo = 15335424
    cursor = intu.conn.cursor()

    """
  Tabla t_group
  """

    try:

        cursor.execute("truncate table intuipos_mexico.t_group")
        intu.conn.commit()

        query = f"""select *
              from dbo.T_Group """

        df_new = pd.read_sql(query, conn_server)

        df_new.columns = map(str.lower, df_new.columns)

        columns_numeric = ['tigroupid',
                           'ipuntodeventaid', 'tireplicationstatusid']

        for column in columns_numeric:

            df_new[column] = df_new[column].replace('NaN', '0')
            df_new[column] = df_new[column].replace("'nan'", '0')
            df_new[column] = df_new[column].replace('nan', '0')
            df_new[column] = df_new[column].replace("", '0')
            df_new[column] = df_new[column].replace(' ', '0')
            df_new[column].fillna(0, inplace=True)
            df_new[column] = df_new[column].astype(int)

        print('Inicio del Insert')

        insert = "INSERT INTO intuipos_mexico.t_group ("
        insert_query_temp = ''

        for column in df_new.columns:

            insert_query_temp = insert_query_temp + column + ', '

        insert_query = insert + insert_query_temp[:-2] + ") VALUES"

        insert_query_t = insert_query
        numero_commit = 0

        for registro in df_new.iterrows():

            insert_query_temp = ''

            for column in df_new.columns:

                dato = registro[1][column]

                dato = str(dato).replace("'", "")
                dato = "'" + dato + "'"
                insert_query_temp = insert_query_temp + dato + ', '

            insert_query_t = insert_query_t + \
                "(" + insert_query_temp[:-2] + "),"

            numero_commit = numero_commit + 1
            if numero_commit == 1500:

                insert_query_t = insert_query_t[:-1] + ";"

                cursor.execute(insert_query_t)
                intu.conn.commit()
                numero_commit = 0
                insert_query_t = insert_query

        if numero_commit != 0:
            insert_query_t = insert_query_t[:-1] + ";"

            cursor.execute(insert_query_t)
            intu.conn.commit()

        print('Fin tabla t_group')

    except Exception as error:
        url = "https://discord.com/api/webhooks/953845930426249307/-tAgfpXqMPuOm8AsM5dI_ZkZAxCshDa4oJdcPhmkGjpE7KB_ngPnaP5UDwEvbrB82Ndt"
        # lista = '\n'.join(map(str, error))
        data = {}

        data["embeds"] = [{
                          "color": color_rojo,
                          "fields": [{
                              "name": 'Lambda Function -> Intuipos_tablas_catalogo_mx:',
                              "value": f""" Descripción: {error}
                                            Tabla: t_group
                                            https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/intuipos_tablas_catalogo_mx?newFunction=true&tab=code"""
                          }]
                          }]

        response = requests.post(url, json=data)

    """
  Tabla t_item
  """

    try:

        cursor.execute("truncate table intuipos_mexico.t_item")
        intu.conn.commit()

        query = f"""select *
              from dbo.T_Item """

        df_new = pd.read_sql(query, conn_server)

        df_new.columns = map(str.lower, df_new.columns)

        columns_numeric = ['iitemid', 'tigroupid', 'iminreorder_deprecated', 'tiitemroleid',
                           'timeasureunitid_recipe', 'timeasureunitid_inventory', 'bvendersobrestock',
                           'tireplicationstatusid', 'ipuntodeventaid', 'tisubgroupid', 'bautomaticprod']

        for column in columns_numeric:

            df_new[column] = df_new[column].replace('NaN', '0')
            df_new[column] = df_new[column].replace("'nan'", '0')
            df_new[column] = df_new[column].replace('nan', '0')
            df_new[column] = df_new[column].replace("", '0')
            df_new[column] = df_new[column].replace(' ', '0')
            df_new[column].fillna(0, inplace=True)
            df_new[column] = df_new[column].astype(int)

        df_new['dqtystandard'].fillna(0, inplace=True)
        df_new['dqtystandard'] = df_new['dqtystandard'].astype(float)

        print('Inicio tabla t_item')

        insert = "INSERT INTO intuipos_mexico.t_item ("
        insert_query_temp = ''

        for column in df_new.columns:

            insert_query_temp = insert_query_temp + column + ', '

        insert_query = insert + insert_query_temp[:-2] + ") VALUES"

        insert_query_t = insert_query
        numero_commit = 0

        for registro in df_new.iterrows():

            insert_query_temp = ''

            for column in df_new.columns:

                dato = registro[1][column]

                dato = str(dato).replace("'", "")
                dato = "'" + dato + "'"
                insert_query_temp = insert_query_temp + dato + ', '

            insert_query_t = insert_query_t + \
                "(" + insert_query_temp[:-2] + "),"

            numero_commit = numero_commit + 1
            if numero_commit == 1500:

                insert_query_t = insert_query_t[:-1] + ";"

                cursor.execute(insert_query_t)
                intu.conn.commit()
                numero_commit = 0
                insert_query_t = insert_query

        if numero_commit != 0:
            insert_query_t = insert_query_t[:-1] + ";"

            cursor.execute(insert_query_t)
            intu.conn.commit()

        print('Fin tabla t_item')

    except Exception as error:
        url = "https://discord.com/api/webhooks/953845930426249307/-tAgfpXqMPuOm8AsM5dI_ZkZAxCshDa4oJdcPhmkGjpE7KB_ngPnaP5UDwEvbrB82Ndt"
        # lista = '\n'.join(map(str, error))
        data = {}

        data["embeds"] = [{
                          "color": color_rojo,
                          "fields": [{
                              "name": 'Lambda Function -> Intuipos_tablas_catalogo_mx:',
                              "value": f""" Descripción: {error}
                                            Tabla: t_item
                                            https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/intuipos_tablas_catalogo_mx?newFunction=true&tab=code"""
                          }]
                          }]

        response = requests.post(url, json=data)

    """
  Tabla t_measureunit
  """

    try:

        cursor.execute("truncate table intuipos_mexico.t_measureunit")
        intu.conn.commit()

        query = f"""select *
              from dbo.T_MeasureUnit """

        df_new = pd.read_sql(query, conn_server)

        df_new.columns = map(str.lower, df_new.columns)

        columns_numeric = ['timeasureunitid',
                           'tireplicationstatusid', 'ipuntodeventaid']

        for column in columns_numeric:

            df_new[column] = df_new[column].replace('NaN', '0')
            df_new[column] = df_new[column].replace("'nan'", '0')
            df_new[column] = df_new[column].replace('nan', '0')
            df_new[column] = df_new[column].replace("", '0')
            df_new[column] = df_new[column].replace(' ', '0')
            df_new[column].fillna(0, inplace=True)
            df_new[column] = df_new[column].astype(int)

        # df_new['dqtystandard'].fillna(0, inplace=True)
        # df_new['dqtystandard'] = df_new['dqtystandard'].astype(float)

        print('Inicio tabla t_measureunit')

        insert = "INSERT INTO intuipos_mexico.t_measureunit ("
        insert_query_temp = ''

        for column in df_new.columns:

            insert_query_temp = insert_query_temp + column + ', '

        insert_query = insert + insert_query_temp[:-2] + ") VALUES"

        insert_query_t = insert_query
        numero_commit = 0

        for registro in df_new.iterrows():

            insert_query_temp = ''

            for column in df_new.columns:

                dato = registro[1][column]

                dato = str(dato).replace("'", "")
                dato = "'" + dato + "'"
                insert_query_temp = insert_query_temp + dato + ', '

            insert_query_t = insert_query_t + \
                "(" + insert_query_temp[:-2] + "),"

            numero_commit = numero_commit + 1
            if numero_commit == 1500:

                insert_query_t = insert_query_t[:-1] + ";"

                cursor.execute(insert_query_t)
                intu.conn.commit()
                numero_commit = 0
                insert_query_t = insert_query

        if numero_commit != 0:
            insert_query_t = insert_query_t[:-1] + ";"

            cursor.execute(insert_query_t)
            intu.conn.commit()

        print('Fin tabla t_measureunit')

    except Exception as error:
        url = "https://discord.com/api/webhooks/953845930426249307/-tAgfpXqMPuOm8AsM5dI_ZkZAxCshDa4oJdcPhmkGjpE7KB_ngPnaP5UDwEvbrB82Ndt"
        # lista = '\n'.join(map(str, error))
        data = {}

        data["embeds"] = [{
                          "color": color_rojo,
                          "fields": [{
                              "name": 'Lambda Function -> Intuipos_tablas_catalogo_mx:',
                              "value": f""" Descripción: {error}
                                            Tabla: t_measureunit
                                            https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/intuipos_tablas_catalogo_mx?newFunction=true&tab=code"""
                          }]
                          }]

        response = requests.post(url, json=data)

    """
  Tabla t_punto_venta
  """

    try:

        cursor.execute("truncate table intuipos_mexico.t_punto_venta")
        intu.conn.commit()

        query = f"""select *
              from dbo.T_PuntoDeVenta """

        df_new = pd.read_sql(query, conn_server)

        df_new.columns = map(str.lower, df_new.columns)

        columns_numeric = ['ipuntodeventaid', 'vphonenumber', 'icompanyid', 'icityid',
                           'bauditapprovalcode', 'baudit4lastdigits', 'bdomiciliospresent',
                           'tireplicationstatusid', 'bauditnumpersonas', 'bactive',
                           'iwarehouseiddefault', 'bislocaldefault', 'benableshareresources',
                           'ipuntodeventaid_parent']

        for column in columns_numeric:

            df_new[column] = df_new[column].replace('NaN', '0')
            df_new[column] = df_new[column].replace("'nan'", '0')
            df_new[column] = df_new[column].replace('nan', '0')
            df_new[column] = df_new[column].replace("", '0')
            df_new[column] = df_new[column].replace(' ', '0')
            df_new[column].fillna(0, inplace=True)
            df_new[column] = df_new[column].astype(int)

        df_new['mtipvalue'].fillna(0, inplace=True)
        df_new['mtipvalue'] = df_new['mtipvalue'].astype(float)

        print('Inicio tabla t_punto_venta')

        insert = "INSERT INTO intuipos_mexico.t_punto_venta ("
        insert_query_temp = ''

        for column in df_new.columns:

            insert_query_temp = insert_query_temp + column + ', '

        insert_query = insert + insert_query_temp[:-2] + ") VALUES"

        insert_query_t = insert_query
        numero_commit = 0

        for registro in df_new.iterrows():

            insert_query_temp = ''

            for column in df_new.columns:

                dato = registro[1][column]

                dato = str(dato).replace("'", "")
                dato = "'" + dato + "'"
                insert_query_temp = insert_query_temp + dato + ', '

            insert_query_t = insert_query_t + \
                "(" + insert_query_temp[:-2] + "),"

            numero_commit = numero_commit + 1
            if numero_commit == 1500:

                insert_query_t = insert_query_t[:-1] + ";"

                cursor.execute(insert_query_t)
                intu.conn.commit()
                numero_commit = 0
                insert_query_t = insert_query

        if numero_commit != 0:
            insert_query_t = insert_query_t[:-1] + ";"

            cursor.execute(insert_query_t)
            intu.conn.commit()

        print('Fin tabla t_punto_venta')

    except Exception as error:
        url = "https://discord.com/api/webhooks/953845930426249307/-tAgfpXqMPuOm8AsM5dI_ZkZAxCshDa4oJdcPhmkGjpE7KB_ngPnaP5UDwEvbrB82Ndt"
        # lista = '\n'.join(map(str, error))
        data = {}

        data["embeds"] = [{
                          "color": color_rojo,
                          "fields": [{
                              "name": 'Lambda Function -> Intuipos_tablas_catalogo_mx:',
                              "value": f""" Descripción: {error}
                                            Tabla: t_punto_venta
                                            https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/intuipos_tablas_catalogo_mx?newFunction=true&tab=code"""
                          }]
                          }]

        response = requests.post(url, json=data)

    """
  Tabla t_subgroup
  """

    try:

        cursor.execute("truncate table intuipos_mexico.t_subgroup")
        intu.conn.commit()

        query = f"""select *
              from dbo.T_SubGroup """

        df_new = pd.read_sql(query, conn_server)

        df_new.columns = map(str.lower, df_new.columns)

        columns_numeric = ['tisubgroupid', 'ipuntodeventaid', 'bactive', 'tireplicationstatusid',
                           'bisfnbcost']

        for column in columns_numeric:

            df_new[column] = df_new[column].replace('NaN', '0')
            df_new[column] = df_new[column].replace("'nan'", '0')
            df_new[column] = df_new[column].replace('nan', '0')
            df_new[column] = df_new[column].replace("", '0')
            df_new[column] = df_new[column].replace(' ', '0')
            df_new[column].fillna(0, inplace=True)
            df_new[column] = df_new[column].astype(int)

        # df_new['mtipvalue'].fillna(0, inplace=True)
        # df_new['mtipvalue'] = df_new['mtipvalue'].astype(float)

        print('Inicio tabla t_subgroup')

        insert = "INSERT INTO intuipos_mexico.t_subgroup ("
        insert_query_temp = ''

        for column in df_new.columns:

            insert_query_temp = insert_query_temp + column + ', '

        insert_query = insert + insert_query_temp[:-2] + ") VALUES"

        insert_query_t = insert_query
        numero_commit = 0

        for registro in df_new.iterrows():

            insert_query_temp = ''

            for column in df_new.columns:

                dato = registro[1][column]

                dato = str(dato).replace("'", "")
                dato = "'" + dato + "'"
                insert_query_temp = insert_query_temp + dato + ', '

            insert_query_t = insert_query_t + \
                "(" + insert_query_temp[:-2] + "),"

            numero_commit = numero_commit + 1
            if numero_commit == 1500:

                insert_query_t = insert_query_t[:-1] + ";"

                cursor.execute(insert_query_t)
                intu.conn.commit()
                numero_commit = 0
                insert_query_t = insert_query

        if numero_commit != 0:
            insert_query_t = insert_query_t[:-1] + ";"

            cursor.execute(insert_query_t)
            intu.conn.commit()

        print('Fin tabla t_subgroup')

    except Exception as error:
        url = "https://discord.com/api/webhooks/953845930426249307/-tAgfpXqMPuOm8AsM5dI_ZkZAxCshDa4oJdcPhmkGjpE7KB_ngPnaP5UDwEvbrB82Ndt"
        # lista = '\n'.join(map(str, error))
        data = {}

        data["embeds"] = [{
                          "color": color_rojo,
                          "fields": [{
                              "name": 'Lambda Function -> Intuipos_tablas_catalogo_mx:',
                              "value": f""" Descripción: {error}
                                            Tabla: t_subgroup
                                            https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/intuipos_tablas_catalogo_mx?newFunction=true&tab=code"""
                          }]
                          }]

        response = requests.post(url, json=data)

    """
  Tabla t_supplier
  """

    try:

        cursor.execute("truncate table intuipos_mexico.t_supplier")
        intu.conn.commit()

        query = f"""select *
              from dbo.T_Supplier """

        df_new = pd.read_sql(query, conn_server)

        df_new.columns = map(str.lower, df_new.columns)

        columns_numeric = ['isupplierid', 'ipuntodeventaid', 'icityid', 'bactive',
                           'tireplicationstatusid', 'iidentitytypeid', 'titiporeterentaid',
                           'titiporegimenid', 'ticlaseproveedorid', 'tipaymenttermid']

        for column in columns_numeric:

            df_new[column] = df_new[column].replace('NaN', '0')
            df_new[column] = df_new[column].replace("'nan'", '0')
            df_new[column] = df_new[column].replace('nan', '0')
            df_new[column] = df_new[column].replace("", '0')
            df_new[column] = df_new[column].replace(' ', '0')
            df_new[column].fillna(0, inplace=True)
            df_new[column] = df_new[column].astype(int)

        # df_new['mtipvalue'].fillna(0, inplace=True)
        # df_new['mtipvalue'] = df_new['mtipvalue'].astype(float)

        print('Inicio tabla t_supplier')

        insert = "INSERT INTO intuipos_mexico.t_supplier ("
        insert_query_temp = ''

        for column in df_new.columns:

            insert_query_temp = insert_query_temp + column + ', '

        insert_query = insert + insert_query_temp[:-2] + ") VALUES"

        insert_query_t = insert_query
        numero_commit = 0

        for registro in df_new.iterrows():

            insert_query_temp = ''

            for column in df_new.columns:

                dato = registro[1][column]

                dato = str(dato).replace("'", "")
                dato = "'" + dato + "'"
                insert_query_temp = insert_query_temp + dato + ', '

            insert_query_t = insert_query_t + \
                "(" + insert_query_temp[:-2] + "),"

            numero_commit = numero_commit + 1
            if numero_commit == 1500:

                insert_query_t = insert_query_t[:-1] + ";"

                cursor.execute(insert_query_t)
                intu.conn.commit()
                numero_commit = 0
                insert_query_t = insert_query

        if numero_commit != 0:
            insert_query_t = insert_query_t[:-1] + ";"

            cursor.execute(insert_query_t)
            intu.conn.commit()

        print('Fin tabla t_supplier')

    except Exception as error:
        url = "https://discord.com/api/webhooks/953845930426249307/-tAgfpXqMPuOm8AsM5dI_ZkZAxCshDa4oJdcPhmkGjpE7KB_ngPnaP5UDwEvbrB82Ndt"
        # lista = '\n'.join(map(str, error))
        data = {}

        data["embeds"] = [{
                          "color": color_rojo,
                          "fields": [{
                              "name": 'Lambda Function -> Intuipos_tablas_catalogo_mx:',
                              "value": f""" Descripción: {error}
                                            Tabla: t_supplier
                                            https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/intuipos_tablas_catalogo_mx?newFunction=true&tab=code"""
                          }]
                          }]

        response = requests.post(url, json=data)

    """
  Tabla t_transactiontype
  """

    try:

        cursor.execute("truncate table intuipos_mexico.t_transactiontype")
        intu.conn.commit()

        query = f"""select *
              from dbo.T_TransactionType """

        df_new = pd.read_sql(query, conn_server)

        df_new.columns = map(str.lower, df_new.columns)

        columns_numeric = ['titransactiontypeid',
                           'bactive', 'tiparenttransactiontypeid']

        for column in columns_numeric:

            df_new[column] = df_new[column].replace('NaN', '0')
            df_new[column] = df_new[column].replace("'nan'", '0')
            df_new[column] = df_new[column].replace('nan', '0')
            df_new[column] = df_new[column].replace("", '0')
            df_new[column] = df_new[column].replace(' ', '0')
            df_new[column].fillna(0, inplace=True)
            df_new[column] = df_new[column].astype(int)

        # df_new['mtipvalue'].fillna(0, inplace=True)
        # df_new['mtipvalue'] = df_new['mtipvalue'].astype(float)

        print('Inicio tabla t_transactiontype')

        insert = "INSERT INTO intuipos_mexico.t_transactiontype ("
        insert_query_temp = ''

        for column in df_new.columns:

            insert_query_temp = insert_query_temp + column + ', '

        insert_query = insert + insert_query_temp[:-2] + ") VALUES"

        insert_query_t = insert_query
        numero_commit = 0

        for registro in df_new.iterrows():

            insert_query_temp = ''

            for column in df_new.columns:

                dato = registro[1][column]

                dato = str(dato).replace("'", "")
                dato = "'" + dato + "'"
                insert_query_temp = insert_query_temp + dato + ', '

            insert_query_t = insert_query_t + \
                "(" + insert_query_temp[:-2] + "),"

            numero_commit = numero_commit + 1
            if numero_commit == 1500:

                insert_query_t = insert_query_t[:-1] + ";"

                cursor.execute(insert_query_t)
                intu.conn.commit()
                numero_commit = 0
                insert_query_t = insert_query

        if numero_commit != 0:
            insert_query_t = insert_query_t[:-1] + ";"

            cursor.execute(insert_query_t)
            intu.conn.commit()

        print('Fin tabla t_transactiontype')

    except Exception as error:
        url = "https://discord.com/api/webhooks/953845930426249307/-tAgfpXqMPuOm8AsM5dI_ZkZAxCshDa4oJdcPhmkGjpE7KB_ngPnaP5UDwEvbrB82Ndt"
        # lista = '\n'.join(map(str, error))
        data = {}

        data["embeds"] = [{
                          "color": color_rojo,
                          "fields": [{
                              "name": 'Lambda Function -> Intuipos_tablas_catalogo_mx:',
                              "value": f""" Descripción: {error}
                                            Tabla: t_transactiontype
                                            https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/intuipos_tablas_catalogo_mx?newFunction=true&tab=code"""
                          }]
                          }]

        response = requests.post(url, json=data)

    """
  Tabla t_trxstatus
  """

    try:

        cursor.execute("truncate table intuipos_mexico.t_trxstatus")
        intu.conn.commit()

        query = f"""select *
              from dbo.T_TrxStatus """

        df_new = pd.read_sql(query, conn_server)

        df_new.columns = map(str.lower, df_new.columns)

        columns_numeric = ['titrxstatusid']

        for column in columns_numeric:

            df_new[column] = df_new[column].replace('NaN', '0')
            df_new[column] = df_new[column].replace("'nan'", '0')
            df_new[column] = df_new[column].replace('nan', '0')
            df_new[column] = df_new[column].replace("", '0')
            df_new[column] = df_new[column].replace(' ', '0')
            df_new[column].fillna(0, inplace=True)
            df_new[column] = df_new[column].astype(int)

        # df_new['mtipvalue'].fillna(0, inplace=True)
        # df_new['mtipvalue'] = df_new['mtipvalue'].astype(float)

        print('Inicio tabla t_trxstatus')

        insert = "INSERT INTO intuipos_mexico.t_trxstatus ("
        insert_query_temp = ''

        for column in df_new.columns:

            insert_query_temp = insert_query_temp + column + ', '

        insert_query = insert + insert_query_temp[:-2] + ") VALUES"

        insert_query_t = insert_query
        numero_commit = 0

        for registro in df_new.iterrows():

            insert_query_temp = ''

            for column in df_new.columns:

                dato = registro[1][column]

                dato = str(dato).replace("'", "")
                dato = "'" + dato + "'"
                insert_query_temp = insert_query_temp + dato + ', '

            insert_query_t = insert_query_t + \
                "(" + insert_query_temp[:-2] + "),"

            numero_commit = numero_commit + 1
            if numero_commit == 1500:

                insert_query_t = insert_query_t[:-1] + ";"

                cursor.execute(insert_query_t)
                intu.conn.commit()
                numero_commit = 0
                insert_query_t = insert_query

        if numero_commit != 0:
            insert_query_t = insert_query_t[:-1] + ";"

            cursor.execute(insert_query_t)
            intu.conn.commit()

        print('Fin tabla t_trxstatus')

    except Exception as error:
        url = "https://discord.com/api/webhooks/953845930426249307/-tAgfpXqMPuOm8AsM5dI_ZkZAxCshDa4oJdcPhmkGjpE7KB_ngPnaP5UDwEvbrB82Ndt"
        # lista = '\n'.join(map(str, error))
        data = {}

        data["embeds"] = [{
                          "color": color_rojo,
                          "fields": [{
                              "name": 'Lambda Function -> Intuipos_tablas_catalogo_mx:',
                              "value": f""" Descripción: {error}
                                            Tabla: t_trxstatus
                                            https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/intuipos_tablas_catalogo_mx?newFunction=true&tab=code"""
                          }]
                          }]

        response = requests.post(url, json=data)

    """
  Tabla t_warehouse
  """

    try:

        cursor.execute("truncate table intuipos_mexico.t_warehouse")
        intu.conn.commit()

        query = f"""select *
              from dbo.T_WareHouse """

        df_new = pd.read_sql(query, conn_server)

        df_new.columns = map(str.lower, df_new.columns)

        columns_numeric = ['ipuntodeventaid', 'iwarehouseid', 'tireplicationstatusid',
                           'bisvirtual', 'tistatusid']

        for column in columns_numeric:

            df_new[column] = df_new[column].replace('NaN', '0')
            df_new[column] = df_new[column].replace("'nan'", '0')
            df_new[column] = df_new[column].replace('nan', '0')
            df_new[column] = df_new[column].replace("", '0')
            df_new[column] = df_new[column].replace(' ', '0')
            df_new[column].fillna(0, inplace=True)
            df_new[column] = df_new[column].astype(int)

        # df_new['mtipvalue'].fillna(0, inplace=True)
        # df_new['mtipvalue'] = df_new['mtipvalue'].astype(float)

        print('Inicio tabla t_warehouse')

        insert = "INSERT INTO intuipos_mexico.t_warehouse ("
        insert_query_temp = ''

        for column in df_new.columns:

            insert_query_temp = insert_query_temp + column + ', '

        insert_query = insert + insert_query_temp[:-2] + ") VALUES"

        insert_query_t = insert_query
        numero_commit = 0

        for registro in df_new.iterrows():

            insert_query_temp = ''

            for column in df_new.columns:

                dato = registro[1][column]

                dato = str(dato).replace("'", "")
                dato = "'" + dato + "'"
                insert_query_temp = insert_query_temp + dato + ', '

            insert_query_t = insert_query_t + \
                "(" + insert_query_temp[:-2] + "),"

            numero_commit = numero_commit + 1
            if numero_commit == 1500:

                insert_query_t = insert_query_t[:-1] + ";"

                cursor.execute(insert_query_t)
                intu.conn.commit()
                numero_commit = 0
                insert_query_t = insert_query

        if numero_commit != 0:
            insert_query_t = insert_query_t[:-1] + ";"

            cursor.execute(insert_query_t)
            intu.conn.commit()

        print('Fin tabla t_warehouse')

    except Exception as error:
        url = "https://discord.com/api/webhooks/953845930426249307/-tAgfpXqMPuOm8AsM5dI_ZkZAxCshDa4oJdcPhmkGjpE7KB_ngPnaP5UDwEvbrB82Ndt"
        # lista = '\n'.join(map(str, error))
        data = {}

        data["embeds"] = [{
                          "color": color_rojo,
                          "fields": [{
                              "name": 'Lambda Function -> Intuipos_tablas_catalogo_mx:',
                              "value": f""" Descripción: {error}
                                            Tabla: t_warehouse
                                            https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/intuipos_tablas_catalogo_mx?newFunction=true&tab=code"""
                          }]
                          }]

        response = requests.post(url, json=data)

        """
  Tabla t_item_item
  """

    try:

        cursor.execute("truncate table intuipos_mexico.t_itemxitem")
        intu.conn.commit()

        query = f"""select *
              from dbo.T_ItemXItem """

        df_new = pd.read_sql(query, conn_server)

        df_new.columns = map(str.lower, df_new.columns)

        columns_numeric = ['iitemid', 'isubitemid', 'tirelationtypeid', 'doptionprice', 'dquantity', 'ipuntodeventaid',
                           'tireplicationstatusid', 'tiitemxitemstatusid', 'iitemxitemid', 'iindex']

        for column in columns_numeric:

            df_new[column] = df_new[column].replace('NaN', '0')
            df_new[column] = df_new[column].replace("'nan'", '0')
            df_new[column] = df_new[column].replace('nan', '0')
            df_new[column] = df_new[column].replace("", '0')
            df_new[column] = df_new[column].replace(' ', '0')
            df_new[column].fillna(0, inplace=True)
            df_new[column] = df_new[column].astype(float)

        print('Inicio del t_itemxitem')

        insert = "INSERT INTO intuipos_mexico.t_itemxitem ("
        insert_query_temp = ''

        for column in df_new.columns:

            insert_query_temp = insert_query_temp + column + ', '

        insert_query = insert + insert_query_temp[:-2] + ") VALUES"

        insert_query_t = insert_query
        numero_commit = 0

        for registro in df_new.iterrows():

            insert_query_temp = ''

            for column in df_new.columns:

                dato = registro[1][column]

                dato = str(dato).replace("'", "")
                dato = "'" + dato + "'"
                insert_query_temp = insert_query_temp + dato + ', '

            insert_query_t = insert_query_t + \
                "(" + insert_query_temp[:-2] + "),"

            numero_commit = numero_commit + 1
            if numero_commit == 1500:

                insert_query_t = insert_query_t[:-1] + ";"

                cursor.execute(insert_query_t)
                intu.conn.commit()
                numero_commit = 0
                insert_query_t = insert_query

        if numero_commit != 0:
            insert_query_t = insert_query_t[:-1] + ";"

            cursor.execute(insert_query_t)
            intu.conn.commit()

        print('Fin tabla t_itemxitem')

    except Exception as error:
        url = "https://discord.com/api/webhooks/953845930426249307/-tAgfpXqMPuOm8AsM5dI_ZkZAxCshDa4oJdcPhmkGjpE7KB_ngPnaP5UDwEvbrB82Ndt"
        # lista = '\n'.join(map(str, error))
        data = {}

        data["embeds"] = [{
                          "color": color_rojo,
                          "fields": [{
                              "name": 'Lambda Function -> Intuipos_tablas_catalogo_mx:',
                              "value": f""" Descripción: {error}
                                            Tabla: t_itemxitem
                                            https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/intuipos_tablas_catalogo_mx?newFunction=true&tab=code"""
                          }]
                          }]

        response = requests.post(url, json=data)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


if __name__ == '__main__':
    lambda_handler()
