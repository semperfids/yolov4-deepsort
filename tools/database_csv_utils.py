## Libraries ###########################################################
import pandas as pd
import os, argparse, json, time
from sqlalchemy import create_engine, text

## Paths and global variables ##########################################
cols_rename = { # Conversion if requered in csv formats
    "store_id": "id_tienda", "camera_id": "id_camara", 
    "index": "indice_conteo", "tracking_id": "indice_seguimiento",
    "timestamp": "tiempo", "confidence": "confianza",
    "source_video": "video_origen"#, "index": "id_registro"
}

# SQL command to drop and create a table.
create_table_query = '''DROP TABLE if exists public.{0};
    CREATE TABLE public.{0}(
    id_tienda varchar NOT NULL,
    id_camara integer NOT NULL,
    "indice_conteo" integer NOT NULL,
    indice_seguimiento integer NOT NULL,
    frame integer NOT NULL,
    "tiempo" date NOT NULL,
    confianza double precision NOT NULL,
    x_min integer NOT NULL,
    y_min integer NOT NULL,
    x_max integer NOT NULL,
    y_max integer NOT NULL,
    x_trans integer NOT NULL,
    y_trans integer NOT NULL,
    video_origen varchar NULL,
    id_registro serial NOT NULL,
    CONSTRAINT {0}_pk PRIMARY KEY (id_registro)); '''

## Class/Functions #####################################################
class database:
    ''' Constructor '''
    # Credentials must be in json file
    def __init__(self, credentials, database = "postgres", **engine_kwargs):
        with open(credentials, "r") as fj: cred = json.load(fj)
        engine_text='postgresql://{}:{}@{}/{}'.format(cred["user_server"], 
            cred["password"], 
            cred["host_server"], 
            database)
        self.engine = create_engine(engine_text, **engine_kwargs)
    
    ''' User functions '''
    def get_dbs_info(self):
        return self.runQuery("SELECT * FROM pg_database")

    def get_current_db(self):
        return self.runQuery("SELECT current_database()")

    def get_tables(self):
        return self.runQuery("SELECT * FROM pg_catalog.pg_tables")

    def runQuery(self, sql):
        result = self.engine.connect().execution_options(isolation_level="AUTOCOMMIT").execute((text(sql)))
        try:
            return pd.DataFrame(result.fetchall(), columns=result.keys())
        except:
            return result.fetchall()

    def to_sql(self, df, sql_table, if_exists = "append"):
        # Data preparation
        df = df.rename(columns = cols_rename)
        if "x_trans" not in df.columns: df["x_trans"] = 10000 # Illegal values
        if "y_trans" not in df.columns: df["y_trans"] = 10000 # Illegal values
        print("["+time.strftime("%x %I:%M:%S %p")+"][INFO]: Starting registers update...")
        df.to_sql(sql_table, self.engine, if_exists=if_exists, index=False) # Add new register if doesn't exist
        print("["+time.strftime("%x %I:%M:%S %p")+"][INFO]: Registers updated successufully.")

    def sqltable_setup(self, sql_table_name): # Clean database
        sql = create_table_query.format(sql_table_name)
        self.engine.connect().execution_options(isolation_level="AUTOCOMMIT").execute((text(sql)))
        print("["+time.strftime("%x %I:%M:%S %p")+"][INFO] Table {} created successfully!".format(sql_table_name))

    ''' Hidden functions '''
    def __existRegister(self, reg, sql_table):
        key_fields = ["id_tienda", "id_camara", "indice_conteo", "indice_seguimiento", "frame", "tiempo", "video_origen"]
        reg = reg[key_fields]
        sql_query = ["{}={}".format(*x) for x in zip(reg.columns, reg.values[0]) if type(x[1]) is not str]
        sql_query += ["{}='{}'".format(*x) for x in zip(reg.columns, reg.values[0]) if type(x[1]) is str]
        sql_query = "SELECT True FROM {} WHERE {} LIMIT 1;".format(sql_table, " and ".join(sql_query))
        sql_response = self.runQuery(sql_query).values
        if len(sql_response) == 0: return False
        else: return True

## Main ################################################################
def main(args):
    sql_connection = database(args.credentials, args.database)
    # if args.sql_table is not None: sql_connection.sqltable_setup(args.sql_table) # Clean db

    print("[INFO]: All databases:")
    print(sql_connection.get_dbs_info())
    current_db = sql_connection.get_current_db()["current_database"].values
    print("[INFO]: Current database:",current_db)
    tables = sql_connection.get_tables()
    print(tables[tables["tableowner"] == current_db[0]])

    if args.sql_query is not None:
        print("\n[INFO] User query:")
        query_cons = sql_connection.runQuery(args.sql_query)
        print(query_cons)
        print(query_cons.info())
    
    if args.csv is not None:
        if args.sql_table is None:
            print("[ERROR]: -sqlt is needed to upload information.")
        else:
            df = pd.read_csv(args.csv, header=0)
            sql_connection.to_sql(df, args.sql_table)

def parse_args():
    '''parse args'''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, 
        description='SQL conection, query and data creation.',
        epilog='''Example:\n\t python %(prog)s # Conection with database and simples consult''')
    parser.add_argument('-c', '--credentials', required = True, type=str, help='SQL credentials in json file')
    parser.add_argument('-db', '--database', default = "postgres", type=str, help='SQL data base (default: %(default)s)')
    parser.add_argument('-sqlq', '--sql_query', default = None, type=str, help='SQL specific query (default: %(default)s)')
    parser.add_argument('-sqlt', '--sql_table', default = None, type=str, help='SQL table to query/upload (default: %(default)s)')
    parser.add_argument('-csv', '--csv', default = None, type=str, help='CSV to upload on database (default: %(default)s)')
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())
