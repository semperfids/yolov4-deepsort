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
        print("["+time.strftime("%x %I:%M:%S %p")+"][INFO]: Starting registers update...")
        df.to_sql(sql_table, self.engine, if_exists=if_exists, index=False) # Add new register if doesn't exist
        print("["+time.strftime("%x %I:%M:%S %p")+"][INFO]: Registers updated successufully.")

    def sqltable_setup(self, sql_table_name): # Clean database
        sql = create_table_query.format(sql_table_name)
        self.engine.connect().execution_options(isolation_level="AUTOCOMMIT").execute((text(sql)))
        print("["+time.strftime("%x %I:%M:%S %p")+"][INFO] Table {} created successfully!".format(sql_table_name))

    def db_scheme_info(self):
        print("["+time.strftime("%x %I:%M:%S %p")+"][INFO]: All databases:")
        print(self.get_dbs_info())
        current_db = self.get_current_db()["current_database"].values
        print("["+time.strftime("%x %I:%M:%S %p")+"][INFO]: Current database:", current_db)
        tables = self.get_tables()
        print(tables[tables["tableowner"] == current_db[0]])

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

## Hidden functions ####################################################
def readFileList(file_directory, ext = 'jpg'): # Read a valid json-files inside folder / independenly file
    files_list = []
    if os.path.isdir(file_directory): # Return files with 'json' extension
        for root_path, _, files_name in os.walk(file_directory):
            files_list += [os.path.join(root_path, element) for element in files_name if element.split(".")[-1].lower() == ext.lower()]
    elif file_directory.split('.')[-1] == ext: files_list = [file_directory] # Return file inside of list
    return files_list

def to_table(sql_connection, sql_table, dir_csv, if_exists = "append", isfilter = False):
    if os.path.isdir(dir_csv):
        file_list = readFileList(dir_csv, "csv")
        if isfilter: file_list = [x for x in file_list if "_output.csv" in x] # Special filter!!
        print("["+time.strftime("%x %I:%M:%S %p")+"][INFO]: Total files to be uploaded to the database:", len(file_list))
        i = 1
        for csv_file in file_list:
            print("\n["+time.strftime("%x %I:%M:%S %p")+"][INFO]: {} uploading ({} of {}) ...".format(csv_file, i, len(file_list)))
            df = pd.read_csv(csv_file, header=0).drop(columns = ["tienda"])
            if if_exists == "append": sql_connection.to_sql(df, sql_table)
            elif if_exists == "replace":
                if i == 1: df_total = pd.DataFrame(columns = df.columns)
                df_total = df_total.append(df)
            i += 1
        if if_exists == "replace": sql_connection.to_sql(df_total, sql_table, if_exists = if_exists)
    elif os.path.isfile(dir_csv) and os.path.splitext(dir_csv)[1] == ".csv":
        df = pd.read_csv(dir_csv, header=0)
        sql_connection.to_sql(df, sql_table, if_exists = if_exists)
    else:
        print("["+time.strftime("%x %I:%M:%S %p")+"][ERROR]: File or directory doesn't exist. Please check") 

## Main ################################################################
def main(args):
    sql_connection = database(args.credentials, args.database)
    # if args.sql_table is not None: sql_connection.sqltable_setup(args.sql_table) # Clean db

    if args.sql_query is not None:
        print("\n["+time.strftime("%x %I:%M:%S %p")+"][INFO] User query:")
        query_cons = sql_connection.runQuery(args.sql_query)
        print(query_cons)
        print(query_cons.shape)
    
    if args.directory_csv is not None:
        if args.sql_table is not None: 
            to_table(sql_connection, args.sql_table, args.directory_csv, "append", isfilter=True)
        else:
            print("["+time.strftime("%x %I:%M:%S %p")+"][ERROR]: -sqlt is needed to upload information.")
    else:
        sql_connection.db_scheme_info()

def parse_args():
    '''parse args'''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, 
        description='SQL conection, query and data creation.',
        epilog='''Example:\n\t python %(prog)s # Conection with database and simples consult''')
    parser.add_argument('-c', '--credentials', required = True, type=str, help='SQL credentials in json file')
    parser.add_argument('-db', '--database', default = "postgres", type=str, help='SQL data base (default: %(default)s)')
    parser.add_argument('-sqlt', '--sql_table', default = None, type=str, help='SQL table to query/upload (default: %(default)s)')
    parser.add_argument('-sqlq', '--sql_query', default = None, type=str, help='SQL specific query (default: %(default)s)')
    parser.add_argument('-dcsv', '--directory_csv', default = None, type=str, help='CSV or directory with CSV to upload on database (default: %(default)s)')
    return parser.parse_args()

if __name__ == "__main__":
    main(parse_args())
