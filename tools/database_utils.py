import psycopg2
from psycopg2 import Error

table_name = 'people_raw'
create_table_query = f'''CREATE TABLE public.{table_name}(
    store_id varchar NOT NULL,
    camera_id integer NOT NULL,
    "index" integer NOT NULL,
    tracking_id integer NOT NULL,
    frame integer NOT NULL,
    "timestamp" date NOT NULL,
    confidence double precision NOT NULL,
    x_min integer NOT NULL,
    y_min integer NOT NULL,
    x_max integer NOT NULL,
    y_max integer NOT NULL,
    source_video varchar NULL,
    record_id serial NOT NULL,
    CONSTRAINT {table_name}_pk PRIMARY KEY (record_id)
); '''
        



def upload_csv_to_database(ht, prt, db, us, passwd, filename, create_db=True):
    try:
        connection = psycopg2.connect(user = us,
                                    password = passwd,
                                    host = ht,
                                    port = prt,
                                    database = db)

        cursor = connection.cursor()
        

        if(create_db):
            cursor.execute(create_table_query)
        
        with open(filename, 'r') as f:
            # Notice that we don't need the `csv` module.
            next(f) # Skip the header row.
            cur.copy_from(f, table_name, sep=',')
        connection.commit()
        print("Files uploaded successfully")

    except (Exception, psycopg2.DatabaseError) as error :
        print ("Error while uploading files", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")



