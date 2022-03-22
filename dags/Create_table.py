# import pandas
import psycopg2


def check_connection():

    try:
        conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow", port='5432')
        cursor = conn.cursor()
        print("Hello")
        add_data = 'create table docker_Assignment_table as select dag_id, execution_date from dag_run order by execution_date; '

        cursor.execute(add_data)
        print("bye")
        conn.commit()

        print("Successfully added the data...")
        conn.close()

    except:
        print("Error in connection")
    finally:
        print("No issues")