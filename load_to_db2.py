import jaydebeapi
import os
from dotenv import load_dotenv
from logger import table_info_logger

load_dotenv()
def load_to_db2(bucket_name, file_name, table_name):

    jdbc_url = (
        "jdbc:db2://54a2f15b-5c0f-46df-8954-7e38e612c2bd.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud:32733/bludb:user=bpp23930;password=IjZfKrb5eEpeowa5;sslConnection=true;"
    )
    jdbc_driver = "com.ibm.db2.jcc.DB2Driver"

    jdbc_user = os.getenv("DB2_USER")
    jdbc_password = os.getenv("DB2_PASSWORD")

    access_key_id = os.getenv("DB2_ACCESS_KEY_ID")
    secret_access_key = os.getenv("DB2_SECRET_ACCESS_KEY")

    connection = jaydebeapi.connect(jdbc_driver, jdbc_url)

    try:
        cursor = connection.cursor()

        load_command = (
            f"CALL SYSPROC.ADMIN_CMD('LOAD FROM \"S3::"
            f"s3.us-south.cloud-object-storage.appdomain.cloud::{access_key_id}::{secret_access_key}::{bucket_name}::{file_name}\""
            f" OF DEL REPLACE INTO {table_name}')"
        )
        table_info_logger("Load Command", load_command)
        cursor.execute(load_command)
        table_info_logger("Data Load", "Data loaded into Db2")

        select_query = f"SELECT * FROM {table_name} LIMIT 2"
        cursor.execute(select_query)
        result = cursor.fetchall()

        table_info_logger(table_name, len(result))

        return result

    except Exception as e:
        table_info_logger("Error", f"Error in loading data into {table_name}: {str(e)}")

    finally:
        connection.close()


if __name__ == "__main__":
    bucket_name = "rushi-grp-stage"
    file_name = "CLOSED_STAGE"
    table_name = "CLOSED"

    load_to_db2(bucket_name, file_name, table_name)
