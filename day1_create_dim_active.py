from ibm_cos import rename_file_with_prefix
from load_to_db2 import load_to_db2
from ibm_dataengine import get_refresh_token
from transform_cos_file import transform_cos_file

def main():
    get_refresh_token()
    transform_cos_file(
        sql_file_name="cdc_day1_create_dim_active.sql",
        sql_query_params={
            "<stage-bucket-name>": "rushi-grp-stage",
            "<transform-bucket-name>": "rushi-grp-transform",
        },
    )

    new_file_name = "DIM_ACTIVE.CSV"

    rename_file_with_prefix(
        source_bucket="rushi-grp-transform",
        prefix_to_filter="DIM_ACTIVE/part",
        destination_bucket="rushi-grp-transform",
        destination_object_key=new_file_name,
    )

    load_to_db2(
        bucket_name="rushi-grp-transform",
        file_name=new_file_name,
        table_name="DIM_ACTIVE",
    )


if __name__ == "__main__":
    main()
