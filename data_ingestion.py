from load_to_db2 import load_to_db2
from ibm_cos import rename_file_with_prefix
from ibm_dataengine import get_refresh_token
from transform_cos_file import transform_cos_file


def main(path_to_source_file, table_name):
    get_refresh_token()

    path_to_target_file = f"{path_to_source_file.split('.')[0].upper()}_STAGE"

    transform_cos_file(
        sql_file_name="clean_transform_source_data.sql",
        sql_query_params={
            "<source-bucket-name>": "rushi-grp-source-files",
            "<stage-bucket-name>": "rushi-grp-stage",
            "<path-to-source-file>": path_to_source_file,
            "<path-to-target-file>": path_to_target_file,
        },
    )

    new_file_name = f"{path_to_target_file}.CSV"

    rename_file_with_prefix(
        source_bucket="rushi-grp-stage",
        prefix_to_filter=f"{path_to_target_file}/part",
        destination_bucket="rushi-grp-stage",
        destination_object_key=new_file_name,
    )

    load_to_db2(
        bucket_name="rushi-grp-stage",
        file_name=new_file_name,
        table_name=table_name,
    )


if __name__ == "__main__":
    main(path_to_source_file="ACTIVE.CSV", table_name="ACTIVE")
    main(path_to_source_file="CLOSED.CSV", table_name="CLOSED")
