from time import sleep
from ibm_dataengine import run_sql, get_jobid_status


def transform_cos_file(
    sql_file_name: str,
    sql_query_params: "dict[str, str]" = dict(),
):
    job_info = run_sql(sql_file_name, sql_query_params)

    if (not job_info) or len(job_info["job_id"]) == 0:
        print("error in recieving job-id")
        exit()

    while True:
        job_status = get_jobid_status(jobid=job_info["job_id"])
        if job_status in ["queued", "running", "stopping"]:
            sleep(15)
        else:
            break

    if job_status not in ["completed"]:
        print(f"sql-job is not completed. STATUS: {job_status}")
        exit()


if __name__ == "__main__":
    transform_cos_file(
        sql_file_name="clean_transform_source_data.sql",
        sql_query_params={
            "<source-bucket-name>": "rushi-grp-source-files",
            "<stage-bucket-name>": "rushi-grp-stage",
            "<path-to-source-file>": "closed.csv",
        },
    )
