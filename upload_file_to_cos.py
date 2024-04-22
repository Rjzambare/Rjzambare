import ibm_boto3
from ibm_botocore.client import Config, ClientError
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
COS_INSTANCE_CRN = os.getenv("COS_INSTANCE_CRN")

# IBM COS credentials
cos_credentials = {
    "apikey": API_KEY,
    "iam_service_endpoint": "https://iam.cloud.ibm.com/identity/token",
    "cos_endpoint": "https://s3.us-south.cloud-object-storage.appdomain.cloud",
    "resource_instance_id": COS_INSTANCE_CRN,
}

# COS client configuration
cos_config = Config(signature_version="oauth", region_name="us-south")


def multi_part_upload(bucket_name, file_name, object_name):
    # # Create resource
    try:
        cos = ibm_boto3.resource(
            "s3",
            ibm_api_key_id=cos_credentials["apikey"],
            ibm_service_instance_id=cos_credentials["resource_instance_id"],
            ibm_auth_endpoint=cos_credentials["iam_service_endpoint"],
            config=cos_config,
            endpoint_url=cos_credentials["cos_endpoint"],
        )

        file_path = os.path.join(os.getcwd(), "public", file_name)
        print("file_path", file_path)
        print(
            "Starting file transfer for {0} to bucket: {1}\n".format(
                object_name, bucket_name
            )
        )
        # set 5 MB chunks
        part_size = 1024 * 1024 * 5

        # set threadhold to 15 MB
        file_threshold = 1024 * 1024 * 15

        # set the transfer threshold and chunk size
        transfer_config = ibm_boto3.s3.transfer.TransferConfig(
            multipart_threshold=file_threshold, multipart_chunksize=part_size
        )

        # the upload_fileobj method will automatically execute a multi-part upload
        # in 5 MB chunks for all files over 15 MB
        with open(file_path, "rb") as file_data:
            cos.Object(bucket_name, object_name).upload_fileobj(
                Fileobj=file_data, Config=transfer_config
            )

        print("Transfer for {0} Complete!\n".format(object_name))
    except ClientError as be:
        print("CLIENT ERROR: {0}\n".format(be))
    except Exception as e:
        print("Unable to complete multi-part upload: {0}".format(e))


def upload_file(bucket_name, file_name, object_name):
    try:
        # Create COS client
        cos_client = ibm_boto3.client(
            "s3",
            ibm_api_key_id=cos_credentials["apikey"],
            ibm_service_instance_id=cos_credentials["resource_instance_id"],
            ibm_auth_endpoint=cos_credentials["iam_service_endpoint"],
            config=cos_config,
            endpoint_url=cos_credentials["cos_endpoint"],
        )
        # Bucket name and file to be uploaded
        file_path = os.path.join(os.getcwd(), "public", file_name)
        print("file_path", file_path)
        # Upload file to COS
        cos_client.upload_file(file_path, bucket_name, object_name)
        print(
            f"File '{file_path}' successfully uploaded to '{bucket_name}/{object_name}'"
        )
    except Exception as e:
        print(f"Error uploading file: {str(e)}")


if __name__ == "__main__":
    upload_file("rushi-grp-source-files", "CLOSED.CSV", "CLOSED.CSV")
    upload_file("rushi-grp-source-files", "ACTIVE.CSV", "ACTIVE.CSV")
    # multi_part_upload("shantanu-grp-source-files", "CLOSED.CSV", "CLOSED.CSV")
    # multi_part_upload("shantanu-grp-source-files", "ACTIVE.CSV", "ACTIVE.CSV")
