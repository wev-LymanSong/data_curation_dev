from google.cloud import storage
import os

# 변수 설정
bucket_name = 'lm_study'
cre = '/Users/lymansong/Documents/GitHub/data_curation_dev/wev-dev-analytics-30e1a0019654.json'
source_file_name = '/Users/lymansong/Documents/GitHub/data_curation_dev/data/specs_queue/spec_template.md'
destination_blob_name = 'wi_report_index/spec_template.md'  # 파일의 경로를 Blob 이름에 지정

from google.cloud import storage

class GoogleCloudStorage(object):
    def __init__(self, bucket_name, cre):
        self.client = storage.Client.from_service_account_json(cre)
        self.bucket_name = bucket_name
        self.bucket = self.client.bucket(self.bucket_name)

    def upload_file(self, source_file_name, destination_blob_name):
        """Downloads a blob from the bucket."""
        blob = self.bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)

        print(
            "File {} uploaded to {}.".format(
                source_file_name, destination_blob_name
            )
        )

    def download_blob(self, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        blob = self.bucket.blob(source_blob_name)

        blob.download_to_filename(destination_file_name)

        print(
            "Blob {} downloaded to {}.".format(
                source_blob_name, destination_file_name
            )
        )

    def delete_blob(self, blob_name):
        """Deletes a blob from the bucket."""
        blob = self.bucket.blob(blob_name)
        blob.delete()

        print("Blob {} deleted.".format(blob_name))

gcs = GoogleCloudStorage(bucket_name, cre)
gcs.upload_file(source_file_name=source_file_name, destination_blob_name=destination_blob_name)
gcs.download_blob(source_blob_name=source_file_name, destination_file_name=destination_blob_name)
gcs.delete_blob(blob_name=destination_blob_name)