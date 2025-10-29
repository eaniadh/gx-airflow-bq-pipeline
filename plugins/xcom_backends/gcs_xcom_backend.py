import json, gzip, uuid, pickle
from airflow.models.xcom import BaseXCom
from google.cloud import storage

BUCKET_NAME = "xcom-bucket"

class GCSXComBackend(BaseXCom):
    
    @staticmethod
    def _client():
        return storage.Client()
    
    @classmethod
    def serialize_value(cls, value):
        """If payload is small, store inline. Else upload to GCS."""
        try:
            payload = json.dumps(value)
            if len(payload) < 20000:
                return {"type": "inline", "data": payload}
        except Exception as e:
            pass
        
        blob_name = f"xcom/{uuid.uuid4().hex}.gz"
        bucket = cls._client().bucket(BUCKET_NAME)
        blob = bucket.blob(blob_name)
        raw = pickle.dumps(blob)
        blob.upload_from_string(gzip.compress(raw))
        
        return {"type": "gcs", "uri": f"gs://{BUCKET_NAME}/{blob_name}"}
    

    @classmethod
    def deserialize_value(cls, result):
        
        if isinstance(result, dict) and result.get("type") == "inline":
            return json.loads(result["data"])
        
        if isinstance(result, dict) and result.get("type") == "gcs":
            uri = result["uri"].replace("gs://", "")
            bucket_name, key, _ = uri.partition("/")
            blob = cls._client().bucket(bucket_name).blob(key)
            raw = gzip.decompress(blob.download_as_bytes())
            return pickle.loads(raw)
        
        return super().deserialize_value(result)