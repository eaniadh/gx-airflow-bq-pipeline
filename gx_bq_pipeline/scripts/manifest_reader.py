# scripts/manifest_reader.py
import json
import os
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional
from dotenv import load_dotenv

from google.cloud import storage
from google.oauth2 import service_account

# ---------------- ENV / PATHS ----------------
current_script_dir = Path(__file__).resolve().parent
dotenv_path = current_script_dir.parent.parent / "keys" / ".env"
load_dotenv(dotenv_path=dotenv_path)


@dataclass
class ManifestEntry:
    uri: str
    name: str
    size: Optional[int] = None
    etag: Optional[str] = None
    md5: Optional[str] = None
    crc32c: Optional[str] = None


def _make_storage_client(project: Optional[str] = None) -> storage.Client:
    key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if key and os.path.exists(key):
        creds = service_account.Credentials.from_service_account_file(
            key, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        if project:
            creds = creds.with_quota_project(project)
        return storage.Client(project=project, credentials=creds)
    return storage.Client(project=project)


def _split_gs_uri(gs_uri: str):
    # "gs://bucket/path/to/obj" -> ("bucket", "path/to/obj")
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Not a gs:// URI: {gs_uri}")
    tail = gs_uri[5:]
    bucket, _, name = tail.partition("/")
    return bucket, name


def partition_has_success(bucket: str, partition_prefix: str, client: storage.Client) -> bool:
    # partition_prefix like "date=2025-10-21/"
    return client.bucket(bucket).blob(partition_prefix + "_SUCCESS").exists()


def load_manifest(bucket: str, partition_prefix: str, client: storage.Client) -> dict:
    # reads JSON at date=YYYY-MM-DD/_MANIFEST.json
    path = partition_prefix + "_MANIFEST.json"
    blob = client.bucket(bucket).blob(path)
    if not blob.exists():
        raise FileNotFoundError(f"Missing manifest: gs://{bucket}/{path}")
    txt = blob.download_as_text()
    return json.loads(txt)


def validate_files(manifest: dict, client: storage.Client, strict: bool = True) -> List[ManifestEntry]:
    entries = []
    for f in manifest.get("files", []):
        uri = f["uri"]
        bkt, name = _split_gs_uri(uri)
        blob = client.bucket(bkt).blob(name)
        if not blob.exists():
            msg = f"Listed file missing: {uri}"
            if strict:
                raise FileNotFoundError(msg)
            else:
                print(f"[WARN] {msg}", file=sys.stderr)
                continue
        # Optional integrity checks
        if f.get("size") is not None and blob.size is not None and int(f["size"]) != int(blob.size):
            msg = f"Size mismatch: {uri} manifest={f['size']} actual={blob.size}"
            if strict:
                raise ValueError(msg)
            else:
                print(f"[WARN] {msg}", file=sys.stderr)
        if f.get("etag") and blob.etag and f["etag"] != blob.etag and strict:
            raise ValueError(f"ETag mismatch: {uri}")

        entries.append(ManifestEntry(
            uri=uri, name=f.get("name") or name, size=blob.size, etag=blob.etag, md5=blob.md5_hash, crc32c=blob.crc32c
        ))
    # deterministic order
    entries.sort(key=lambda e: e.name)
    return entries


def get_partition_files(partition_uri: str, project: Optional[str] = None, strict: bool = True) -> List[str]:
    """
    partition_uri: e.g. 'gs://gx_data_ready_to_process/date=2025-10-21/'
    returns a sorted list of gs:// file URIs from the manifest.
    """
    bucket, prefix = _split_gs_uri(partition_uri)
    client = _make_storage_client(project)

    if not partition_has_success(bucket, prefix, client):
        raise RuntimeError(f"Partition not published (no _SUCCESS): {partition_uri}")

    manifest = load_manifest(bucket, prefix, client)
    files = validate_files(manifest, client, strict=strict)
    return [f.uri for f in files]


# --- tiny CLI ---
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/manifest_reader.py gs://<bucket>/date=YYYY-MM-DD/ [--project <id>] [--loose]")
        sys.exit(2)
    partition = sys.argv[1]
    project = None
    strict = True
    if "--project" in sys.argv:
        i = sys.argv.index("--project")
        project = sys.argv[i + 1]
    if "--loose" in sys.argv:
        strict = False
    uris = get_partition_files(partition, project=project, strict=strict)
    for u in uris:
        print(u)
