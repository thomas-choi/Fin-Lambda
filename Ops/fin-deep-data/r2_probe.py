"""Probe R2 access for fin-deep-data's credentials -- a manual tool, not a Lambda.

For each bucket (``UPSTREAM_R2_BUCKET``, ``NEWS_R2_BUCKET``): list, put a small
probe object, head it, get it back, then delete it. Reads ``R2_ENDPOINT`` /
``R2_ACCESS_KEY_ID`` / ``R2_SECRET_ACCESS_KEY`` and the two bucket names from
this folder's ``.env``. Never prints a secret: only the endpoint and the first
and last four characters of the access key id, to match against the token in
the Cloudflare dashboard.

The probe **writes** ``_probe/r2_probe-{UTC stamp}.txt`` to both live buckets
and deletes it again; ``--keep`` leaves it in place. Exit status 0 only when
every step passed on every bucket. Written for the 2026-10-14 403 incident
(``doc/OPERATIONS.md`` §8.14). Not packaged: ``serverless.yml`` ships ``!**``.

    cd Ops/fin-deep-data
    ../../venv-py313/bin/python r2_probe.py [path/to/.env] [--keep]
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import ClientError
from dotenv import dotenv_values

DEFAULT_ENV = Path(__file__).resolve().parent / ".env"
CREDENTIAL_KEYS = ("R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY")
BUCKET_KEYS = ("UPSTREAM_R2_BUCKET", "NEWS_R2_BUCKET")
PROBE_PREFIX = "_probe/"


def mask(value):
    """First and last four characters only -- enough to identify a key id."""
    return f"{value[:4]}…{value[-4:]}" if len(value) > 8 else "****"


def step(name, fn):
    """Run one S3 call; print OK or the HTTP status and R2 error code."""
    try:
        result = fn()
        print(f"  {name:<10} OK")
        return True, result
    except ClientError as e:
        err = e.response.get("Error", {})
        status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        print(f"  {name:<10} FAIL  {status} {err.get('Code')} {err.get('Message', '')}")
        return False, None


def probe_bucket(client, bucket, key, body, keep=False):
    """list / put / head / get / delete on one bucket. True when all passed."""
    print(f"\n[{bucket}]  key={key}")
    ok_list, _ = step("list", lambda: client.list_objects_v2(Bucket=bucket, MaxKeys=1))
    ok_put, _ = step("put", lambda: client.put_object(
        Bucket=bucket, Key=key, Body=body, ContentType="text/plain"))
    ok_head, _ = step("head", lambda: client.head_object(Bucket=bucket, Key=key))
    ok_get, resp = step("get", lambda: client.get_object(Bucket=bucket, Key=key))
    if ok_get:
        ok_get = resp["Body"].read() == body
        print(f"  {'roundtrip':<10} {'OK' if ok_get else 'MISMATCH'}")
    ok_del = True
    if ok_put and not keep:
        ok_del, _ = step("delete", lambda: client.delete_object(Bucket=bucket, Key=key))
    return all((ok_list, ok_put, ok_head, ok_get, ok_del))


def probe(client, buckets, keep=False, now=None):
    """Probe every bucket with one shared key. True when every bucket passed."""
    stamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    key = f"{PROBE_PREFIX}r2_probe-{stamp}.txt"
    body = f"r2 probe {stamp}".encode()
    results = [probe_bucket(client, b, key, body, keep) for b in buckets]
    return bool(results) and all(results)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    paths = [a for a in argv if not a.startswith("--")]
    keep = "--keep" in argv
    env_path = Path(paths[0]) if paths else DEFAULT_ENV
    env = dotenv_values(env_path)

    missing = [k for k in CREDENTIAL_KEYS if not env.get(k)]
    buckets = [env[k] for k in BUCKET_KEYS if env.get(k)]
    if missing:
        sys.exit(f"{env_path}: missing {', '.join(missing)}")
    if not buckets:
        sys.exit(f"{env_path}: neither {' nor '.join(BUCKET_KEYS)} is set")

    print(f".env:          {env_path}")
    print(f"endpoint:      {env['R2_ENDPOINT']}")
    print(f"access key id: {mask(env['R2_ACCESS_KEY_ID'])}  (match against the dashboard)")

    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=env["R2_ENDPOINT"],
        aws_access_key_id=env["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    ok = probe(client, buckets, keep)
    print("\nRESULT:", "all checks passed" if ok else "FAILED -- see above")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
