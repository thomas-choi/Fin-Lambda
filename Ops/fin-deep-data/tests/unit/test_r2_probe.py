"""r2_probe -- the manual R2 access check. Hermetic: a fake S3 client."""

import io
from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

import r2_probe

NOW = datetime(2026, 10, 14, 9, 30, tzinfo=timezone.utc)
KEY = "_probe/r2_probe-20261014T093000Z.txt"


def forbidden(op):
    return ClientError({"Error": {"Code": "403", "Message": "Forbidden"},
                        "ResponseMetadata": {"HTTPStatusCode": 403}}, op)


class FakeS3:
    """In-memory bucket store; buckets in ``denied`` 403 on every call."""

    def __init__(self, denied=()):
        self.objects, self.denied, self.calls = {}, set(denied), []

    def _check(self, op, bucket):
        self.calls.append((op, bucket))
        if bucket in self.denied:
            raise forbidden(op)

    def list_objects_v2(self, Bucket, MaxKeys):
        self._check("ListObjectsV2", Bucket)
        return {}

    def put_object(self, Bucket, Key, Body, ContentType):
        self._check("PutObject", Bucket)
        self.objects[(Bucket, Key)] = Body

    def head_object(self, Bucket, Key):
        self._check("HeadObject", Bucket)
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}

    def get_object(self, Bucket, Key):
        self._check("GetObject", Bucket)
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def delete_object(self, Bucket, Key):
        self._check("DeleteObject", Bucket)
        self.objects.pop((Bucket, Key), None)


def test_all_steps_pass_and_probe_object_is_removed():
    s3 = FakeS3()
    assert r2_probe.probe(s3, ["fin-data", "fin-modal-data"], now=NOW) is True
    assert s3.objects == {}
    assert [op for op, b in s3.calls if b == "fin-data"] == [
        "ListObjectsV2", "PutObject", "HeadObject", "GetObject", "DeleteObject"]


def test_keep_leaves_the_probe_object():
    s3 = FakeS3()
    assert r2_probe.probe(s3, ["fin-modal-data"], keep=True, now=NOW) is True
    assert list(s3.objects) == [("fin-modal-data", KEY)]


def test_one_denied_bucket_fails_the_run_but_the_other_is_still_probed(capsys):
    s3 = FakeS3(denied={"fin-data"})
    assert r2_probe.probe(s3, ["fin-data", "fin-modal-data"], now=NOW) is False
    assert ("DeleteObject", "fin-modal-data") in s3.calls
    # nothing was put on the denied bucket, so no delete is attempted there
    assert ("DeleteObject", "fin-data") not in s3.calls
    assert "FAIL  403 403 Forbidden" in capsys.readouterr().out


def test_no_buckets_is_not_a_pass():
    assert r2_probe.probe(FakeS3(), [], now=NOW) is False


def test_mask_never_shows_the_middle():
    key = "0123456789abcdef0123456789abcdef"
    assert r2_probe.mask(key) == "0123…cdef"
    assert r2_probe.mask("short") == "****"


@pytest.mark.parametrize("content, message", [
    ('R2_ENDPOINT="x"\nNEWS_R2_BUCKET="b"\n', "missing R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY"),
    ('R2_ENDPOINT="x"\nR2_ACCESS_KEY_ID="k"\nR2_SECRET_ACCESS_KEY="s"\n', "neither"),
])
def test_main_exits_before_any_client_when_env_is_incomplete(tmp_path, content, message):
    env = tmp_path / ".env"
    env.write_text(content)
    with pytest.raises(SystemExit) as exc:
        r2_probe.main([str(env)])
    assert message in str(exc.value)
