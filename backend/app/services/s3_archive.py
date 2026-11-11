import asyncio
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError


class S3ArchiveService:
    """Archives raw webhook payloads to S3 (or LocalStack in local mode)."""

    def __init__(
        self,
        bucket_name: str,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        client: Any | None = None,
    ) -> None:
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self._client = client or boto3.client(
            "s3",
            region_name=region,
            endpoint_url=endpoint_url,
        )

    @staticmethod
    def build_key(source: str, event_id: str, received_at: datetime) -> str:
        utc_ts = (
            received_at.replace(tzinfo=UTC)
            if received_at.tzinfo is None
            else received_at.astimezone(UTC)
        )
        return f"raw/{source}/{utc_ts:%Y/%m/%d}/{event_id}.json"

    def _ensure_bucket_sync(self) -> None:
        try:
            self._client.head_bucket(Bucket=self.bucket_name)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in {"404", "NoSuchBucket"}:
                self._client.create_bucket(Bucket=self.bucket_name)
            else:
                raise

    async def ensure_bucket(self) -> None:
        if self.endpoint_url is None:
            return
        await asyncio.to_thread(self._ensure_bucket_sync)

    async def archive_raw_event(
        self,
        source: str,
        event_id: str,
        received_at: datetime,
        raw_body: bytes,
    ) -> str:
        key = self.build_key(source, event_id, received_at)
        await asyncio.to_thread(
            self._client.put_object,
            Bucket=self.bucket_name,
            Key=key,
            Body=raw_body,
            ContentType="application/json",
        )
        return key

