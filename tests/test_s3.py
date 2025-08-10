import boto3
from moto import mock_aws
from httpxthrottlecache import HttpxThrottleCache
import time
import pytest
import asyncio

@mock_aws
def test_s3_sync():
    url = "https://httpbin.org/cache/60"

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="mybucket")

    mgr = HttpxThrottleCache(httpx_params={"headers": {}}, cache_enabled=True, s3_bucket="mybucket", s3_client=s3)
    
    with mgr.http_client() as client:
        response1 = client.get(url=url)

        assert response1.status_code == 200, response1.status_code 
        
        time.sleep(1.5)

        response2 = client.get(url=url)

        assert response2.status_code == 200, response2.status_code 

        assert response1.headers["date"] == response2.headers["date"]

@pytest.mark.asyncio
async def test_s3_async():
    with mock_aws():
        url = "https://httpbin.org/cache/60"

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="mybucket")

        mgr = HttpxThrottleCache(httpx_params={"headers": {}}, cache_enabled=True, s3_bucket="mybucket", s3_client=s3)
        
        async with mgr.async_http_client() as client:
            response1 = await client.get(url=url)

            assert response1.status_code == 200, response1.status_code 
            
            await asyncio.sleep(1.5)
            
            response2 = await client.get(url=url)

            assert response2.status_code == 200, response2.status_code 

            assert response1.headers["date"] == response2.headers["date"]