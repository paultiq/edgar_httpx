from httpxthrottlecache import HttpxThrottleCache
import time
import pytest
import asyncio
import io

class s3_mock:
    def __init__(self): 
        self.store={}
    def create_bucket(self, Bucket): 
        ...
    def put_object(self, Bucket, Key, Body, Metadata): 
        self.store[(Bucket,Key)]={"Body":Body if isinstance(Body,bytes) else Body.encode(),"Metadata":Metadata}
    def get_object(self, Bucket, Key): 
        o=self.store[(Bucket,Key)]
        return {"Body": io.BytesIO(o["Body"]), "Metadata": o["Metadata"]}
    def head_object(self, Bucket, Key): 
        return {"Metadata": self.store[(Bucket,Key)]["Metadata"]}
    def list_objects(self, Bucket): 
        return {"Contents":[{"Key": k[1]} for k in self.store.keys() if k[0]==Bucket]}
    def delete_object(self, Bucket, Key): 
        self.store.pop((Bucket,Key), None)

class boto3():
    def client(*args, **kwargs):
        return s3_mock()

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
