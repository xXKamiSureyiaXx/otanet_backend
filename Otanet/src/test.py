
import boto3
s3_client = boto3.client('s3')
objs = s3_client.list_objects_v2(Bucket='otanet-manga-devo', Prefix=f"servamp/chapter_138/", MaxKeys=1)
print(objs)