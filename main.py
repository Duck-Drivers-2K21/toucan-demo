import boto3
from boto3.dynamodb.conditions import Key
import cv2
import numpy as np

TABLE_NAME = 'toucan-dynamoDB2'
WEBCAM_ID = "36:8f:3b:e1:44:db"

# * Query...
# Key condition expression & expression attribute values
# kce = 'WebcamID = :wc_id and TOD >= :tod'
# tod = str(int(time.time() - 60))  # time of day
# eav = {':wc_id': {'S': '36:8f:3b:e1:44:db'}, ':tod': {'N': tod}}
# response = dynamodb.query(TableName=table_name, KeyConditionExpression=kce, ExpressionAttributeValues=eav)
# for item in response['Items']:
#   print(item, type(item))


def fetch_entries(N: int) -> list:
  """
  Retrieve N entries from the DynamoDB table using the specified WebcamID value.
  Args:
  - N: An integer representing the number of entries to retrieve.
  Returns:
  - A list of dictionaries containing the retrieved entries.
  """
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(TABLE_NAME)
  return table.query(KeyConditionExpression=Key('WebcamID').eq(WEBCAM_ID),
    ScanIndexForward=False, Limit=5)['Items']


def get_image(key: str) -> np.ndarray:
  """
  Retrieve an image from an S3 bucket and decode it into a NumPy array.
  Args:
  - key: A string representing the key of the image in the S3 bucket.
  Returns:
  - A NumPy array representing the decoded image.
  """
  BUCKET_NAME = 'toucan-data'
  s3 = boto3.client('s3')
  img_data = s3.get_object(Bucket=BUCKET_NAME, Key=key)['Body'].read()
  np_img = np.frombuffer(img_data, np.uint8)
  return cv2.imdecode(np_img, cv2.IMREAD_COLOR)


if __name__ == '__main__':
  entries = fetch_entries(1)
  for i in range(len(entries) - 1, -1, -1):
    tod = entries[i]['TOD']
    img = get_image(entries[i]['image_uuid'])
    cv2.imshow(str(tod), img)
    cv2.waitKey(0)
