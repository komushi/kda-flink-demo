import json
import boto3
import random
import datetime
import time

kinesis = boto3.client('kinesis')
def getReferrer():
    data = {}
    properties = {}
    now = datetime.datetime.utcnow()
    str_now = now.isoformat(timespec='milliseconds')
    properties['RECEIVED_ON'] = str_now
    properties['N02_001'] = random.choice(['11', '12', '13', '14', '15', '16', '17', '18'])
    properties['N02_002'] = random.choice(['1', '2', '3', '4', '5'])
    properties['N02_003'] = random.choice(['上越新幹線', '九州新幹線', '北海道新幹線', '北陸新幹線', '山陽新幹線', '東北新幹線', '東海道新幹線'])
    properties['N02_004'] = random.choice(['東日本旅客鉄道', '西日本旅客鉄道'])
    properties['ID'] = properties['N02_002'] + '_' + str(random.randint(1, 101))
    properties['COUNT'] = random.randint(10, 20)
    data['type'] = 'Feature'
    data['properties'] = properties
    return data

cnt = 0

while cnt < 40:
        time.sleep(.100)
        cnt += 1
        data = getReferrer()
        dataString = json.dumps(getReferrer())
        print(dataString)
        kinesis.put_record(
                StreamName="amp_geojson",
                Data=dataString,
                PartitionKey=data['properties']['ID'])