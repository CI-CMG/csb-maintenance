import json

msg = {
    'Records': [
        {
            'EventSource': 'aws:sns',
            'EventVersion': '1.0',
            'EventSubscriptionArn': 'arn:aws:sns:us-east-1:709902155096:RemoveDCDBObject:9f29d42b-7b19-4b53-86ce-b4d6d9197a9a',
            'Sns': {
                'Type': 'Notification',
                'MessageId': 'e2d3ac34-ae2a-5bf0-a92d-f223706f4936',
                'TopicArn': 'arn:aws:sns:us-east-1:709902155096:RemoveDCDBObject',
                'Subject': 'Amazon S3 Notification',
                'Message': '{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2024-01-09T23:51:32.093Z","eventName":"ObjectRemoved:Delete","userIdentity":{"principalId":"AWS:AIDA2KSLXHVMNXFLE4OIY"},"requestParameters":{"sourceIPAddress":"73.229.72.157"},"responseElements":{"x-amz-request-id":"KDSEJKSSMVZJNZ0C","x-amz-id-2":"acraABKAK6sB7cjVdhGoCKfIE2lZGZz03BamZdEp9xnQNwV376NpIJ7Yox6X6eIWmcILHiweYYYp+mbCvHYvl424JBmHpuvq"},"s3":{"s3SchemaVersion":"1.0","configurationId":"Remove Objects","bucket":{"name":"noaa-dcdb-bathymetry-pds","ownerIdentity":{"principalId":"A1FTB9C2R32E6T"},"arn":"arn:aws:s3:::noaa-dcdb-bathymetry-pds"},"object":{"key":"docs/test.txt","sequencer":"00659DDC0415E11641"}}}]}',
                'Timestamp': '2024-01-09T23:51:33.519Z', 'SignatureVersion': '1',
                'Signature': 'Lhocy3TPgTiVLMAELPK2i0Z+pqRDrrTBkG9eiofN3COdLAa0VHIom1OPyLF0a0zgBqwF7ot5qqH/9ObFdNDzAgvAGTFH22kLN63fELn6dtqRdpFFQb1NGKPLlojkVKcw5Gvecro9+7Yq5tEvHTVMHcJ9ccXb1Z/MHCzh0uQ6jUreAcOpzJufmVfOlGO3/zVsnAjlgYowCoXFqoAV8/wLyC4xNmqdylL+FEYnMGK5ID4ytxp2u/qSi9dAJR/FOs4SiFmL3gmQmk9uMvI3U5MAZwMHNP/b1pbxl0k8nlBpZwxc1I+GbJZZ8u2YQyq8jPmDfVoyCr8A7LlQDeUQFT2/Yg==',
                'SigningCertUrl': 'https://sns.us-east-1.amazonaws.com/SimpleNotificationService-01d088a6f77103d0fe307c0069e40ed6.pem',
                'UnsubscribeUrl': 'https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:709902155096:RemoveDCDBObject:9f29d42b-7b19-4b53-86ce-b4d6d9197a9a',
                'MessageAttributes': {}
            }
        }
    ]
    }

print(json.dumps(msg, indent=2))
