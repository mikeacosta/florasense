# package data stream
aws cloudformation package --s3-bucket real-time-sensors --template-file kinesis_datastream.yaml --output-template-file gen/kinesis_datastream_generated.yaml

# deploy data stream
aws cloudformation deploy --template-file gen/kinesis_datastream_generated.yaml --stack-name FloraSenseStream

# package firehose
aws cloudformation package --s3-bucket real-time-sensors --template-file kinesis_firehose.yaml --output-template-file gen/kinesis_firehose_generated.yaml

# deploy firehose
aws cloudformation deploy --template-file gen/kinesis_firehose_generated.yaml --stack-name FloraSenseFirehose

# delete firehose
aws cloudformation delete-stack --stack-name FloraSenseFirehose

# delete data stream
aws cloudformation delete-stack --stack-name FloraSenseStream