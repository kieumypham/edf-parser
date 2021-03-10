import boto3
import json
from urllib.parse import unquote_plus
import base64

# import sys
# sys.path.append('./../aws-lambda')
# from . import edf

from edf import EdfRecording

import time

# For other files, smaller than 1MB input, we shall return the first signal in the parsing
# We will come up with a format good enough for returning the whole parsed result. For now, as file as large as
# PSG.edf (50MB in edf) will yield near 500MB parsed result if we do not control the output.
LAMBDA_EXPORT_OPTIONS_SMALL_FILE = {
    'header': 'true',
    'slicing_by_index': [0],
    'signal': ['metadata', 'values']
}

# For very large file such as PSG.edf we shall return only metadata info to avoid an output of 130MB, even for just one signal
LAMBDA_EXPORT_OPTIONS_LARGE_FILE = {
    'header': 'true',
    'slicing_by_index': -1,
    'signal': ['metadata']
}

ONE_MEGA_BYTE = 1024 * 1024


def lambda_handler_api_gateway(event, context):
    triggered_time_in_millisecs = int(time.time() * 1000)
    print(f'Receiving request at {triggered_time_in_millisecs}')
    return {
        "statusCode": 200,
        "body": f'{triggered_time_in_millisecs}'
    }


def lambda_handler_api_gateway_edf(event, context):
    print(f'Processing EDF data received thru API gateway!')
    http_request_body = event["body"]
    raw_bytes = base64.b64decode(http_request_body)
    return {
        "statusCode": 200,
        "body": parse_binary(raw_bytes)
    }


s3_client = boto3.client('s3')


def lambda_handler_s3(event, context):
    print(f'Processing EDF data received in S3 bucket!')
    for record in event['Records']:
        in_bucket = record['s3']['bucket']['name']
        out_bucket = 'edf-data-output'
        
        key = unquote_plus(record['s3']['object']['key'])
        tmpkey = key.replace('/', '')
        input = f'/tmp/{tmpkey}'
        output = input.replace('.edf', '.json')

        print(f'Parsing file {tmpkey} from {in_bucket}: Local input: {input} Local output: {output}')

        s3_client.download_file(in_bucket, key, input)

        parse_file(input, output)
        
        s3_client.upload_file(output, out_bucket, tmpkey.replace('.edf', '.json'))


# In lambda, we defaulted to parsing just up to first hour of recording. This will cover for small file
# and will allow large file to be parsed quicker. If we need to do a full parsing of large file (10+ hrs of recording) we need to come
# up with different stratergy for delivery of the result. API gateway is no good for that.
def parse_file(input, output):
    with EdfRecording().open(input) as edf:
        edf.stream() # header
        edf.stream() # metadata
        edf.stream(EdfRecording.DEFAULT_DURATION_IN_SECONDS) # one hour worth of data
        parsed_content = json.dumps(edf.to_json_object({'header' : 'true', 'slicing_by_index': [0], 'signal': ['metadata', 'values']}))
        with open(output, "w+") as f:
            f.write(parsed_content)


def parse_binary(raw_bytes):
    # export_option = LAMBDA_EXPORT_OPTIONS_SMALL_FILE if len(raw_bytes) < ONE_MEGA_BYTE else LAMBDA_EXPORT_OPTIONS_LARGE_FILE
    file_size = len(raw_bytes)
    if file_size < ONE_MEGA_BYTE:
        export_option = LAMBDA_EXPORT_OPTIONS_SMALL_FILE
        edf = EdfRecording().parse_binary(raw_bytes)
        return json.dumps(edf.to_json_object(export_option))
    else:
        return f'{{"too_large_input_for_lambda": {file_size}}}'
