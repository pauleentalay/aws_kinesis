from boto3 import client

stream_arn = "arn:aws:kinesis:eu-central-1:247548036690:stream/dataset_trial"
powerdata_file = "./dataset/household_power_consumption.txt"

def create_records_by_100(data_file):
    # creates dictionary for Data parameter in put_records()
    # partition key changes every 100 lines
    with open(data_file, 'r') as f:
        all_lines = [line for line in f]
        data_chunks = [all_lines[x:x+100] for x in range(0, len(all_lines),100)] 
    records = []
    count = 0
    for chunk in data_chunks:
        record = []
        for line in chunk:
            record.append({'Data': line, 'PartitionKey': f'part_{count}'})
        count += 1
        records.append(record)
    return records


def main():
    kinesis = client('kinesis')
    power_records = create_records_by_100(powerdata_file)
    for data in power_records:
        kinesis.put_records(Records=data, StreamARN=stream_arn)
    print(f'No. of partitions: {len(power_records)}')


if __name__ == "__main__":
    main()





