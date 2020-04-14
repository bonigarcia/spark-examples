from influxdb_client import InfluxDBClient

influxClient = InfluxDBClient.from_config_file("../config/influxdb.ini")
bucket = "boni.garcia's Bucket"
kind = "sine-wave"
myquery = f'from(bucket: "{bucket}") |> range(start: -1d) |> filter(fn: (r) => r._measurement == "{kind}")'
tables = influxClient.query_api().query(myquery)

for table in tables:
    print(table)
    for row in table.records:
        print(row.values)
