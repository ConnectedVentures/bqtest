from google.cloud import bigquery
from functools import partial
import sys

project_id = 'fresh-8-testing'
data_set_name = 'lab_lee'
table_name = 'array_test_inferred_1532701620'


def put_item(uploader, rows_to_insert):
    errors = uploader(rows_to_insert)  # API request

    if len(errors) > 0:
        for err in errors:
            print(err)
        sys.exit(1)


def main():
    client = bigquery.Client(project=project_id)

    data_set_ref = client.dataset('lab_lee')

    schema = [
        bigquery.SchemaField('an_array', 'RECORD', mode='REPEATED', fields=[
            bigquery.SchemaField('number', 'INT64', mode='REQUIRED'),
        ]),
    ]

    table_ref = data_set_ref.table(table_name)
    table = bigquery.Table(table_ref, schema=schema)

    uploader = partial(client.insert_rows, table)
    # passes as expected
    put_item(uploader, [{u'an_array': [{u'number': 42}]}])
    print("42 passed")

    # passes as expected
    put_item(uploader, [{u'an_array': []}])
    print("empty passed")

    # fails with similar but different message from Go impl
    put_item(uploader, [{u'an_array': None}])
    print("None passed")

main()
