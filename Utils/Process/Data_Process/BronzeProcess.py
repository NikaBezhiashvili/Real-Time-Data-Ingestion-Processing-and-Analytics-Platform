import csv
from Utils.Process.Data_Process.Send_Request import send_row_to_api


# Path to  CSV file
transactions_file = '../../Data/transactions.csv'
# Specify the API endpoint
api_endpoint = 'http://127.0.0.1:8000/Bronze/store_transaction'


def read_csv():
    """
    Reads data by rows from csv, fills blank end_date with 4444-11-11 sends the POST REQUEST to api_endpoint
    :return:
    """
    with open(transactions_file, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        try:
            for row in reader:
                # Convert the row to a JSON object (dict)
                row_data = {key: value for key, value in row.items()}

                if row_data['END_DATE'] == '':
                    row_data['END_DATE'] = '4444-11-11'
                # Send the row data to the API
                send_row_to_api(row_data, api_endpoint)
                # time.sleep(2)
        except Exception as e:
            print(e)


read_csv()
