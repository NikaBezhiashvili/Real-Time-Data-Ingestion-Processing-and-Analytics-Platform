from Utils.Database.Bronze_Database.BronzeDB import conn as bronze_connnection
from Utils.Process.Data_Process.Send_Request import send_row_to_api

bronze_cursor = bronze_connnection.cursor()
api_endpoint = 'http://127.0.0.1:8000/Silver/store_transaction'
Transaction_ID = 0
Client_ROWID = 0

while True:
    """ GETS TRANSACTION DATA FROM BronzeDB and sends to SilverDB with api_endpoint"""
    Transaction_ID += 1
    bronze_cursor.execute(
        f"""select cust_id, trans_id, tran_date, exp_type, amount from  raw_transactions where rowid = {Transaction_ID}""")
    try:
        row_data = bronze_cursor.fetchmany()[0]
        row_json = {
            'CUST_ID': row_data[0],
            'TRANS_ID': row_data[1],
            'TRAN_DATE': str(row_data[2]),
            'EXP_TYPE': row_data[3],
            'AMOUNT': row_data[4],
        }
        send_row_to_api(row_json, api_endpoint)

    except Exception as e:
        print(f'All items have done: {e}')
        break


def add_clients():
    """GETS CLIENT DATA FROM BronzeDB and sends to SilverDB with client_endpoint"""
    client_endpoint = 'http://127.0.0.1:8000/Silver/store_client'
    bronze_cursor.execute(
        f"""select distinct cust_id, start_date, end_date from  raw_transactions  """)
    try:
        client_data = bronze_cursor.fetchall()
        for client in client_data:
            client_row = {'CUST_ID': client[0],
                          'START_DATE': str(client[1]),
                          'END_DATE': str(client[2])}
            print(client_row)
            send_row_to_api(client_row, client_endpoint)

    except Exception as e:
        print(f'All items have done: {e}')


add_clients()
