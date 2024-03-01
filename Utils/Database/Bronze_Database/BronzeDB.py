import psycopg2

# BronzeDB is used for raw data storage

# BronzeDB Connection
conn = psycopg2.connect(
    dbname="BronzeDB",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)

def create_main_table(connection, name) -> bool:
    """
    Creates table
    :param cursor: connected cursor to BronzeDB database
    :param name: name of table
    :return: its considered as a Procedure which returns True/False depending on how process finished
    """
    cursor = connection.cursor()

    try:
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {name} (         ROWID SERIAL PRIMARY KEY,
                                                                       CUST_ID VARCHAR(50) NOT NULL,
                                                                       START_DATE DATE  DEFAULT NULL ,
                                                                       END_DATE DATE DEFAULT NULL ,
                                                                       TRANS_ID VARCHAR(50) NOT NULL,
                                                                       TRAN_DATE DATE,
                                                                       YEAR INT,
                                                                       MONTH INT,
                                                                       DAY INT,
                                                                       EXP_TYPE VARCHAR(50),
                                                                       AMOUNT FLOAT)""")
        connection.commit()
        print(f'Table {name} Created')
        return True
    except Exception as e:
        print(e)
        return False

def insert_data_to_bronzedb(connection ,data) -> bool:
    """

    :param connection: BronzeDB connection
    :param data: gets data from Utils.Process.Endpoints.bronze_endpoints.process_transaction
    :return: inserts data in raw_transactions
    """

    cursor = connection.cursor()

    try:
        cursor.execute(f"""INSERT INTO raw_transactions  (CUST_ID, START_DATE, END_DATE, TRANS_ID, TRAN_DATE, 
                                                          YEAR, MONTH, DAY, EXP_TYPE, AMOUNT) 
                                                         values ('{data.CUST_ID}', '{data.START_DATE}', '{data.END_DATE}',
                                                                 '{data.TRANS_ID}', '{data.DATE}', {data.YEAR}, {data.MONTH},
                                                                  {data.DAY}, '{data.EXP_TYPE}', {data.AMOUNT}) """)
        connection.commit()
        print('Row inserted sucessfully')
    except Exception as e:
        print(e)
        return False

def check_if_tran_exists(connection, trans_id) -> bool:
    """
    checks if transaction exists in raw_transactions, prevents transaction duplication

    :param connection: BronzeDB connection
    :param trans_id: Transaction ID
    :return: returns True/False
    """

    cursor = connection.cursor()

    try:
        cursor.execute(f"""SELECT TRANS_ID FROM RAW_TRANSACTIONS WHERE TRANS_ID = '{trans_id}'""")
        transaction_id = cursor.fetchall()
        if len(transaction_id) == 0:
            return True
        else:
            return False
    except Exception as e:
        print(f'check_if_tran_exists: {e} ' )
        return False

def delete_record(connection, trans_id) -> bool:
    """
    Deletes records with given trans_id from raw_transactions
    :param connection: BronzeDB connection
    :param trans_id: Transaction ID
    :return:
    """

    cursor = connection.cursor()
    try:
        if not check_if_tran_exists(connection, trans_id):
            cursor.execute(f"""DELETE FROM RAW_TRANSACTIONS WHERE TRANS_ID = '{trans_id}'""")
            print(f'{trans_id} Transaction has been deleted!')
            connection.commit()
            return True
        else:
            print(f'{trans_id} Transaction on this id does not exists!')
            return False
    except Exception as e:
        print(f'delete_record: {e} ')
        return False


