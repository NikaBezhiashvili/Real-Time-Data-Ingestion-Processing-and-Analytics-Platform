import psycopg2

# SilverDB is used for clean, partitioned data

# SilverDB Connection
conn = psycopg2.connect(
    dbname="SilverDB",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)


def create_silver_table(connection, name) -> bool:
    """
    Creates table
    :param cursor: connected cursor to SilverDB database
    :param name: name of table
    :return: its considered as a Procedure which returns True/False depending on how process finished
    """
    cursor = connection.cursor()

    try:
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {name} (CUST_ID VARCHAR(50) NOT NULL,
                                                                       TRANS_ID VARCHAR(50) NOT NULL,
                                                                       TRAN_DATE DATE,
                                                                       EXP_TYPE INT,
                                                                       AMOUNT FLOAT) partition by LIST  (EXP_TYPE)""")
        cursor.execute(f"""CREATE INDEX IF NOT EXISTS tran_index ON {name}(TRANS_ID);""")
        connection.commit()
        print(f'Table {name} Created')
        return True
    except Exception as e:
        print(e)
        return False


def create_silver_exp_type_table(connection, name) -> bool:
    """
    creates exp_type table, which contains ID (EXP_TYPE) :TYPE (EXP_TITLE) references
    :param connection: SilverDB ConnectionSilverDB Connection
    :param name: table name
    :return: returns True/False
    """
    cursor = connection.cursor()

    try:
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {name} (exp_type INT, exp_title varchar(50) )""")
        connection.commit()
        print(f'Table {name} Created')
        return True
    except Exception as e:
        print(e)
        return False


def create_silver_partitions(connection) -> bool:
    """
    Creates table
    :param connection: SilverDB Connection
    :param name: name of table
    :return: its considered as a Procedure which returns True/False depending on how process finished
    """
    cursor = connection.cursor()

    try:
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS transactions_1 PARTITION OF silver_transactions FOR VALUES in (1);""")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS transactions_2 PARTITION OF silver_transactions FOR VALUES in (2);""")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS transactions_3 PARTITION OF silver_transactions FOR VALUES in (3);""")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS transactions_4 PARTITION OF silver_transactions FOR VALUES in (4);""")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS transactions_5 PARTITION OF silver_transactions FOR VALUES in (5);""")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS transactions_6 PARTITION OF silver_transactions FOR VALUES in (6);""")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS transactions_7 PARTITION OF silver_transactions FOR VALUES in (7);""")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS transactions_8 PARTITION OF silver_transactions FOR VALUES in (8);""")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS transactions_9 PARTITION OF silver_transactions FOR VALUES in (9);""")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS transactions_10 PARTITION OF silver_transactions FOR VALUES in (10);""")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS transactions_11 PARTITION OF silver_transactions FOR VALUES in (11);""")
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS transactions_12 PARTITION OF silver_transactions FOR VALUES in (12);""")

        connection.commit()
        print(f'Partitions Created')
        return True
    except Exception as e:
        print(e)
        return False


def create_exp_types(connection) -> bool:
    """
    inserts types in exp_types table according to EXP_TYPES Dictionary
    :param connection: SilverDB ConnectionSilverDB
    :return: returns True/False
    """
    cursor = connection.cursor()
    exp_types = {'Groceries': 1,
                 'Clothing': 2,
                 'Housing': 3,
                 'Education': 4,
                 'Health': 5,
                 'Motor/Travel': 6,
                 'Entertainment': 7,
                 'Gambling': 8,
                 'Savings': 9,
                 'Bills and Utilities': 10,
                 'Tax': 11,
                 'Fines': 12,
                 }
    try:
        for key in exp_types.keys():
            cursor.execute(f"""select * from exp_types where exp_title = '{key}'""")
            existKey = cursor.fetchall()
            if not existKey:
                cursor.execute(f"""INSERT INTO EXP_TYPES VALUES ({exp_types[key]}, '{key}')""")
                connection.commit()
            else:
                pass

        # connection.commit()
        print(f'keys added')
        return True
    except Exception as e:
        print(f'create_exp_types: {e}')
        return False


def create_customer_table(connection, name) -> bool:
    """
    creates Customer table
    :param connection: SilverDB connection
    :param name: table name
    :return: True/False
    """
    cursor = connection.cursor()

    try:
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {name} (CUST_ID varchar(50), START_DATE DATE, END_DATE DATE )""")
        connection.commit()
        print(f'Table {name} Created')
        return True
    except Exception as e:
        print(e)
        return False


def create_customer_function(connection):
    """
    creates function which checks if end_date = '4444-11-11' means that customer is still active, changes end_date to null
    :param connection: SilverDB connection
    :return:
    """
    cursor = connection.cursor()

    cursor.execute("""CREATE OR REPLACE FUNCTION UPDATE_END_DATE() RETURNS TRIGGER AS  $$
                      BEGIN  if NEW.END_DATE = '4444-11-11' then
                              UPDATE customers SET end_date = NULL WHERE cust_id = NEW.CUST_ID;  
                              END IF;
                            RETURN NEW;
                            END;
                        $$ LANGUAGE plpgsql""")
    connection.commit()


def customer_trigger(connection):
    """
    Creates trigger after insert on customer and executes function UPDATE_END_DATE
    :param connection: SilverDB connection
    :return:
    """
    cursor = connection.cursor()

    cursor.execute("""CREATE TRIGGER customer_end_date_trigger
                    AFTER INSERT ON customers
                    FOR EACH ROW
                    EXECUTE FUNCTION UPDATE_END_DATE();
                    """)
    connection.commit()


def add_customer(connection, cust_id, start_date, end_date) -> bool:
    """
    gets data from POST REQUEST checks if customer does not exists inserts to customers,
    :param connection: SilverDB connection
    :param cust_id: customer_id
    :param start_date:
    :param end_date:
    :return:
    """
    cursor = connection.cursor()

    cursor.execute(f"""select cust_id from customers where cust_id = '{cust_id}'""")

    client_data = cursor.fetchall()
    try:
        if len(client_data) == 0:
            cursor.execute(f"""insert into customers values ('{cust_id}', '{start_date}', '{end_date}')""")
            print(f'Customer {cust_id} Added')
            connection.commit()
            return True
        else:
            print(f'Customer already exists {cust_id} ')
    except Exception as e:
        print(f'add_customer: {e}')
        return False


def store_transaction(connection, cust_id, trans_id, tran_date, exp_type, amount) -> bool:
    """
    Gets data from POST REQUEST checks if transaction does not exists, inserts data in silver_transactions

    PREVENTS DUPLICATION

    :param connection: SilverDB
    :param cust_id:
    :param trans_id:
    :param tran_date:
    :param exp_type:
    :param amount:
    :return:
    """
    cursor = connection.cursor()

    cursor.execute(f"""select trans_id from silver_transactions where trans_id = '{trans_id}'""")

    transaction_info = cursor.fetchall()
    try:
        if not transaction_info:
            cursor.execute(f"""insert into silver_transactions values 
                                        ('{cust_id}', '{trans_id}', '{tran_date}',
                                        (select exp_type from exp_types where exp_title = '{exp_type}'),
                                        {amount})""")
            connection.commit()
            print(f'Transaction {trans_id} has been added!')
            return True
        else:
            print(f'Transaction {trans_id} already exists!')
            return False
    except Exception as e:
        print(e)
        return False

