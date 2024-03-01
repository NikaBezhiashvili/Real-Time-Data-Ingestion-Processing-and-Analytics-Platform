import psycopg2

# GoldLayer is used for materialized views

# SilverDB Connection for gold layer
conn = psycopg2.connect(
    dbname="SilverDB",
    user="postgres",
    password="admin",
    host="localhost",
    port="5432"
)


def create_transaction_stat_refresh(connection) -> bool:

    """
    creates refresh_tran_stats function which returns as trigger
    and refreshes transaction_type_amount_report CONCURRENTLY

    :param connection: SilverDB connection
    :return: returns True/False
    """
    cursor = connection.cursor()

    try:
        cursor.execute("""create or replace function refresh_tran_stats() returns trigger as $$ begin 
                          REFRESH MATERIALIZED VIEW CONCURRENTLY transaction_type_amount_report;
                           RETURN NULL; 
                           END;
                           $$ LANGUAGE plpgsql""")

        connection.commit()
        print('Function for transaction_type_amount_report triggers Created')
        return True
    except Exception as e:
        print(f'create_transaction_stat_refresh: {e}')
        return False


def create_transaction_stat_trigger(connection) -> bool:
    """
    Creates trigger trans_stat_insert, trans_stat_update, trans_stat_delete which calls refresh_tran_stats function
    :param connection: SilverDB connection
    :return:
    """
    cursor = connection.cursor()

    try:
        cursor.execute(
            """create trigger trans_stat_insert after insert on silver_transactions  for each row execute function refresh_tran_stats()""")

        connection.commit()

        cursor.execute(
            """create trigger trans_stat_update after update on silver_transactions  for each row execute function refresh_tran_stats()""")

        connection.commit()

        cursor.execute(
            """create trigger trans_stat_delete after delete on silver_transactions  for each row execute function refresh_tran_stats()""")

        connection.commit()

        print('Triggers for transaction_type_amount_report Created')
        return True
    except Exception as e:
        print(f'create_transaction_stat_trigger: {e}')
        return False


def create_transaction_stats(connection) -> bool:

    """
    creates materialized_view transaction_type_amount_report, which contains sum amounts grouped by exp_type
    also creates Unique index exp_type_index on view's exp_type

    :param connection:  SilverDB connection
    :return:
    """
    cursor = connection.cursor()

    try:
        cursor.execute("""CREATE MATERIALIZED VIEW IF NOT EXISTS transaction_type_amount_report as 
                          select exp_type, round(sum(amount)) S_amount from silver_transactions group by exp_type;""")

        connection.commit()

        cursor.execute(
            """CREATE  unique INDEX IF NOT EXISTS exp_type_index ON transaction_type_amount_report (exp_type);""")

        connection.commit()

        print('Materialized View transaction_type_amount_report Created ')
        return True
    except Exception as e:
        print(f'create_transaction_stats: {e}')
        return False


def select_transaction(connection, trans_id) -> dict:
    """
    returns data about given transaction

    :param connection: SilverDB connection
    :param trans_id: transaction_id
    :return:
    """
    cursor = connection.cursor()

    try:
        cursor.execute(f"""select t.CUST_ID, t.TRANS_ID, t.TRAN_DATE, e.exp_title,  t.AMOUNT  from silver_transactions t
                            inner join exp_types e on t.exp_type = e.exp_type where t.trans_id = '{trans_id}'""")

        row_data = cursor.fetchall()[0]

        transaction_info = {
            'CUST_ID': row_data[0],
            'TRANS_ID': row_data[1],
            'TRAN_DATE': row_data[2],
            'EXP_TYPE': row_data[3],
            'AMOUNT': row_data[4],

        }
        return transaction_info
    except Exception as e:
        print(f'select_transaction: {e}')
        return {'message': e}
