import Utils.Database.Bronze_Database.BronzeDB as Bronze
import Utils.Database.Silver_Database.SilverDB as Silver
import Utils.Database.Gold_Layer.GoldL as Gold


def database_buildup():
    """
    Builds Database with tables, indexes, partitions, functions and triggers
    :return:
    """
    try:
        Bronze.create_main_table(Bronze.conn, 'RAW_TRANSACTIONS')
        Silver.create_silver_table(Silver.conn, 'SILVER_TRANSACTIONS')
        Silver.create_silver_partitions(Silver.conn)
        Silver.create_silver_exp_type_table(Silver.conn, 'EXP_TYPES')
        Silver.create_exp_types(Silver.conn)
        Silver.create_customer_table(Silver.conn, 'CUSTOMERS')
        Silver.create_customer_function(Silver.conn)
        Silver.customer_trigger(Silver.conn)
        Gold.create_transaction_stats(Gold.conn)
        Gold.create_transaction_stat_refresh(Gold.conn)
        Gold.create_transaction_stat_trigger(Gold.conn)
    except:
        return False
