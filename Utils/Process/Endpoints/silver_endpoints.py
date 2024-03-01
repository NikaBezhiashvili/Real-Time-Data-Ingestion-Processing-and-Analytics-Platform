import datetime

from fastapi import APIRouter
from Utils.Database.Silver_Database.SilverDB import create_silver_table, conn, create_silver_partitions, \
    create_silver_exp_type_table, create_exp_types, create_customer_table, add_customer, store_transaction
from pydantic import BaseModel

router = APIRouter(prefix='/Silver', tags=['Silver Endpoints'])


class SilverTransaction(BaseModel):
    """SilverDB Transaction Model
        CUSTOMER_ID,
        TRANSACTION_ID,
        TRANSACTION_DATE,
        TRANSACTION_TYPE,
        AMOUNT"""
    CUST_ID: str
    TRANS_ID: str
    TRAN_DATE: datetime.date
    EXP_TYPE: str
    AMOUNT: float


class SilverClient(BaseModel):
    """SilverDB Customer Mode
        CUSTOMER_ID,
        CUSTOMER_REGISTRATION_DATE
        CUSTOMER_INACTIVE_STATUS (DATE)"""
    CUST_ID: str
    START_DATE: datetime.date
    END_DATE: datetime.date


@router.post("/create_silver_table")
async def create_silver_transactions():
    """
    creates silver_transactions table if not exists, according to create_silver_partitions function
    :return:
    """
    try:
        create_silver_table(conn, 'silver_transactions')
        return {"Table": "Created"}
    except Exception as e:
        return {'Failed': e}


@router.post("/create_customer_table")
async def customer_table():
    create_customer_table(conn, 'CUSTOMERS')
    return {'Table' : 'Created'}


@router.post("/create_partitions")
async def silver_partitions():
    """
    creates silver_transactions table if not exists, according to create_main_table function
    :return:
    """
    try:
        create_silver_partitions(conn)
        return {"Partitions": "Created"}
    except Exception as e:
        return {'Failed': e}


@router.post("/create_silver_exp_types")
async def create_silver_exp_types():
    """
    creates exp_types table if not exists, according to create_silver_exp_types function
    :return:
    """
    try:
        create_silver_exp_type_table(conn, 'exp_types')
        return {"Table": "Created"}
    except Exception as e:
        return {'Failed': e}


@router.post("/create_exp_type_keys")
async def create_exp_type_keys():
    """Adds EXP_TYPE key references """
    try:
        create_exp_types(conn)
        return {'Keys': 'Added'}
    except Exception as e:
        return {'Failed': e}


@router.post("/store_transaction")
async def store_silver_transaction(data: SilverTransaction):
    """
    Stores Transaction From BronzeDB
    :param data: SilverTransaction model JSON
    :return:
    """
    try:
        store_transaction(conn, data.CUST_ID, data.TRANS_ID, data.TRAN_DATE, data.EXP_TYPE, data.AMOUNT)
    except Exception as e:
        return {'failed': e}


@router.post("/store_client")
async def store_silver_client(data: SilverClient):
    """
    Stores Client from BronzeDB
    :param data: SilverClient model JSON
    :return:
    """
    add_customer(conn, data.CUST_ID, data.START_DATE, data.END_DATE)
    return {'message': 'hello'}
