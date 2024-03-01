from fastapi import APIRouter
from Utils.Database.Bronze_Database.BronzeDB import create_main_table, conn, insert_data_to_bronzedb, \
    check_if_tran_exists, delete_record
from datetime import date
from typing import Optional
from pydantic import BaseModel

router = APIRouter(prefix='/Bronze', tags=['Bronze Endpoints'])


class Transaction(BaseModel):
    CUST_ID: str
    START_DATE: date
    END_DATE: Optional[date] = None
    TRANS_ID: str
    DATE: date
    YEAR: int
    MONTH: int
    DAY: int
    EXP_TYPE: str
    AMOUNT: Optional[float] = 0


@router.post("/create_table")
async def create_table():
    """
    creates RAW_TRANSACTIONS table if not exists, according to create_main_table function
    :return:
    """
    try:
        create_main_table(conn, 'RAW_TRANSACTIONS')
        return {"Table": "Created"}
    except Exception as e:
        return {'Failed': e}


@router.post('/store_transaction/')
async def process_transaction(data: Transaction):
    """
    gets data with request following to Transaction basemodel, checks if transaction exists in raw_transactions
    table, if not check_if_tran_exists function returns true and inserts data in table
    :param data: request data
    :return:
    """
    if check_if_tran_exists(conn, data.TRANS_ID):
        insert_data_to_bronzedb(conn, data)
        return {'Transaction': 'Stored'}
    else:
        return {'Duplicate': 'Found'}


@router.delete('/delete_transaction/{tran_id}')
async def delete_transaction_by_id(tran_id: str):
    """Checks if transaction exists, then deletes."""
    delete_record(conn, tran_id)
    return {'Delete': tran_id}
