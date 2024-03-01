from fastapi import APIRouter
from Utils.Database.Gold_Layer.GoldL import conn, select_transaction

router = APIRouter(prefix='/Gold', tags=['Gold Endpoints'])


@router.get('/get_transaction_info/{trans_id}')
async def get_transaction(trans_id):
    """
    gets transaction id as parameter, selects data from silver_transactions @SilverDB and returns as JSON
    :param trans_id: transaction id
    :return: returns transaction info
    """
    return {'Data': select_transaction(conn, trans_id)}
