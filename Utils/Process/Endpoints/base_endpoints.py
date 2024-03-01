from fastapi import APIRouter

router = APIRouter(prefix='', tags=['Base Endpoint'])


@router.get("/")
async def base_endpoint():
    return {"message": "Base_Endpoint"}
