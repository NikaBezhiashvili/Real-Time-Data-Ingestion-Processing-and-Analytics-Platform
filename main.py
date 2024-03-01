import uvicorn
from fastapi import FastAPI
from Utils.Process.Endpoints.base_endpoints import router as base_router
from Utils.Process.Endpoints.bronze_endpoints import router as bronze_router
from Utils.Process.Endpoints.silver_endpoints import router as silver_router
from Utils.Process.Endpoints.gold_endpoints import router as gold_router
from Utils.Database.database_buildup import database_buildup

app = FastAPI(title='Commschool Project' ,description='Commschool DataEngineering Project')

"""Endpoint Include"""

app.include_router(base_router)
app.include_router(bronze_router)
app.include_router(silver_router)
app.include_router(gold_router)

"""UNCOMMENT THIS SECTION IF DATABASE DOES NOT EXISTS"""

try:
     database_buildup()
except Exception as e:
     print('Database already built')

"""Server Start"""

if __name__ == '__main__':
    uvicorn.run(app, port=8000)
