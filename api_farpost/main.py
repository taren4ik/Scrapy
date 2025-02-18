import os
from fastapi import FastAPI
from pydantic import BaseModel
from dotenv import load_dotenv
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import delete, func, insert, select, update

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")



engine = create_async_engine((f'postgresql+asyncpg://{DB_USER}:{DB_PASS}'
                              f'@{DB_HOST}:{DB_PORT}'
                f'/{DB_NAME}'))



new_session = async_sessionmaker(engine, expire_on_commit=False)

async  def get_session():
    async with new_session() as session:
        yield session



app = FastAPI()


class Param(BaseModel):
    room: str
    square: float | None = None


@app.post("/sale_flat/")
async def create_item(data: Param):
    room = data.room
    square = data.square


    return {"Выгрузка для ": f"{room}- комнатных квартир, площадью -"
                             f" {square}"}



