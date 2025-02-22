import os
from datetime import datetime
from fastapi import FastAPI, Depends
from pydantic import BaseModel
from typing import Annotated
from dotenv import load_dotenv
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import delete, func, insert, select, update

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine, \
    AsyncSession

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


async def get_session():
    """Generator session."""
    async with new_session() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]


class Base(DeclarativeBase):
    pass


class BookModel(Base):
    __tablename__ = "books"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str]
    author: Mapped[str]


class FlatModel(Base):
    __tablename__ = "s1"
    __table_args__ = {"schema": "farpost"}  # Указываем схему "farpost"

    id: Mapped[int] = mapped_column(primary_key=True)
    text: Mapped[str]
    area: Mapped[str]
    link: Mapped[str]
    view: Mapped[int]
    cost: Mapped[float]
    room: Mapped[str]
    is_check: Mapped[bool]
    square: Mapped[float]
    author: Mapped[str]
    view_first: Mapped[int]
    delta_view: Mapped[int]
    is_blocked: Mapped[bool]
    date_from: Mapped[datetime]
    batch_date: Mapped[datetime]
    date_delete: Mapped[datetime]
    delta_cost: Mapped[float]
    type_post: Mapped[str]
    type_rental: Mapped[str]


app = FastAPI()


class RoomShema(BaseModel):
    room: str
    square: float | None = None


class BookAddShema(BaseModel):
    title: str
    author: str


@app.post("/setup_database/", summary="Создание БД",
          tags=["Создание таблицы в БД Postgres"])
async def setup_database():
    async with engine.begin() as conn:
        # await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    return {"Create Table": "Succsess"}


@app.post("/books/", summary="Добавление записи", tags=["Добавить запись"])
async def add_book(data: BookAddShema, session: SessionDep):
    new_book = BookModel(
        title=data.title,
        author=data.author
    )
    session.add(new_book)
    await session.commit()
    return {"message": "Book added successfully"}


@app.get("/flat/")
async def get_flat(session: SessionDep):
    query = select(FlatModel).limit(2)
    result = await session.execute(query)
    return result.scalars().all()



@app.post("/sale_flat/")
async def create_item(data: RoomShema):
    room = data.room
    square = data.square

    return {"Выгрузка для ": f"{room}- комнатных квартир, площадью -"
                             f" {square}"}
