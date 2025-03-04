import os
from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException, Response
from pydantic import BaseModel
from typing import Annotated, Literal
from dotenv import load_dotenv
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import delete, func, insert, select, update, text
from authx import AuthX, AuthXConfig





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

config = AuthXConfig()
config.JWT_ALGORITHM = "HS256"
config.JWT_SECRET_KEY = os.getenv('SECRET_KEY')
config.JWT_ACCESS_COOKIE_NAME = "auth_token"
config.JWT_TOKEN_LOCATION = ["cookies"]
config.JWT_COOKIE_CSRF_PROTECT = False
security = AuthX(config=config)


class RoomShema(BaseModel):
    room: str
    area: str

class UserLoginShema(BaseModel):
    login: str
    password: str

# class BookAddShema(BaseModel):
#     title: str
#     author: str


# @app.post("/setup_database/", summary="Создание БД",
#           tags=["Создание таблицы в БД Postgres"])
# async def setup_database():
#     async with engine.begin() as conn:
#         # await conn.run_sync(Base.metadata.drop_all)
#         await conn.run_sync(Base.metadata.create_all)
#     return {"Create Table": "Succsess"}


# @app.post("/books/", summary="Добавление записи", tags=["Добавить запись"])
# async def add_book(data: BookAddShema, session: SessionDep):
#     new_book = BookModel(
#         title=data.title,
#         author=data.author
#     )
#     session.add(new_book)
#     await session.commit()
#     return {"message": "Book added successfully"}

@app.post("/login", summary="Аутентификация.", tags=["Вход в учетную запись."])
async def login(creds: UserLoginShema, response: Response):
    """Authentication."""
    if creds.login == "admin" and creds.password == '123456':
        token =security.create_access_token(uid="555")
        response.set_cookie(config.JWT_ACCESS_COOKIE_NAME, token)
        return {"access_token": token}
    raise HTTPException(status_code=401, detail="Incorrect username or "
                                                "password")


@app.post("/protected",
          dependencies=[Depends(security.access_token_required)],
          )
async def protected():
    """Login session."""
    return {"data": "TOP SECRET"}



@app.get("/flat/", summary="Вывод 2 первые записи.", tags=["Вывести записи"])
async def get_flat(session: SessionDep):
    query = select(FlatModel).limit(2)
    result = await session.execute(query)
    return result.scalars().all()


@app.get("/flat_sale/", summary="Вывести самые дешовые квартиры в районе по "
                                 "числу комнат", tags=["Вывести ссылки"])
async def get_posts(room: str,
                    area: Literal["Снеговая падь",
                                  "Фадеева",
                                  "Толстого (Буссе)",
                                  "Сахарный ключ",
                                  "Центр",
                                  "Пригород",
                                  "Садгород",
                                  "Чуркин",
                                  "Некрасовская",
                                  "Третья рабочая",
                                  "Горностай",
                                  "Снеговая",
                                  "Луговая",
                                  "Спутник",
                                  "Чайка",
                                  "64, 71 микрорайоны",
                                  "Борисенко",
                                  "Весенняя",
                                  "Заря",
                                  "Трудовая",
                                  "Вторая речка",
                                  "частное лицо",
                                  "БАМ",
                                  "Столетие",
                                  "Гайдамак",
                                  "Седанка",
                                  "о. Русский",
                                  "Первая речка",
                                  "Патрокл",
                                  "Трудовое",
                                  "Баляева",
                                  "о. Попова",
                                  "Океанская",
                                  "Эгершельд",
                                  "Тихая"] = "Русский",
                    session:
                    AsyncSession = Depends(
                        get_session)):
    query = text(f"SELECT cost, concat('farpost.ru',link)  as link FROM "
                 f"farpost.s1 WHERE room = '{room}' and "
                 f"area='{area}' ORDER BY cost  limit 5")
    result = await session.execute(query)

    # Преобразуем результат в список словарей
    flats = [dict(row) for row in result.mappings()]
    return flats
