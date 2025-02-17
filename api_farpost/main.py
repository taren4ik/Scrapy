from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class Param(BaseModel):
    room: str
    square: float | None = None

@app.post("/sale_flat/")
async def create_item(item: Param):
    # запроса
    return item