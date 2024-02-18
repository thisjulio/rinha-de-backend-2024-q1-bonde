import asyncio
import datetime
import os
from enum import Enum
from typing import List

import asyncpg
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


class FastAPIWithDatabase(FastAPI):
    database_connection: asyncpg.Connection = None
    database_pool: asyncpg.Pool = None


app = FastAPIWithDatabase()


class TransactionTypeInputEnum(str, Enum):
    credit = "c"
    debit = "d"


class TransactionInput(BaseModel):
    valor: int
    tipo: TransactionTypeInputEnum
    descricao: str = Field(max_length=10)


class TransactionOutput(BaseModel):
    limite: int
    saldo: int


class StatementBalanceOutput(BaseModel):
    total: int
    limite: int
    data_extrato: str


class StatementTransactionOutput(TransactionInput):
    realizada_em: str


class StatementOutput(BaseModel):
    saldo: StatementBalanceOutput
    ultimas_transacoes: List[StatementTransactionOutput]


@app.get("/")
def read_root():
    return {"Hello": "World"}


client_cache = {}


async def fetch_client_from_db(id: int):
    if id in client_cache:
        return client_cache[id]

    async with app.database_pool.acquire() as connection:
        query = "SELECT * FROM clientes WHERE id = $1"
        client_cache[id] = await connection.fetchrow(query, id)
        return client_cache[id]


async def credit_balance_in_db(cliente_id: int, change_value: int):
    async with app.database_pool.acquire() as connection:
        async with connection.transaction():
            query = "UPDATE saldos SET valor = valor + $1 WHERE cliente_id = $2 RETURNING valor"
            result = await connection.fetchrow(query, change_value, cliente_id)
            return result


async def debit_balance_in_db(cliente_id: int, change_value: int, limit: int):
    async with app.database_pool.acquire() as connection:
        async with connection.transaction():
            query = "UPDATE saldos SET valor = valor - $1 WHERE (cliente_id = $2) and (abs(valor - $1) <= $3) RETURNING valor"
            result = await connection.fetchrow(query, change_value, cliente_id, limit)
            if not result:
                raise ValueError("Limite Insuficiente")
            return result


async def create_transaction_in_db(cliente_id: int, transaction: TransactionInput):
    async with app.database_pool.acquire() as connection:
        query = "INSERT INTO transacoes (cliente_id, valor, tipo, descricao, realizada_em) VALUES ($1, $2, $3, $4, $5) RETURNING *"
        await connection.fetchrow(query, cliente_id, transaction.valor, transaction.tipo, transaction.descricao, datetime.datetime.now())


async def get_statement_from_db(cliente_id: int):
    async with app.database_pool.acquire() as connection:
        query = "SELECT * FROM transacoes WHERE cliente_id = $1 ORDER BY realizada_em DESC LIMIT 10 OFFSET 0"
        return await connection.fetch(query, cliente_id)


async def get_balance_from_db(cliente_id: int):
    async with app.database_pool.acquire() as connection:
        query = "SELECT * FROM saldos WHERE cliente_id = $1"
        return await connection.fetchrow(query, cliente_id)


@app.post("/clientes/{id}/transacoes", response_model=TransactionOutput)
async def create_transaction(id: int, transaction: TransactionInput):
    account = await fetch_client_from_db(id)
    if not account:
        raise HTTPException(status_code=404, detail="Cliente não encontrado")

    try:
        if transaction.tipo == TransactionTypeInputEnum.credit:
            result = await credit_balance_in_db(id, transaction.valor)
        elif transaction.tipo == TransactionTypeInputEnum.debit:
            result = await debit_balance_in_db(id, transaction.valor, account['limite'])
        else:
            raise HTTPException(
                status_code=400, detail="Tipo de transação inválido")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except HTTPException as e:
        raise e

    new_balance = result['valor']
    await create_transaction_in_db(id, transaction)
    return TransactionOutput(limite=account['limite'], saldo=new_balance)


@app.get("/clientes/{id}/extrato", response_model=StatementOutput)
async def get_extrato(id: int):
    account = await fetch_client_from_db(id)
    if not account:
        raise HTTPException(status_code=404, detail="Cliente não encontrado")
    balance = await get_balance_from_db(id)
    statement = await get_statement_from_db(id)
    return StatementOutput(
        saldo=StatementBalanceOutput(
            total=balance['valor'],
            limite=account['limite'],
            data_extrato=datetime.datetime.now().isoformat()),
        ultimas_transacoes=[
            StatementTransactionOutput(
                valor=t['valor'],
                tipo=t['tipo'],
                descricao=t['descricao'],
                realizada_em=t['realizada_em'].isoformat()
            )
            for t in statement
        ]
    )


async def start_connection():
    db_host = os.getenv("DB_HOSTNAME", "localhost")
    while True:
        try:
            app.database_connection = await asyncpg.connect(f'postgresql://admin:123@{db_host}/rinha')
            app.database_pool = await asyncpg.create_pool(f'postgresql://admin:123@{db_host}/rinha')
            break
        except ConnectionError as e:
            print("Database connection error, will sleep for 5 seconds:", e)
            await asyncio.sleep(5)


@app.on_event("startup")
async def _startup():
    await start_connection()
    print("Starting up...")

app.on_event("shutdown")


async def _shutdown():
    if app.database_connection:
        await app.database_connection.close()
    print("Shutting down...")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9999, lifespan="on")
