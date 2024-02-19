import asyncio
import datetime
from http import client
import json
import os
from enum import Enum
from typing import List
from webbrowser import get

import aioredis
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


class FastAPIWithDatabase(FastAPI):
    redis: aioredis.Redis = None


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


# Client_{id}_limit -> int
async def fetch_client_from_db(id: int):
    client_limit = await app.redis.get(f"Client_{id}_limit")
    
    if not client_limit:
        return {}
    
    result = {}
    result["id"] = id
    result["limite"] = int(client_limit)

    return result


# Client_{id}_balance -> json{valor: int}
async def credit_balance_in_db(cliente_id: int, change_value: int):
    async with app.redis.pipeline() as pipe:
        await pipe.incr(f"Client_{cliente_id}_balance", change_value)
        res = await pipe.execute()
        result = {}
        result["valor"] = res[0]
        return result


async def debit_balance_in_db(cliente_id: int, change_value: int, limit: int):
    tentativas = 0  
    while tentativas < 3:
        try:
            async with app.redis.pipeline(transaction=True) as pipe:
                await pipe.watch(f"Client_{cliente_id}_balance")
                current_balance = await pipe.get(f"Client_{cliente_id}_balance")
                
                if current_balance is None:
                    raise ValueError("Saldo não encontrado")
                
                current_balance = int(current_balance.decode())
                new_value = current_balance - change_value
                
                if new_value < -limit:
                    raise HTTPException(status_code=422, detail="Limite de crédito ultrapassado")

                pipe.multi()
                await pipe.set(f"Client_{cliente_id}_balance", new_value)
                
                res = await pipe.execute()

                if res:
                    result = {}
                    result["valor"] = res[0]
                    return result
                else:
                    raise aioredis.WatchError
        except aioredis.WatchError:
                tentativas = tentativas + 1
                continue
        

# Transaction -> json{id: int, valor: int, tipo: str, descricao: str, realizada_em: str}
async def create_transaction_in_db(cliente_id: int, transaction: TransactionInput):
    current_statement = await app.redis.get(f"Client_{cliente_id}_statement")
    current_statement = json.loads(current_statement) if current_statement else []
    transaction_to_store = {**transaction.dict(), "realizada_em": datetime.datetime.now().isoformat()}

    if not current_statement:
        new_statement = [transaction_to_store]
        await app.redis.set(f"Client_{cliente_id}_statement", json.dumps(new_statement))
        return

    new_statement = [*current_statement, transaction_to_store][-10:][::-1]
    await app.redis.set(f"Client_{cliente_id}_statement", json.dumps(new_statement))


async def get_statement_from_db(cliente_id: int):
    current_statement = await app.redis.get(f"Client_{cliente_id}_statement")
    current_statement = json.loads(current_statement) if current_statement else []
    if not current_statement:
        return []

    result = [
        {
            "valor": t['valor'],
            "tipo": t['tipo'],
            "descricao": t['descricao'],
            "realizada_em": t['realizada_em']
        } for t in current_statement
    ]

    return result


async def get_balance_from_db(cliente_id: int):
    async with app.redis.pipeline() as pipe: 
        await pipe.get(f"Client_{cliente_id}_balance")
        current_balance = (await pipe.execute())[0]
        result = {}
        result["valor"] = int(current_balance) if current_balance else 0
        return result


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
                realizada_em=t['realizada_em']
            )
            for t in statement
        ]
    )


async def start_connection():
    db_host = os.getenv("DB_HOSTNAME", "localhost")
    while True:
        try:
            app.redis = await aioredis.from_url(f"redis://{db_host}")
            break
        except ConnectionError as e:
            print("Database connection error, will sleep for 5 seconds:", e)
            await asyncio.sleep(5)


async def init_clients():
    clients = [
        (1, 1000 * 100),
        (2, 800 * 100),
        (3, 10000 * 100),
        (4, 100000 * 100),
        (5, 5000 * 100)
    ]
    for client_id, limit in clients:
        await app.redis.set(f"Client_{client_id}_limit", limit)
        await app.redis.set(f"Client_{client_id}_balance", 0)
        await app.redis.set(f"Client_{client_id}_statement", "[]")
    

@app.on_event("startup")
async def _startup():
    await start_connection()
    await init_clients()
    print("Starting up...")

app.on_event("shutdown")


async def _shutdown():
    if app.redis:
        await app.redis.close()
    print("Shutting down...")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9999, lifespan="on")
