from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Tuple, Optional
from sqlalchemy import select
from sqlalchemy.engine import Result
from yahoo_finance_api2 import share
from yahoo_finance_api2.exceptions import YahooFinanceError
from datetime import datetime
from fastapi import HTTPException

import api.models.task as task_model
import api.schemas.task as task_schema


async def create_task(
        db: AsyncSession, task_create: task_schema.TaskCreate
) -> task_model.Task:
    task = task_model.Task(**task_create.dict())
    db.add(task)
    await db.commit()
    await db.refresh(task)
    return task


async def get_tasks_with_done(db: AsyncSession) -> List[Tuple[int, str, bool]]:
    result: Result = await (
        db.execute(
            select(
                task_model.Task.id,
                task_model.Task.title,
                task_model.Done.id.isnot(None).label("done"),
            ).outerjoin(task_model.Done)
        )
    )
    return result.all()


async def get_task(db: AsyncSession, task_id: int) -> Optional[task_model.Task]:
    result: Result = await db.execute(
        select(task_model.Task).filter(task_model.Task.id == task_id)
    )
    task: Optional[Tuple[task_model.Task]] = result.first()
    return task[0] if task is not None else None


async def update_task(
        db: AsyncSession, task_create: task_schema.TaskCreate, original: task_model.Task
) -> task_model.Task:
    original.title = task_create.title
    db.add(original)
    await db.commit()
    await db.refresh(original)
    return original


async def delete_task(db: AsyncSession, original: task_model.Task) -> None:
    await db.delete(original)
    await db.commit()


def get_last_stock_data(stock_cd: str):
    stock_str = stock_cd + ".T"
    stock_share = share.Share(stock_str)
    data = None

    try:
        data = stock_share.get_historical(share.PERIOD_TYPE_DAY, 1, share.FREQUENCY_TYPE_DAY, 1)
    except YahooFinanceError as e:
        print(e.message)
        raise HTTPException(status_code=500, detail="yahoo finance error")

    print('close', type(data["close"][-1]))
    # 直前の終値
    last_open = data["open"][-1]
    last_close = data["close"][-1]
    last_data = {
        "close": data["close"][-1],
        "open": data["open"][-1],
        "high": data["high"][-1],
        "low": data["low"][-1],
    }
    return last_data

