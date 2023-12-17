import contextlib
import datetime
import logging
from os import getenv
from typing import Optional

from aiogram.types import ChatMemberUpdated
from sqlalchemy import (BigInteger, func, select, exists)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import (
    AsyncAttrs, AsyncEngine, AsyncSession,
    async_sessionmaker, create_async_engine
)
from sqlalchemy.orm import (DeclarativeBase, Mapped, mapped_column)

DB_URL = getenv("DB_URL", "postgresql+asyncpg://tgcmbot:tgcmbot@localhost:5432/tgcmbot")
engine: AsyncEngine
async_session: async_sessionmaker[AsyncSession]


class Base(AsyncAttrs, DeclarativeBase):
    pass


class MembersHistory(Base):
    __tablename__ = "members_history"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    is_member: Mapped[bool] = mapped_column(default=True)
    fullname: Mapped[Optional[str]] = mapped_column(default=None)
    username: Mapped[Optional[str]] = mapped_column(default=None)
    meta: Mapped[dict] = mapped_column(JSONB, server_default="{}")
    created_dt: Mapped[datetime.datetime] = mapped_column(server_default=func.now())
    updated_dt: Mapped[datetime.datetime] = mapped_column(server_default=func.now())


async def get_stats() -> dict:
    async with async_session() as session:
        stmt = select(MembersHistory.is_member, func.count()).group_by(MembersHistory.is_member)
        result = await session.execute(stmt)
        joined = left = 0

        for row in result.mappings():
            if row["is_member"]:
                joined = row["count"]
            else:
                left = row["count"]

        stmt = select(MembersHistory).order_by(MembersHistory.created_dt).limit(1)
        result = await session.execute(stmt)
        member = result.scalar()

        logging.info(f"[stats]: {left=}, {joined=} since {member.created_dt}")
        return {"joined": joined, "left": left, "since": member.created_dt}


def event2user_meta(event: ChatMemberUpdated, old_meta: dict = None) -> dict:
    new_user_info = {
        "fullname": event.from_user.full_name,
        "username": event.from_user.username
    }

    if old_meta is None:
        return {
            "user_data": event.from_user.model_dump(exclude_none=True),
            "status_history": [event.new_chat_member.status.value],
            "user_history": [new_user_info]
        }

    user_history = old_meta.get("user_history", [])

    if len(user_history) > 0 and user_history[-1] != new_user_info:
        user_history = [*user_history, new_user_info]

    return {
        "user_data": event.from_user.model_dump(exclude_none=True),
        "status_history": [
            *old_meta.get("status_history", []),
            event.new_chat_member.status.value
        ],
        "user_history": user_history
    }


async def add_or_update_member(event: ChatMemberUpdated, joined=True):
    log_name = event.from_user.full_name + (
        f":{event.from_user.username}"
        if event.from_user.username else ""
    )

    async with async_session() as session:
        stmt = select(MembersHistory).where(MembersHistory.user_id == event.from_user.id)
        is_exists = await session.execute(exists(stmt).select())
        if is_exists.scalar():
            logging.info(
                f"[{event.from_user.id}:{log_name}][old_member]: "
                f"{event.old_chat_member.status} -> {event.new_chat_member.status}"
            )
            member = (await session.execute(stmt)).scalar()
            member.is_member = joined
            member.meta = event2user_meta(event, member.meta)
            member.fullname = event.from_user.full_name
            member.username = event.from_user.username
            member.updated_dt = event.date.replace(tzinfo=None)
        else:
            logging.info(
                f"[{event.from_user.id}:{log_name}][new_member]: "
                f"{event.old_chat_member.status} -> {event.new_chat_member.status}"
            )
            member = MembersHistory(
                user_id=event.from_user.id,
                fullname=event.from_user.full_name,
                username=event.from_user.username,
                meta=event2user_meta(event),
                is_member=joined,
                created_dt=event.date.replace(tzinfo=None)
            )
            session.add(member)
        await session.commit()


@contextlib.asynccontextmanager
async def db_init() -> None:
    global engine, async_session
    try:
        engine = create_async_engine(DB_URL)
        async_session = async_sessionmaker(engine)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        yield
    except Exception as _:  # noqa
        await engine.dispose()
