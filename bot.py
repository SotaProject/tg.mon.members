import asyncio
import logging
import sys
from os import getenv

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import (
    Command, ChatMemberUpdatedFilter,
    IS_MEMBER, IS_NOT_MEMBER
)
from aiogram.types import Message, ChatMemberUpdated

from db import get_stats, db_init, add_or_update_member

TOKEN = getenv("TELEGRAM_TOKEN")
dp = Dispatcher()

ADMIN_ID = getenv("ADMIN_ID", "").split(",")
CHANNEL_ID = getenv("CHANNEL_ID")


@dp.message(Command("stats"))
async def stats_handler(message: Message) -> None:
    if str(message.chat.id) not in ADMIN_ID:
        await message.answer("who are u?")
        return

    stats = await get_stats()
    await message.answer(
        "left: {left}\njoined: {joined}\nsince: {since}".format(**stats)
    )


@dp.chat_member(ChatMemberUpdatedFilter(IS_MEMBER >> IS_NOT_MEMBER))
async def on_user_leave(event: ChatMemberUpdated):
    if str(event.chat.id) != CHANNEL_ID:
        return
    await add_or_update_member(event, False)


@dp.chat_member(ChatMemberUpdatedFilter(IS_NOT_MEMBER >> IS_MEMBER))
async def on_user_join(event: ChatMemberUpdated):
    if str(event.chat.id) != CHANNEL_ID:
        return
    await add_or_update_member(event)


async def run() -> None:
    async with db_init():
        bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
        await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(run())
