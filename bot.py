import asyncio
import datetime
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
bot: Bot

ADMIN_IDS = getenv("ADMIN_IDS", "").split(",")
CHANNEL_ID = int(getenv("CHANNEL_ID"))


@dp.message(Command("stats", "stats_1h", "stats_6h", "stats_12h", "stats_24h"))
async def stats_handler(message: Message) -> None:
    since = None

    if "_" in message.text:
        since = datetime.datetime.utcnow() - datetime.timedelta(
            hours=int(message.text.split("_")[1].replace("h", ""))
        )

    if (
        str(message.from_user.id) not in ADMIN_IDS and
        message.from_user.id not in [
            a.user.id for a in
            await bot.get_chat_administrators(CHANNEL_ID)
        ]
    ):
        await message.answer("who are u?")
        return

    stats = await get_stats(since)
    await message.answer(
        "left: {left}\njoined: {joined}\nsince: {since} [UTC]".format(**stats)
    )


@dp.chat_member(ChatMemberUpdatedFilter(IS_MEMBER >> IS_NOT_MEMBER))
async def on_user_leave(event: ChatMemberUpdated):
    if event.chat.id != CHANNEL_ID:
        return
    await add_or_update_member(event, False)


@dp.chat_member(ChatMemberUpdatedFilter(IS_NOT_MEMBER >> IS_MEMBER))
async def on_user_join(event: ChatMemberUpdated):
    if event.chat.id != CHANNEL_ID:
        return
    await add_or_update_member(event)


async def run() -> None:
    global bot
    async with db_init():
        bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
        await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(run())
