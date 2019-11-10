import os
import random
import asyncio

from gloop.channels import Channel
from gloop.channels.redis import RedisChannel as channel_factory

from gloop import transform_loop

REDIS_ADDRESS_KEY = 'REDIS_ADDRESS'
DEFAULT_REDIS_ADDRESS = 'redis://localhost:6379'

WAITING_LIST_CHANNEL_NAME_KEY = 'WAITING_LIST_CHANNEL_NAME'
DEFAULT_WAITING_LIST_CHANNEL_NAME = 'waiting_list'

NEW_MATCHES_CHANNEL_NAME_KEY = 'NEW_MATCHES_CHANNEL_NAME'
DEFAULT_NEW_MATCHES_CHANNEL_NAME = 'new_matches'

MATCH_SIZE_KEY = 'MATCH_SIZE'
DEFAULT_MATCH_SIZE = 2


async def collect_players_loop(
        waiting_list_channel: Channel,
        new_match_channel: Channel,
        match_size: int):

    buffer = list()

    await waiting_list_channel.open()
    await new_match_channel.open()

    await transform_loop(
        collect_players(buffer, match_size),
        waiting_list_channel,
        new_match_channel
    )


def collect_players(buffer, match_size):

    async def _collect(player):
        buffer.append(player)
        if len(buffer) == match_size:
            match_id = generate_match_id()
            new_match_message = match_id + ' '
            new_match_message += ' '.join(buffer)
            buffer.clear()
            print(new_match_message)
            return new_match_message

    return _collect


def generate_match_id():
    return str(random.randint(100000, 999999))


if __name__ == '__main__':

    print(os.environ.get(REDIS_ADDRESS_KEY))

    REDIS_ADDRESS = os.environ.get(
        REDIS_ADDRESS_KEY,
        DEFAULT_REDIS_ADDRESS
    )

    WAITING_LIST_CHANNEL_NAME = os.environ.get(
        WAITING_LIST_CHANNEL_NAME_KEY,
        DEFAULT_WAITING_LIST_CHANNEL_NAME
    )

    NEW_MATCHES_CHANNEL_NAME = os.environ.get(
        NEW_MATCHES_CHANNEL_NAME_KEY,
        DEFAULT_NEW_MATCHES_CHANNEL_NAME
    )

    MATCH_SIZE = os.environ.get(
        MATCH_SIZE_KEY,
        DEFAULT_MATCH_SIZE
    )

    _waiting_list = channel_factory(
        WAITING_LIST_CHANNEL_NAME,
        address=REDIS_ADDRESS
    )
    _new_matches = channel_factory(
        NEW_MATCHES_CHANNEL_NAME,
        address=REDIS_ADDRESS
    )

    asyncio.run(
        collect_players_loop(
            _waiting_list,
            _new_matches,
            MATCH_SIZE
        )
    )
