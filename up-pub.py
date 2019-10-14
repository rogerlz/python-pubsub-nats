import asyncio
from nats.aio.client import Client as NATS


async def run(loop):
    nc = NATS()

    async def error_cb(e):
        print(e)

    async def closed_cb():
        print("connection to nats is closed")

    async def reconnected_cb():
        print("reconnected to nats")

    options = {
        "servers": ["nats://127.0.0.1:4222"],
        "io_loop": loop,
        "error_cb": error_cb,
        "closed_cb": closed_cb,
        "reconnected_cb": reconnected_cb
    }

    try:
        await nc.connect(**options)
    except Exception as e:
        print(e)

    print("connected to nats")
    await nc.publish("subject", "message".encode())
    await nc.flush()
    await nc.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run(loop))
    finally:
        loop.close()
