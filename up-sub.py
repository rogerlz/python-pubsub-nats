import asyncio
import signal
from nats.aio.client import Client as NATS


async def run(loop):
    nc = NATS()

    async def error_cb(e):
        print(e)

    async def closed_cb():
        print("connection to nats is closed")
        await asyncio.sleep(0.1, loop=loop)
        loop.stop()

    async def reconnected_cb():
        print("reconnected to nats")

    async def subscribe_handler(msg):
        data = msg.data.decode()
        print("received a message on '{subject} {reply}': {data}".format(
            subject=msg.subject, reply=msg.reply, data=data))

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

    def signal_handler():
        if nc.is_closed:
            return
        print("disconnecting from nats")
        loop.create_task(nc.close())

    for sig in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, sig), signal_handler)

    await nc.subscribe("subject", "workers", subscribe_handler)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    try:
        loop.run_forever()
    finally:
        loop.close()
