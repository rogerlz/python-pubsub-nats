import asyncio
import time
import snappy

from nats.aio.client import Client as NATS
from protob.remote_pb2 import WriteRequest
from protob.types_pb2 import Sample, Label, TimeSeries


def metric(name, value):
    labels = "method=post,type=http"
    plabels = []

    plabels.append(Label(name="__name__", value=name))

    """ TODO: use _validate_labelnames from client_python"""
    for label in labels.split(','):
        k, v = label.split('=')
        plabels.append(Label(name=k, value=v))

    samples = [Sample(
        value=value,
        timestamp=int(time.time())
    )]

    wreq = WriteRequest(timeseries=[TimeSeries(labels=plabels, samples=samples)])

    return wreq.SerializeToString()


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
    await nc.publish("subject", metric("my_custom_metric"))
    await nc.flush()
    await nc.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run(loop))
    finally:
        loop.close()
