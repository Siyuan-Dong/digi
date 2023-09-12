import digi
from digi import util, on
import random


def report():
    digi.pool.load([{
        "spl": random.random() * 120,
        "unit": "db",
    }])


loader = util.Loader(load_fn=report)


@on.meta
def do_meta(meta):
    i = meta.get("report_interval", -1)
    if i < 0:
        digi.logger.info("Stop loader")
        loader.stop()
    else:
        loader.reset(i)
        loader.start()


if __name__ == '__main__':
    digi.run()
