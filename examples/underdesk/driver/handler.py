import digi
from digi import on
import random


def report():
    motion = random.choice([True, False])
    digi.model.patch({
        "obs": {
            "motion_detected": motion
        }
    })
    digi.pool.load([{"motion": motion}])


def make_load_interval(avg_t):
    def fn() -> int:
        min_, max_ = int(avg_t / 2), int(avg_t * 2)
        return random.randint(min_, max_)

    return fn


loader = digi.util.Loader(load_fn=report)


@on.meta
def do_meta(meta):
    global loader
    i = meta.get("report_interval", -1)
    if i < 0:
        loader.stop()
    else:
        loader.stop()
        loader = digi.util.Loader(
            load_fn=report,
            load_interval_fn=make_load_interval(i),
        )
        loader.start()


if __name__ == '__main__':
    digi.run()
