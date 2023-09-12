import digi
import digi.on as on

import digi.util as util
from digi.util import deep_get, deep_set, deep_set_all

"""
Home has the following capabilities:
- Mode -> room mode 
- Room obs.objects -> home.objects
"""

room_gvr = "mock.digi.dev/v1/rooms"

mode_config = {
    "away": {
        "rooms": {
            "mode": "sleep"
        }
    },
    "sleep": {
        "rooms": {
            "mode": "sleep"
        }
    },
    "work": {
        "rooms": {
            "mode": "work"
        }
    },
    "adaptive": {
        "rooms": {
            "mode": "adaptive"
        }
    },
    "emergency": {},
}

# TBD brightness of last week
brightness = digi.pool.query("avg(brightness)")
digi.logger.info(f"DEBUG {brightness}")

@on.mount
def do_home_status(parent, mounts):
    home = parent

    mode = deep_get(home, "control.mode.intent")
    if mode is None:
        return

    # handle rooms
    rooms = mounts.get(room_gvr, {})

    # update home's mode
    if all(deep_get(r, "spec.control.mode.status") ==
           deep_get(mode_config, f"{mode}.rooms.mode")
           for _, r in rooms.items()):
        deep_set(home, "control.mode.status", mode)
    else:
        deep_set(home, "control.mode.status", "undef")

    # update observations
    obs_rooms = dict()
    for n, r in rooms.items():
        objects = deep_get(r, "spec.obs.objects", {})
        obs_rooms[util.simple_name(n)] = {
            "objects": [{"class": o.get("class", None),
                         "name": o.get("name", None)} for o in objects],
            "brightness": deep_get(r, "spec.control.brightness.status"),
            "mode": deep_get(r, "spec.control.mode.status"),
        }
    deep_set(home, "obs.rooms", obs_rooms)


@on.mount
@on.control
def do_home_mode(parent, mounts):
    mode = deep_get(parent, "control.mode.intent")
    room_mode = deep_get(mode_config, f"{mode}.rooms.mode")

    rooms = mounts.get(room_gvr, {})
    deep_set_all(rooms, "spec.control.mode.intent", room_mode)


def load():
    model = digi.rc.view()
    record = util.get(model, "obs", {})
    digi.pool.load([record])


loader = util.Loader(load_fn=load)


@on.meta
def do_meta(meta):
    i = meta.get("load_interval", -1)
    if i < 0:
        digi.logger.info("Stop loader")
        loader.stop()
    else:
        loader.reset(i)
        loader.start()


if __name__ == '__main__':
    digi.run()
