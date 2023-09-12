import digi
import digi.on as on
import digi.util as util
import random

from digi import dbox
dbox.init()


# event
@dbox.loop
def event():
    presence = dbox.random.choice([True, False])
    digi.model.patch("obs.human_presence", presence)


occupancy = random.random() * 2

# sim
@on.obs
def do_obs(sv, mounts):
    global occupancy
    ocs = mounts.get(util.gvr_of("occupancy"), {})
    for _, oc in ocs.items():
        util.update(oc, "spec.obs.motion_detected",
                    sv.get("human_presence", False))
    uds = mounts.get(util.gvr_of("underdesk"), {})
    for _, ud in uds.items():
        util.update(ud, "spec.obs.motion_detected",
                    sv.get("human_presence", False))

    sv["occupancy"] = min([0.1, round(int(sv.get("human_presence", False)) * occupancy * random.random(), 2)])
    digi.pool.load([sv])


if __name__ == '__main__':
    digi.run()
