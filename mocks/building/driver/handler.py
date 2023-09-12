import digi
import digi.on as on
import digi.util as util

from digi import dbox
dbox.init()


@dbox.loop
def event():
    num_human = dbox.event_random.randint(0, 4)
    digi.model.patch("obs.num_human", num_human)


# sim
@on.obs
def do_obs(sv, mounts):
    rooms = mounts.get(util.gvr_of("room"), {})
    names, num_human = list(rooms.keys()), sv.get("num_human", 0)
    if len(names) < 1:
        return
    picked = set(dbox.sim_random.choices(names, k=num_human, weights=tuple(range(1, len(names) + 1))))
    for name, room in rooms.items():
        util.update(room, "spec.obs.human_presence", name in picked)


# log
@on.obs
def log(sv):
    digi.pool.load([sv])


if __name__ == '__main__':
    digi.run()
