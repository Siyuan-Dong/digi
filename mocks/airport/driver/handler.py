import digi
from digi import on, util

plane_gvr = "mock.digi.dev/v1/planes"
planes = set()

@on.mount
def handle_flight(mount):
    global planes
    new_planes = set()
    for plane, _ in mount.get(plane_gvr, {}).items():
        new_planes.add(util.trim_default_space(plane))
    depart_planes, arrive_planes = planes.difference(new_planes), new_planes.difference(planes)
    records = [{"name": name, "park": False} for name in depart_planes] \
              + [{"name": name, "park": True} for name in arrive_planes]
    digi.pool.load(records)
    planes = new_planes

if __name__ == '__main__':
    digi.run()
