import digi
from digi import on, util

"""Occupancy sensor reads from mock."""

mock_gvr = "mock.digi.dev/v1/occupancies"


@on.mount
def do_mock(mount):
    mocks, motion = mount.get(mock_gvr, {}), None
    assert len(mocks) <= 1, "at most one mock allowed"
    for _, mock in mocks.items():
        motion = util.get(mock, "spec.obs.motion_detected")
    if motion is not None:
        digi.model.patch("obs.motion_detected", motion)


@on.obs("motion_detected")
def do_obs(sv):
    digi.pool.load([{"motion": sv}])


# Ring motion sensor
...

if __name__ == '__main__':
    digi.run()
