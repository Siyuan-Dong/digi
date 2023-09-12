import digi
import digi.on as on
import digi.util as util

mock_gvr = "mock.digi.dev/v1/lamps"


# mock status
@on.mount
def do_mock_status(pv, mount):
    mock = get_mock(mount)
    power_attr, bright_attr = "control.power.status", "control.brightness.status"
    power, bright = util.get(mock, f"spec.{power_attr}"), \
                    util.get(mock, f"spec.{bright_attr}")
    old_power, old_bright = util.get(pv, power_attr), \
                            util.get(pv, bright_attr)
    util.update(pv, power_attr, power)
    util.update(pv, bright_attr, bright)
    if power != old_power or bright != old_bright:
        report()


def get_mock(mount):
    mocks = mount.get(mock_gvr, {})
    assert len(mocks) <= 1, "at most one mock allowed"
    for _, mock in mocks.items():
        return mock
    return None


@on.control
def do_control(pv, mount):
    mock = get_mock(mount)
    power = util.get(pv, "control.power.intent")
    bright = util.get(pv, "control.brightness.intent")
    util.update(mock, "spec.control.power.intent", power)
    util.update(mock, "spec.control.brightness.intent", bright)


@on.control
@on.meta("auto_brightness")
def do_auto_brightness(pv, meta):
    if meta.get("auto_brightness", False):
        bright = 0.0
        records = list(digi.pool.query('power=="on" | avg(brightness)'))
        if len(records) > 0:
            bright = round(records[0]["avg"], 2)
        bright = max(bright, 0.1)
        util.update(pv, "control.brightness.intent", bright)
        digi.rc.do_not_skip()


def report():
    model = digi.rc.view()
    power, brightness = util.get(model, "control.power.status"), \
                        util.get(model, "control.brightness.status")
    wattage = util.get(model, "meta.wattage")
    watt = 0 if power != "on" or wattage is None else round(wattage * brightness, 1)
    digi.pool.load(
        [{
            "power": power,
            "brightness": brightness,
            "watt": watt,
        }]
    )


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
