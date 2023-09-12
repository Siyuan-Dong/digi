import digi
import digi.on as on
import digi.util as util
from digi import dbox
import time


@on.control(cond=dbox.managed)
def do_control(sv, meta):
    p, b = sv.get("power", {}), sv.get("brightness", {})
    p_old_status, b_old_status = p.get("status"), b.get("status")
    if "intent" in p:
        p["status"] = p["intent"]

    if "intent" in b:
        if p.get("status", "off") == "on":
            b["status"] = b["intent"]
        else:
            b["status"] = 0

    # report only when status has change
    if p.get("status") != p_old_status \
            or b.get("status") != b_old_status:
        time.sleep(meta.get("actuation_delay", 0))
        report()


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
    i, managed = meta.get("report_interval", -1), \
                 meta.get("managed", False)
    if i < 0 or managed:
        digi.logger.info("Stop loader")
        loader.stop()
    else:
        loader.reset(i)
        loader.start()


if __name__ == '__main__':
    digi.run()
