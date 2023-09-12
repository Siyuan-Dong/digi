import digi
import digi.on as on
import digi.util as util


@on.control
def do_control(sv):
    p = sv.get("power", {})
    p_old_status = p.get("status")
    if "intent" in p:
        p["status"] = p["intent"]

    if p.get("status") != p_old_status:
        report()


def report():
    model = digi.rc.view()
    power = util.get(model, "control.power.status")
    wattage = util.get(model, "meta.wattage")
    unit = util.get(model, "meta.unit")
    watt = 0 if power != "on" or wattage is None else wattage
    digi.pool.load(
        [{
            "power": power,
            unit: watt,
        }]
    )


if __name__ == '__main__':
    digi.run()
