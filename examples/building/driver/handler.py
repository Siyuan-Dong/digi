import digi
from digi import on, util

room_gvr = "example.digi.dev/v1/rooms"

# @on.mount
# def update_occupancy_flow(proc_view):
#     rooms = util.get(proc_view, f"mount.'{room_gvr}'", {})
#     num_room = len(rooms)
#     if num_room == 0:
#         digi.util.update(proc_view, "egress.occupancy.pause", True)
#     else:
#         digi.util.update(proc_view, "egress.occupancy.pause", False)
#         digi.util.update(proc_view, "egress.occupancy.flow",
#                          f"sort -r event_ts | head | "
#                          f"occupancy > 0.0 | count := count() | "
#                          f"occupancy:=cast(count, <float64>)/{len(rooms)} |"
#                          f"cut occupancy")


# @on.pool(in_flow="")
# def gen_occupancy(records):
#     digi.logger.info(f"Records: {records}")
#     if len(records) == 0:
#         return

if __name__ == '__main__':
    digi.run()
