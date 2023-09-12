import digi
from digi import on, util

ship_gvr = "example.digi.dev/v1/ships"
ships = set()

@on.mount
def handle_ship(mount):
    global ships
    new_ships = set()
    for ship, _ in mount.get(ship_gvr, {}).items():
        ship = util.trim_default_space(ship)
        new_ships.add(ship)
    depart_ships = ships.difference(new_ships)
    arrive_ships = new_ships.difference(ships)
    ships = new_ships
    records = [{"name": name, "dock": False} for name in depart_ships] \
              + [{"name": name, "dock": True} for name in arrive_ships]
    digi.pool.load(records)

if __name__ == '__main__':
    digi.run()
