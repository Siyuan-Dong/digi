import copy
import sys
import time
import threading
import random
from collections import defaultdict

import digi
import digi.on as on
import digi.util as util

DEFAULT_ENTER_PROB = 0.1
DEFAULT_LEAVE_PROB = 0.1
DEFAULT_CLASS = "human"

scene = None


class MultiScene(threading.Thread):
    def __init__(self, objects,
                 refresh_interval=1,
                 scene_prefix="scene_",
                 num_scene=1,
                 ):
        threading.Thread.__init__(self)

        self.cur_objects = dict()
        self.possible_objects = objects
        self.interval = refresh_interval
        self.scene_prefix = scene_prefix
        self.num_scene = num_scene
        self.name_to_class = {
            name: config.get("class", "human")
            for name, config in self.possible_objects.items()
        }

        self._stop_flag = threading.Event()

    def run(self):
        self.gen_scene()

    def stop(self):
        self._stop_flag.set()

    def split_objects(self):

        pass

    def gen_scene(self):
        scenes_temp = dict()
        for i in range(self.num_scene):
            scenes_temp[self.scene_prefix + str(i + 1)] = {
                "objects": list(),
            }

        while not self._stop_flag.is_set():
            scenes = copy.deepcopy(scenes_temp)
            scene_names = list(scenes.keys())
            for o in self.gen_objects():
                name = random.sample(scene_names, 1)[0]
                scenes[name]["objects"].append(o)
            util.check_gen_and_patch_spec(*digi.auri,
                                          spec={
                                              "data": {
                                                  "output": {
                                                      "scenes": scenes,
                                                  }
                                              }
                                          },
                                          gen=sys.maxsize)
            time.sleep(self.interval)

    def gen_objects(self):
        # TBD gen objects from trace
        object_to_enter = dict()
        for name, config in self.possible_objects.items():
            if name in self.cur_objects:
                continue

            prob = config.get("enter_prob", DEFAULT_ENTER_PROB)

            if not happen(prob):
                continue

            object_to_enter[name] = {
                "class": self.name_to_class[name],
                "location": {
                    "x1": random.randint(100, 1000),
                    "x2": random.randint(100, 1000),
                    "w": random.randint(0, 200),
                    "h": random.randint(0, 200),
                },
                "name": name if self.name_to_class[name] == "human" else "misc",
            }

        object_to_leave = list()
        for name, _ in self.cur_objects.items():
            config = self.possible_objects[name]
            prob = config.get("leave_prob", DEFAULT_ENTER_PROB)
            if happen(prob):
                object_to_leave.append(name)

        # merge
        for name in object_to_leave:
            del self.cur_objects[name]

        self.cur_objects.update(object_to_enter)

        return [o for _, o in self.cur_objects.items()]


def happen(prob):
    return random.random() < prob


@on.meta
def init(model):
    global scene
    if scene is not None:
        scene.stop()
    if util.get(model, "meta.pause", False):
        return

    digi.logger.info("init scene")

    seed = util.get(model, "meta.seed", 42)
    scene = MultiScene(objects=util.get(model, "meta.object_config", {}),
                       refresh_interval=util.get(model, "meta.refresh_interval", 1),
                       scene_prefix=util.get(model, "meta.scene_prefix", "scene_"),
                       num_scene=util.get(model, "meta.num_scene", 1))
    random.seed(seed)
    scene.start()

    util.update(model, "data.output.scenes", None)


if __name__ == '__main__':
    digi.run()
