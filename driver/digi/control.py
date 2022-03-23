import digi
import inflection


class Model():
    def get(self):
        return digi.rc.view()

    def patch(self, view):
        _, e = digi.util.patch_spec(digi.g, digi.v, digi.r,
                                    digi.n, digi.ns, view)
        if e != None:
            digi.logger.info(f"patch error: {e}")

    def get_mount(self, group=None,
                  version=None,
                  kind=None,
                  resource=None) -> dict:
        path = "mount"
        if group and version and (kind or resource):
            resource = inflection.pluralize(kind).lower() \
                if resource is None else resource
            path += f".'{group}/{version}/{resource}'"
        return digi.util.get(digi.rc.view(), path)


def create_model():
    return Model()