

class Control(object):

    def __init__(self, name, keys):
        self.name = name
        self.slots = {k: None for k in keys}

    def find(self, name):

        if self.name == name:
            return self
        for key in self.slots:
            ctrl = self.slots[key].find(name)
            if ctrl is not None:
                return ctrl
        return None

    def render(self):
        views = []
        for ctrl in self.slots.values():
            views += ctrl.render()
        return views