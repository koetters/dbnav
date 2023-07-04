from dbnav.control.main import MainControl


class MasterControl(object):

    def __init__(self):
        self.root = MainControl()

    def execute(self, name, cmd, args):
        ctrl = self.root.find(name)
        getattr(ctrl, cmd)(**args)
        return ctrl.render()

#    def render(self):
