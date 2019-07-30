from dbnav.control.control import Control
from dbnav.control.index import IndexControl
from dbnav.control.navigator import NavigatorControl
from dbnav.control.editor import EditorControl


class MainControl(Control):

    def __init__(self):
        self.name = "main"
        self.slots = {"A": None}

    def insert_index_view(self):
        self.slots["A"] = IndexControl()

    def insert_navigate_view(self, pcf_name):
        self.slots["A"] = NavigatorControl(pcf_name)

    def insert_edit_view(self, pcf_name):
        self.slots["A"] = EditorControl(pcf_name)

