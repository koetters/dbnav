from dbnav.control.control import Control
from dbnav.storage import Storage
from dbnav.dbcf import DBContextFamily


class IndexControl(Control):

    def __init__(self):
        self.name = "index"
        self.slots = {}

    def render(self):

        views = []

        views.append({
            "cmd": "set_index_view",
            "args": {
                "slot": "mainView",
                "template": "script#index_base_template",
                "data": self.index_view_data(),
            }
        })

        return views

    # View Updates
    def index_view_data(self):
        bindings = Storage.ls()
        return bindings

    # Actions
    def create_binding(self, form):

        pcf = DBContextFamily(form["user"], form["password"], form["host"], form["database"])
        pcf.load_contents()
        Storage.write(pcf, form["name"] if form["name"] else form["database"])


