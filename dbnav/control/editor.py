from dbnav.control.control import Control
from dbnav.storage import Storage


mysql_main_types = ["bool", "date", "int", "varchar"]
mysql_types = [
    {
        "name": "Numeric",
        "types": ["tinyint", "smallint", "mediumint", "bigint", "decimal", "float", "double", "real", "bit", "serial"]
    },
    {
        "name": "Date and time",
        "types": ["datetime", "timestamp", "time", "year"]
    },
    {
        "name": "String",
        "types": ["char", "tinytext", "text", "mediumtext", "longtext", "binary", "varbinary",
                  "tinyblob", "mediumblob", "blob", "longblob", "enum", "set"]
    },
    {
        "name": "Spatial",
        "types": ["geometry", "point", "linestring", "polygon", "multipoint",
                  "multilinestring", "multipolygon", "geometrycollection"]
    },
    {
        "name": "JSON",
        "types": ["json"]
    },
]


class EditorControl(Control):

    def __init__(self, pcf_name):

        self.name = "editor"
        self.slots = {}
        self.pcf_name = pcf_name
        self.current_sort = None
        self.mva_form1 = None
        self.mva_form2 = None
        self.mva_form1_data = {}
        self.mva_form2_data = {}

    def render(self):

        views = []
        pcf = Storage.read(self.pcf_name)

        views.append({
            "cmd": "set_edit_view",
            "args": {
                "slot": "mainView",
                "template": "script#edit_base_template",
                "data": {},
            }
        })

        views.append({
            "cmd": "set_pcf_sorts_view",
            "args": {
                "slot": "sortsMenu",
                "template": "script#pcf_sorts_template",
                "data": self.pcf_sorts_view_data(pcf, self.current_sort),
            }
        })

        if self.current_sort is None:
            views.append({
                "cmd": "set_sort_api_view",
                "args": {
                    "slot": "sortView",
                    "template": None,
                    "data": None,
                }
            })

        else:
            views.append({
                "cmd": "set_sort_api_view",
                "args": {
                    "slot": "sortView",
                    "template": "script#sort_api_template",
                    "data": self.pcf_api_view_data(pcf, self.current_sort, self.mva_form2),
                }
            })

            if self.mva_form1 == "derived":
                views.append({
                    "cmd": "set_derived_mva_form",
                    "args": {
                        "slot": "mvaForm1",
                        "template": "script#derived_mva_form_template",
                        "data": self.derived_mva_form_data(pcf, self.mva_form1_data),
                    }
                })

            if self.mva_form2 == "derived":
                views.append({
                    "cmd": "set_derived_mva_form",
                    "args": {
                        "slot": "mvaForm2",
                        "template": "script#derived_mva_form_template",
                        "data": self.derived_mva_form_data(pcf, self.mva_form2_data),
                    }
                })

            elif self.mva_form2 == "fk":
                views.append({
                    "cmd": "set_foreign_key_form",
                    "args": {
                        "slot": "mvaForm2",
                        "template": "script#foreign_key_form_template",
                        "data": self.foreign_key_form_data(pcf, self.mva_form2_data),
                    }
                })

        return views

    # View Updates
    def pcf_sorts_view_data(self, pcf, sort):
        sort_list = [{"name": s, "selected": s == sort} for s in pcf.sorts]
        return {"sorts": sort_list}

    def pcf_api_view_data(self, pcf, sort, form2):

        mva_list = {
            "print_function": pcf.output[sort],
            "properties": [],
            "relations": [],
        }

        for mva_id, mva in pcf.mvas.items():

            if sort not in mva.sort:
                continue

            rcontexts = {context_id: context for context_id, context in pcf.rcontexts.items()
                         if context.mva_id == mva_id}

            assert(len(rcontexts) <= 1)
            if not rcontexts:
                current_cls = ""
            else:
                rcontext = next(iter(rcontexts.values()))
                current_cls = rcontext.get_class()

            context_classes = [""] + Storage.available_context_classes.get(mva.datatype, [])

            mva_info = {
                "id": mva_id,
                "name": mva.name,
                "class": mva.get_class(),
                "datatype": mva.datatype,
                "sqldef": mva.sqldef,
                "params": [{"sort": mva.sort[i], "role": mva.roles[i]} for i in range(len(mva.sort))],
                "scales": [{"class": cls, "selected": current_cls is cls} for cls in context_classes],
            }

            if len(mva.sort) == 1:
                mva_list["properties"].append(mva_info)
            else:
                mva_list["relations"].append(mva_info)

        mva_list["relations"].sort(key=lambda x: "" if x["class"] == "ForeignKey" else x["name"])
        return {"mvas": mva_list, "forms2": [{"id": "derived", "name": "Derived", "selected": form2 == "derived"},
                                             {"id": "fk", "name": "Foreign Key", "selected": form2 == "fk"}]}

    def derived_mva_form_data(self, pcf, form_data):
        params = []
        for i in range(1, int(form_data["nargs"])+1):
            sort_key = "sort{0}".format(i)
            role_key = "role{0}".format(i)
            params.append({
                "sort_key": sort_key,
                "role_key": role_key,
                "sort": form_data[sort_key],
                "role": form_data[role_key],
            })
        return {
            "datatype": form_data["datatype"],
            "main_types": mysql_main_types,
            "datatype_groups": mysql_types,
            "name": form_data["name"],
            "sqldef": form_data["sqldef"],
            "sorts": [{"name": s} for s in pcf.sorts],
            "params": params,
            "unary": int(form_data["nargs"]) == 1
        }

    def foreign_key_form_data(self, pcf, form_data):
        params = []
        for i in [1, 2]:
            sort_key = "sort{0}".format(i)
            column_key = "column{0}".format(i)
            role_key = "role{0}".format(i)

            columns = []
            for mva_id, mva in pcf.mvas.items():
                if mva.get_class() == "DBColumn":
                    if form_data[sort_key] in mva.sort:
                        column_info = {"id": mva_id, "name": mva.name}
                        columns.append(column_info)

            params.append({
                "sort_key": sort_key,
                "column_key": column_key,
                "role_key": role_key,
                "sort": form_data[sort_key],
                "column": form_data[column_key],
                "role": form_data[role_key],
                "columns": columns,
            })
        return {
            "name": form_data["name"],
            "sorts": [{"name": s} for s in pcf.sorts],
            "params": params,
        }

    # Actions
    def select_sort(self, sort):
        self.current_sort = sort
        self.mva_form1 = "derived"
        self.mva_form2 = "derived"
        self.mva_form1_data = {
            "datatype": None,
            "name": "",
            "sqldef": "",
            "sort1": sort,
            "role1": "ATTR",
            "nargs": "1",
        }
        self.mva_form2_data = {
            "datatype": None,
            "name": "",
            "sqldef": "",
            "sort1": sort,
            "role1": "ARG1",
            "sort2": sort,
            "role2": "ARG2",
            "nargs": "2",
        }

    def select_mva_form2(self, form_id):
        if form_id == "derived":
            self.mva_form2 = "derived"
            self.mva_form2_data = {
                "datatype": None,
                "name": "",
                "sqldef": "",
                "sort1": self.current_sort,
                "role1": "ARG1",
                "sort2": self.current_sort,
                "role2": "ARG2",
                "nargs": "2",
            }

        elif form_id == "fk":
            self.mva_form2 = "fk"
            self.mva_form2_data = {
                "name": "",
                "sort1": self.current_sort,
                "column1": None,
                "role1": "ARG1",
                "sort2": self.current_sort,
                "column2": None,
                "role2": "ARG2",
            }

    def update_form1(self, form_data):
        self.mva_form1_data = form_data
        if self.mva_form1 == "derived":
            assert(int(self.mva_form1_data["nargs"]) == 1)

    def update_form2(self, form_data):
        self.mva_form2_data = form_data
        #  TODO: the code below (handling nargs change) probably works, but doesn't look good
        if self.mva_form2 == "derived":
            nargs = int(self.mva_form2_data["nargs"])
            nparams = 0
            while True:
                if "sort{0}".format(nparams+1) in self.mva_form2_data:
                    nparams += 1
                else:
                    break
            if nparams != nargs:
                #  check that nargs is in the allowed range
                if nargs > 5:
                    assert(nparams == 5)
                    self.mva_form2_data["nargs"] = str(nparams)
                elif nargs < 2:
                    assert(nparams == 2)
                    self.mva_form2_data["nargs"] = str(nparams)
                #  add or delete a parameter to match nargs
                elif nargs < nparams:
                    assert(nparams == nargs+1)
                    del self.mva_form2_data["sort{0}".format(nparams)]
                    del self.mva_form2_data["role{0}".format(nparams)]
                elif nparams < nargs:
                    assert(nargs == nparams+1)
                    self.mva_form2_data["sort{0}".format(nargs)] = self.current_sort
                    self.mva_form2_data["role{0}".format(nargs)] = "ARG{0}".format(nargs)

    def create_mva1(self, form_data):
        pcf = Storage.read(self.pcf_name)
        if self.mva_form1 == "derived":
            # form = self.mva_form1_data

            # assert(form["datatype"] == form_data["datatype"])
            # assert(form["name"] == form_data["name"])
            # assert(form["sqldef"] == form_data["sqldef"])
            # assert(form["nargs"] == form_data["nargs"] and form["nargs"] == str(1))
            # assert(form["sort1"] == form_data["sort1"])
            # assert(form["role1"] == form_data["role1"])

            if not (form_data["name"] and form_data["sqldef"] and form_data["datatype"] and form_data["role1"]):
                return
            if form_data["sort1"] not in pcf.sorts:
                return

            sort = [form_data["sort{0}".format(i+1)] for i in range(int(form_data["nargs"]))]
            roles = [form_data["role{0}".format(i+1)] for i in range(int(form_data["nargs"]))]
            pcf.add_mva(form_data["name"], sort, form_data["datatype"], form_data["sqldef"], roles)

            self.mva_form1_data = {
                "datatype": None,
                "name": "",
                "sqldef": "",
                "sort1": self.current_sort,
                "role1": "ATTR",
                "nargs": "1",
            }
        Storage.write(pcf, self.pcf_name)

    def create_mva2(self, form_data):
        pcf = Storage.read(self.pcf_name)
        if self.mva_form2 == "derived":
            # form = self.mva_form2_data

            # assert(form["datatype"] == form_data["datatype"])
            # assert(form["name"] == form_data["name"])
            # assert(form["sqldef"] == form_data["sqldef"])
            # assert(form["nargs"] == form_data["nargs"])

            # for i in range(1,int(form["nargs"])+1):
            #     assert(form["sort{0}".format(i)] == form_data["sort{0}".format(i)])
            #     assert(form["role{0}".format(i)] == form_data["role{0}".format(i)])

            if not (form_data["name"] and form_data["sqldef"] and form_data["datatype"]):
                return
            for i in range(1, int(form_data["nargs"])+1):
                if form_data["sort{0}".format(i)] not in pcf.sorts:
                    return
                if not form_data["role{0}".format(i)]:
                    return

            sort = [form_data["sort{0}".format(i+1)] for i in range(int(form_data["nargs"]))]
            roles = [form_data["role{0}".format(i+1)] for i in range(int(form_data["nargs"]))]
            pcf.add_mva(form_data["name"], sort, form_data["datatype"], form_data["sqldef"], roles)

            self.mva_form2_data = {
                "datatype": None,
                "name": "",
                "sqldef": "",
                "sort1": self.current_sort,
                "role1": "ARG1",
                "sort2": self.current_sort,
                "role2": "ARG2",
                "nargs": "2",
            }

        elif self.mva_form2 == "fk":
            # form = self.mva_form2_data

            # assert(form["name"] == form_data["name"])
            # assert(form["sort1"] == form_data["sort1"])
            # assert(form["column1"] == form_data["column1"])
            # assert(form["role1"] == form_data["role1"])
            # assert(form["sort2"] == form_data["sort2"])
            # assert(form["column2"] == form_data["column2"])
            # assert(form["role2"] == form_data["role2"])

            if not (form_data["name"] and form_data["column1"] and form_data["column2"]
                    and form_data["role1"] and form_data["role2"]):
                return
            if not (form_data["sort1"] in pcf.sorts and form_data["sort2"] in pcf.sorts):
                return

            roles = [form_data["role1"], form_data["role2"]]
            colname1 = pcf.mvas[form_data["column1"]].name
            colname2 = pcf.mvas[form_data["column2"]].name
            mva_id = pcf.add_foreign_key(form_data["name"], form_data["sort1"], colname1, form_data["sort2"], colname2)
            pcf.mvas[mva_id].roles = roles

            self.mva_form2_data = {
                "name": "",
                "sort1": self.current_sort,
                "column1": None,
                "role1": "ARG1",
                "sort2": self.current_sort,
                "column2": None,
                "role2": "ARG2",
            }
        Storage.write(pcf, self.pcf_name)

    def scale_mva(self, mva_id, context_class):
        pcf = Storage.read(self.pcf_name)
        pcf.scale_mva(mva_id, context_class)
        Storage.write(pcf, self.pcf_name)

    def delete_mva(self, mva_id):
        pcf = Storage.read(self.pcf_name)
        pcf.delete_mva(mva_id)
        Storage.write(pcf, self.pcf_name)

    def set_output_sql(self, sqlterm):
        pcf = Storage.read(self.pcf_name)
        pcf.set_printsql(self.current_sort, sqlterm)
        Storage.write(pcf, self.pcf_name)
