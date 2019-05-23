from dbnav.storage import Storage
from dbnav.graph import Graph, Point
from dbnav.dbcf import DBContextFamily, BooleanScale, DateIntervalScale, PrefixScale
import mysql.connector


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


# class ViewOp(object):
#
#     def __init__(self,cmd,args):
#         self.cmd = cmd;
#         self.args = args;


class Control(object):

    def __init__(self, state):

        self.state = state

    def render(self):

        views = []

        if self.state["main"] == "index":

            views.append({
                "cmd": "set_index_view",
                "args": {
                    "slot": "mainView",
                    "template": "script#index_base_template",
                    "data": self.index_view_data(),
                }
            })

        elif self.state["main"] == "navigate":

            views.append({
                "cmd": "set_navigate_view",
                "args": {
                    "slot": "mainView",
                    "template": "script#navigate_base_template",
                    "data": {},
                }
            })

            views.append({
                "cmd": "set_links_view",
                "args": {
                    "slot": "linksView",
                    "template": "script#links_template",
                    "data": self.links_view_data(self.state["graph"], self.state["current_node"],
                                                 self.state["current_link"]),
                }
            })

            views.append({
                "cmd": "set_graph_view",
                "args": {
                    "graph": self.state["graph"],
                    "current_node": self.state["current_node"],
                }
            })

            views.append({
                "cmd": "set_table_view",
                "args": {
                    "slot": "tableView",
                    "template": "script#result_template",
                    "data": self.table_view_data(self.state["graph"], self.state["current_node"],
                                                 self.state["current_link"]["linkID"]),
                }
            })

            if self.state["current_link_type"] is None:

                views.append({
                    "cmd": "set_sort_label_view",
                    "args": {
                        "slot": "labelView",
                        "template": "script#sort_label_template",
                        "data": self.sort_label_view_data(self.state["graph"], self.state["current_node"]),
                    }
                })

            else:

                rnode_id = self.state["current_link"]["linkID"]
                rnode = self.state["graph"].rnodes[rnode_id]
                #  TODO: while len(endpoints) is indeed the index of the table's mva column,\
                #        a more explicit treatment would be less error-prone
                data = self.label_view_data(self.state["graph"], rnode_id)

                if self.state["current_link_type"] == "DateIntervalScale":
                    views.append({
                        "cmd": "set_interval_label_view",
                        "args": {
                            "slot": "labelView",
                            "template": "script#interval_label_template",
                            "data": data,
                            "label": rnode.label,
                        }
                    })

                elif self.state["current_link_type"] == "PrefixScale":
                    views.append({
                        "cmd": "set_prefix_label_view",
                        "args": {
                            "slot": "labelView",
                            "template": "script#prefix_label_template",
                            "data": data,
                            "label": rnode.label,
                        }
                    })

                elif self.state["current_link_type"] == "BooleanScale":
                    views.append({
                        "cmd": "set_fk_label_view",
                        "args": {
                            "slot": "labelView",
                            "template": "script#fk_label_template",
                            "data": data,
                            "label": rnode.label,
                        }
                    })

        elif self.state["main"] == "edit":

            pcf = Storage.read(self.state["pcf_name"])

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
                    "data": self.pcf_sorts_view_data(pcf, self.state["current_sort"]),
                }
            })

            if self.state["current_sort"] is None:
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
                        "data": self.pcf_api_view_data(pcf, self.state["current_sort"], self.state["mva_form2"]),
                    }
                })

                if self.state["mva_form1"] == "derived":
                    views.append({
                        "cmd": "set_derived_mva_form",
                        "args": {
                            "slot": "mvaForm1",
                            "template": "script#derived_mva_form_template",
                            "data": self.derived_mva_form_data(pcf, self.state["mva_form1_data"]),
                        }
                    })

                if self.state["mva_form2"] == "derived":
                    views.append({
                        "cmd": "set_derived_mva_form",
                        "args": {
                            "slot": "mvaForm2",
                            "template": "script#derived_mva_form_template",
                            "data": self.derived_mva_form_data(pcf, self.state["mva_form2_data"]),
                        }
                    })

                elif self.state["mva_form2"] == "fk":
                    views.append({
                        "cmd": "set_foreign_key_form",
                        "args": {
                            "slot": "mvaForm2",
                            "template": "script#foreign_key_form_template",
                            "data": self.foreign_key_form_data(pcf, self.state["mva_form2_data"]),
                        }
                    })

        return views

#  View Updates

    def index_view_data(self):
        bindings = Storage.ls()
        return bindings

    def links_view_data(self, graph, node_id, selected_link):
        # compute propertyLinks and relationLinks
        properties = {}
        relations = {}
        node = graph.nodes[node_id]

        if node.sort is None:
            return {"propertyLinks": [], "relationLinks": []}

        for link in graph.pcf.neighbors(node.sort):
            context_id = link["linkID"]
            role_id = link["roleID"]
            rcontext = graph.pcf.rcontexts[context_id]
            arity = len(rcontext.sort)
            key = "{0}#{1}".format(role_id, context_id)
            value = {"edgeID": None, "contextID": context_id, "roleID": role_id, "contextName": rcontext.name,
                     "roleName": rcontext.roles[role_id-1], "exists": False, "selected": False}
            if arity == 1:
                value["displayed"] = context_id in node.display
                properties[key] = value
            else:
                relations[key] = value

        for link in graph.neighbors(node_id):
            rnode_id = link["linkID"]
            role_id = link["roleID"]
            rnode = graph.rnodes[rnode_id]
            context_id = rnode.context_id
            rcontext = graph.pcf.rcontexts[context_id]
            arity = len(rcontext.sort)
            key = "{0}#{1}".format(role_id, context_id)
            if arity == 1:
                value = properties[key]
                value["edgeID"] = rnode_id
                value["exists"] = True
                value["selected"] = (selected_link["linkID"] == rnode_id
                                     and selected_link["roleID"] == role_id)

            else:
                selected = (selected_link["linkID"] == rnode_id
                            and selected_link["roleID"] == role_id)
                value = {"edgeID": rnode_id, "contextID": context_id, "roleID": role_id, "contextName": rcontext.name,
                         "roleName": rcontext.roles[role_id-1], "exists": True, "selected": selected}
                if isinstance(relations[key], list):
                    relations[key].append(value)
                else:
                    relations[key] = [value]

        property_links = sorted(iter(properties.values()), key=lambda x: x["contextName"])
        relation_links = []
        for value in relations.values():
            if isinstance(value, list):
                relation_links += value
            else:
                relation_links.append(value)
        relation_links.sort(key=lambda x: (x["contextName"], x["roleName"]))

        return {"propertyLinks": property_links, "relationLinks": relation_links}

    def sort_label_view_data(self, graph, node_id):
        return graph.stats(node_id)

    def label_view_data(self, graph, rnode_id):
        return graph.rstats(rnode_id)

    def table_view_data(self, graph, node_id, rnode_id):

        if rnode_id is None:
            table = graph.extent(node_id)
        else:
            table = graph.rextent(rnode_id)
        #  process header and rows
        header = [{"value": value, "width": 200} for value in table.header]
        rows = [None]*len(table.rows)
        for i, row in enumerate(table.rows):
            rows[i] = [{"value": str(value), "width": 200} for value in row]

        return {"header": header, "rows": rows}

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
                current_scale = ""
            else:
                rcontext = next(iter(rcontexts.values()))
                current_scale = rcontext.scale.get_class()

            scales_list = [""] + Storage.available_scales.get(mva.datatype, [])

            mva_info = {
                "id": mva_id,
                "name": mva.name,
                "class": mva.get_class(),
                "datatype": mva.datatype,
                "sqldef": mva.sqldef,
                "params": [{"sort": mva.sort[i], "role": mva.roles[i]} for i in range(len(mva.sort))],
                "scales": [{"class": s, "selected": current_scale is s} for s in scales_list],
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

    #  Actions

    def insert_index_view(self):
        self.state = {"main": "index"}

    def insert_navigate_view(self, pcf_name):
        pcf = Storage.read(pcf_name)
        graph = Graph(pcf)
        x1 = graph.add_node(None, Point(305, 150))

        self.state = {"main": "navigate", "pcf_name": pcf_name, "graph": graph, "current_node": x1,
                      "current_link": {"linkID": None, "roleID": None}, "current_link_type": None}

    def insert_edit_view(self, pcf_name):
        self.state = {"main": "edit", "pcf_name": pcf_name, "current_sort": None, "mva_form1": None,
                      "mva_form2": None, "mva_form1_data": {}, "mva_form2_data": {}}

    def set_label(self, label):
        if self.state["current_link"]["linkID"] is None:  # object node
            node = self.state["graph"].nodes[self.state["current_node"]]
            node.sort = label
        else:
            rnode = self.state["graph"].rnodes[self.state["current_link"]["linkID"]]
            rnode.label = label

    def set_position(self, node_id, x, y):
        self.state["graph"].set_position(node_id, x, y)

    def merge(self, target_id):
        self.state["graph"].merge(self.state["current_node"], target_id)

    def create_edge(self, context_id, role_id):
        node_id = self.state["current_node"]
        rcontext = self.state["graph"].pcf.rcontexts[context_id]

        endpoints = [None]*len(rcontext.sort)
        endpoints[role_id-1] = node_id
        self.state["graph"].add_rnode(context_id, endpoints)

    def select_edge(self, edge_id, role_id):
        if edge_id is None:
            self.state["current_link"] = {"linkID": None, "roleID": None}
            self.state["current_link_type"] = None
        else:
            rnode = self.state["graph"].rnodes[edge_id]
            rcontext = self.state["graph"].pcf.rcontexts[rnode.context_id]
            scale = rcontext.scale

            self.state["current_link"] = {"linkID": edge_id, "roleID": role_id}
            self.state["current_link_type"] = scale.get_class()

    def remove_edge(self, edge_id, role_id):
        self.state["graph"].remove_rnode(edge_id, role_id)
        self.state["current_link"] = {"linkID": None, "roleID": None}
        self.state["current_link_type"] = None

    def select_node(self, node_id):
        self.state["current_node"] = node_id
        self.state["current_link"] = {"linkID": None, "roleID": None}
        self.state["current_link_type"] = None

    def toggle_display(self, context_id):
        node = self.state["graph"].nodes[self.state["current_node"]]
        if context_id in node.display:
            node.display.remove(context_id)
        else:
            node.display.add(context_id)

    def create_binding(self, form):

        cnx = mysql.connector.connect(user=form["user"], password=form["password"],
                                      host=form["host"], database=form["database"])

        cursor1 = cnx.cursor()
        query1 = "SELECT column_name,table_name,data_type FROM information_schema.columns WHERE table_schema='{0}'"
        cursor1.execute(query1.format(form["database"]))
        rows1 = cursor1.fetchall()
        cursor1.close()

        cursor2 = cnx.cursor()
        query2 = ("SELECT t1.constraint_name, t2.constraint_type, t1.table_name, t1.column_name, "
                  + "t1.referenced_table_name, t1.referenced_column_name "
                  + "FROM information_schema.key_column_usage AS t1 "
                  + "LEFT JOIN information_schema.table_constraints AS t2 "
                  + "ON t1.constraint_name = t2.constraint_name AND t1.table_schema = t2.table_schema "
                  + "AND t1.table_name = t2.table_name "
                  + "WHERE t1.table_schema = '{0}'")
        cursor2.execute(query2.format(form["database"]))
        rows2 = cursor2.fetchall()
        cursor2.close()

        cnx.close()

        sorts = set(row[1] for row in rows1)
        pcf = DBContextFamily({s: None for s in sorts}, form["user"], form["password"], form["host"], form["database"])

        for column, sort, datatype in rows1:
            pcf.add_column(column, sort, datatype)

        for keyname, keytype, sort1, column1, sort2, column2 in rows2:
            assert keytype in ["PRIMARY KEY", "FOREIGN KEY"]
            if keytype == "PRIMARY KEY":
                assert pcf.output[sort1] is None
                pcf.set_printsql(sort1, "{{0}}.{0}".format(column1))
            else:
                pcf.add_foreign_key(keyname, sort1, column1, sort2, column2)

        Storage.write(pcf, form["name"] if form["name"] else form["database"])

    def select_sort(self, sort):
        self.state["current_sort"] = sort
        self.state["mva_form1"] = "derived"
        self.state["mva_form2"] = "derived"
        self.state["mva_form1_data"] = {
            "datatype": None,
            "name": "",
            "sqldef": "",
            "sort1": sort,
            "role1": "ATTR",
            "nargs": "1",
        }
        self.state["mva_form2_data"] = {
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
            self.state["mva_form2"] = "derived"
            self.state["mva_form2_data"] = {
                "datatype": None,
                "name": "",
                "sqldef": "",
                "sort1": self.state["current_sort"],
                "role1": "ARG1",
                "sort2": self.state["current_sort"],
                "role2": "ARG2",
                "nargs": "2",
            }

        elif form_id == "fk":
            self.state["mva_form2"] = "fk"
            self.state["mva_form2_data"] = {
                "name": "",
                "sort1": self.state["current_sort"],
                "column1": None,
                "role1": "ARG1",
                "sort2": self.state["current_sort"],
                "column2": None,
                "role2": "ARG2",
            }

    def update_form1(self, form_data):
        self.state["mva_form1_data"] = form_data
        if self.state["mva_form1"] == "derived":
            assert(int(self.state["mva_form1_data"]["nargs"]) == 1)

    def update_form2(self, form_data):
        self.state["mva_form2_data"] = form_data
        #  TODO: the code below (handling nargs change) probably works, but doesn't look good
        if self.state["mva_form2"] == "derived":
            nargs = int(self.state["mva_form2_data"]["nargs"])
            nparams = 0
            while True:
                if "sort{0}".format(nparams+1) in self.state["mva_form2_data"]:
                    nparams += 1
                else:
                    break
            if nparams != nargs:
                #  check that nargs is in the allowed range
                if nargs > 5:
                    assert(nparams == 5)
                    self.state["mva_form2_data"]["nargs"] = str(nparams)
                elif nargs < 2:
                    assert(nparams == 2)
                    self.state["mva_form2_data"]["nargs"] = str(nparams)
                #  add or delete a parameter to match nargs
                elif nargs < nparams:
                    assert(nparams == nargs+1)
                    del self.state["mva_form2_data"]["sort{0}".format(nparams)]
                    del self.state["mva_form2_data"]["role{0}".format(nparams)]
                elif nparams < nargs:
                    assert(nargs == nparams+1)
                    self.state["mva_form2_data"]["sort{0}".format(nargs)] = self.state["current_sort"]
                    self.state["mva_form2_data"]["role{0}".format(nargs)] = "ARG{0}".format(nargs)

    def create_mva1(self, form_data):
        pcf = Storage.read(self.state["pcf_name"])
        if self.state["mva_form1"] == "derived":
            # form = self.state["mva_form1_data"]

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

            self.state["mva_form1_data"] = {
                "datatype": None,
                "name": "",
                "sqldef": "",
                "sort1": self.state["current_sort"],
                "role1": "ATTR",
                "nargs": "1",
            }
        Storage.write(pcf, self.state["pcf_name"])

    def create_mva2(self, form_data):
        pcf = Storage.read(self.state["pcf_name"])
        if self.state["mva_form2"] == "derived":
            # form = self.state["mva_form2_data"]

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

            self.state["mva_form2_data"] = {
                "datatype": None,
                "name": "",
                "sqldef": "",
                "sort1": self.state["current_sort"],
                "role1": "ARG1",
                "sort2": self.state["current_sort"],
                "role2": "ARG2",
                "nargs": "2",
            }

        elif self.state["mva_form2"] == "fk":
            # form = self.state["mva_form2_data"]

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

            self.state["mva_form2_data"] = {
                "name": "",
                "sort1": self.state["current_sort"],
                "column1": None,
                "role1": "ARG1",
                "sort2": self.state["current_sort"],
                "column2": None,
                "role2": "ARG2",
            }
        Storage.write(pcf, self.state["pcf_name"])

    def scale_mva(self, mva_id, scale_class):
        pcf = Storage.read(self.state["pcf_name"])

        scale = None
        if scale_class == "BooleanScale":
            scale = BooleanScale()
        elif scale_class == "DateIntervalScale":
            scale = DateIntervalScale(1800, 2000, 10)
        if scale_class == "PrefixScale":
            scale = PrefixScale()

        pcf.scale_mva(mva_id, scale)
        Storage.write(pcf, self.state["pcf_name"])

    def delete_mva(self, mva_id):
        pcf = Storage.read(self.state["pcf_name"])
        pcf.delete_mva(mva_id)
        Storage.write(pcf, self.state["pcf_name"])

    def set_output_sql(self, sqlterm):
        pcf = Storage.read(self.state["pcf_name"])
        pcf.set_printsql(self.state["current_sort"], sqlterm)
        Storage.write(pcf, self.state["pcf_name"])
