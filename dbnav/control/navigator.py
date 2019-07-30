from dbnav.control.control import Control
from dbnav.storage import Storage
from dbnav.graph import Graph, Point


class NavigatorControl(Control):

    def __init__(self, pcf_name):

        self.name = "navigator"
        self.slots = {}
        self.graph = Graph(Storage.read(pcf_name))
        self.current_node = self.graph.add_node(None, Point(305, 150))
        self.current_link = {"linkID": None, "roleID": None}

    def render(self):

        views = []
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
                "data": self.links_view_data(self.graph, self.current_node,
                                             self.current_link),
            }
        })

        views.append({
            "cmd": "set_graph_view",
            "args": {
                "graph": self.graph,
                "current_node": self.current_node,
            }
        })

        views.append({
            "cmd": "set_table_view",
            "args": {
                "slot": "tableView",
                "template": "script#result_template",
                "data": self.table_view_data(self.graph, self.current_node,
                                             self.current_link["linkID"]),
            }
        })

        if self.current_link["linkID"] is None:

            views.append({
                "cmd": "set_sort_label_view",
                "args": {
                    "slot": "labelView",
                    "template": "script#sort_label_template",
                    "data": self.graph.stats(self.current_node),
                }
            })

        else:

            rnode_id = self.current_link["linkID"]
            rnode = self.graph.rnodes[rnode_id]
            rcontext = self.graph.pcf.rcontexts[rnode.context_id]

            views.append({
                "cmd": rcontext.cmd(),
                "args": {
                    "slot": "labelView",
                    "template": rcontext.template(),
                    "data": self.graph.rstats(rnode_id),
                }
            })

        return views

    # View Updates
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

    # Actions
    def set_label(self, label):
        if self.current_link["linkID"] is None:  # object node
            node = self.graph.nodes[self.current_node]
            node.sort = label
            # TODO forgetting all display attributes when changing sorts - simple, but is it intuitive (for user)?
            node.display = set()
        else:
            rnode = self.graph.rnodes[self.current_link["linkID"]]
            rnode.label = label

    def set_position(self, node_id, x, y):
        self.graph.set_position(node_id, x, y)

    def merge(self, target_id):
        self.graph.merge(self.current_node, target_id)

    def create_edge(self, context_id, role_id):
        node_id = self.current_node
        rcontext = self.graph.pcf.rcontexts[context_id]

        endpoints = [None]*len(rcontext.sort)
        endpoints[role_id-1] = node_id
        self.graph.add_rnode(context_id, endpoints)

    def select_edge(self, edge_id, role_id):
        if edge_id is None:
            self.current_link = {"linkID": None, "roleID": None}
        else:
            self.current_link = {"linkID": edge_id, "roleID": role_id}

    def remove_edge(self, edge_id, role_id):
        self.graph.remove_rnode(edge_id, role_id)
        self.current_link = {"linkID": None, "roleID": None}

    def select_node(self, node_id):
        self.current_node = node_id
        self.current_link = {"linkID": None, "roleID": None}

    def toggle_display(self, context_id):
        node = self.graph.nodes[self.current_node]
        if context_id in node.display:
            node.display.remove(context_id)
        else:
            node.display.add(context_id)
