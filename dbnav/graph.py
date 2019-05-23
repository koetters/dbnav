import copy


class Point(object):

    def __init__(self, x, y):

        self.x = x
        self.y = y

    def to_dict(self):
        return {
            "x": self.x,
            "y": self.y,
        }

    @classmethod
    def from_dict(cls, obj):
        return Point(obj["x"], obj["y"])


class Node(object):

    def __init__(self, sort):

        self.sort = sort
        self.display = set()
        self.point = Point(100, 100)

    def to_dict(self):
        return {
            "sort": self.sort,
            "display": self.display,
            "point": self.point,
        }

    @classmethod
    def from_dict(cls, obj):
        node = Node(obj["sort"])
        node.display = obj["display"]
        node.point = obj["point"]
        return node


class RNode(object):

    def __init__(self, context_id, context_name, endpoints, label):

        # TODO how to get rid of context_name? it violates the DRY principle ...
        self.context_id = context_id
        self.context_name = context_name
        self.endpoints = endpoints
        self.label = label
        self.point = Point(200, 200)

    def to_dict(self):
        return {
            "context_id": self.context_id,
            "context_name": self.context_name,
            "endpoints": self.endpoints,
            "label": self.label,
            "point": self.point,
        }

    @classmethod
    def from_dict(cls, obj):
        rnode = RNode(obj["context_id"], obj["context_name"], obj["endpoints"], obj["label"])
        rnode.point = obj["point"]
        return rnode


class Graph(object):

    def __init__(self, pcf):

        self.nodes = {}
        self.rnodes = {}
        self.pcf = pcf
        self._next_id = 1
        self._next_rid = 1

    def add_node(self, sort, pos=None):

        node_id = "x" + str(self._next_id)
        self._next_id += 1
        self.nodes[node_id] = Node(sort)
        if pos is None:
            self.nodes[node_id].point = Point(100, 100)
        else:
            self.nodes[node_id].point = pos
        return node_id

    def add_rnode(self, context_id, endpoints, pos=None):

        rcontext = self.pcf.rcontexts[context_id]
        mva = rcontext.mva
        rsort = mva.sort
        scale = rcontext.scale

        assert(len(endpoints) == len(rsort))

        for i, n in enumerate(endpoints):
            if n is None:
                endpoints[i] = self.add_node(rsort[i])
            else:
                assert(self.pcf.sort_leq(rsort[i], self.nodes[n].sort))

        rnode_id = "e" + str(self._next_rid)
        self._next_rid += 1
        self.rnodes[rnode_id] = RNode(context_id, rcontext.name, endpoints, scale.top())
        if pos is None:
            self.rnodes[rnode_id].point = Point(200, 200)
        else:
            self.rnodes[rnode_id].point = pos
        return rnode_id

    def remove_rnode(self, rnode_id, role_id):

        rnode = self.rnodes[rnode_id]
        del self.rnodes[rnode_id]
        if role_id is not None:
            component = self.component(rnode.endpoints[role_id-1])
            for x in list(self.nodes.keys()):
                if x not in component.nodes:
                    del self.nodes[x]

            for e in list(self.rnodes.keys()):
                if e not in component.rnodes:
                    del self.rnodes[e]

    def set_position(self, node_id, x, y):
        # TODO: ensure (in the graph class) that node ID's and rnode ID's are disjoint
        if node_id in self.nodes:
            self.nodes[node_id].point = Point(x, y)
        else:
            self.rnodes[node_id].point = Point(x, y)

    def get_context(self, rnode_id):
        context_id = self.rnodes[rnode_id].context_id
        return self.pcf.rcontexts[context_id]

    # returns the sort constraints (as a set of sorts) that are imposed on a given node by its incident rnodes
    def lock_set(self, node_id):
        return {self.get_context(link["linkID"]).sort[link["roleID"]-1] for link in self.neighbors(node_id)}

    def merge(self, node_id, target_id):

        node = self.nodes[node_id]
        target = self.nodes[target_id]
        target_sort = self.pcf.sort_sup(node.sort, target.sort)

        hashtable = {}
        quotient = Graph(self.pcf)

        for x, node in self.nodes.items():
            if x != node_id:
                quotient.nodes[x] = copy.deepcopy(node)

        quotient.nodes[target_id].sort = target_sort
        quotient.nodes[target_id].display |= self.nodes[node_id].display

        for rnode_id, rnode in self.rnodes.items():
            endpoints = [target_id if x == node_id else x for x in rnode.endpoints]
            key = (rnode.context_id, tuple(endpoints))

            if key not in hashtable:
                hashtable[key] = rnode_id
                quotient.rnodes[rnode_id] = RNode(rnode.context_id, rnode.context_name, endpoints, rnode.label)

            else:
                qnode = quotient.rnodes[hashtable[key]]
                scale = self.pcf.rcontexts[key[0]].scale
                qnode.label = scale.supremum(qnode.label, rnode.label)

        self.nodes = quotient.nodes
        self.rnodes = quotient.rnodes

    def neighbors(self, node_id):

        links = []
        for rnode_id, rnode in self.rnodes.items():
            for i, other_id in enumerate(rnode.endpoints, 1):
                if node_id == other_id:
                    links.append({"linkID": rnode_id, "roleID": i})
        return links

    def component(self, node_id):

        queue = [node_id]
        rqueue = []
        component = Graph(self.pcf)

        while queue or rqueue:
            if queue:
                x = queue.pop(0)
                if x not in component.nodes:
                    rqueue += [link["linkID"] for link in self.neighbors(x)]
                    component.nodes[x] = copy.deepcopy(self.nodes[x])
            else:
                e = rqueue.pop(0)
                if e not in component.rnodes:
                    queue += self.rnodes[e].endpoints
                    component.rnodes[e] = copy.deepcopy(self.rnodes[e])

        return component

    def stats(self, node_id):

        table = self.extent(node_id)
        # TODO check whether sort really needs to be passed as a parameter. currently this in only needed
        #  because the DB backend can't figure the sort of objects from the table. This could be done by
        #  either supplementing sort info in column headers, or by putting sort info directly on the
        #  objects (i.e. prefixing with the sort).
        return self.pcf.stats(self.nodes[node_id].sort, table, self.lock_set(node_id))

    def rstats(self, rnode_id):

        table = self.rextent(rnode_id)
        return self.get_context(rnode_id).stats(table)

    def extent(self, node_id):
        # passing "self" undoubtedly looks strange; it is a consequence of pcf being an attribute of graph;
        # TODO: rename this class e.g. navigation_state or semiconcept, pass self.graph instead of "self"
        return self.pcf.result_table(self, [node_id], [])

    def rextent(self, rnode_id):
        return self.pcf.result_table(self, self.rnodes[rnode_id].endpoints, [rnode_id])

    def to_dict(self):
        return {
            "pcf": self.pcf,
            "nodes": self.nodes,
            "rnodes": self.rnodes,
            "_next_id": self._next_id,
            "_next_rid": self._next_rid,
        }

    @classmethod
    def from_dict(cls, obj):
        graph = Graph(obj["pcf"])
        graph.nodes = obj["nodes"]
        graph.rnodes = obj["rnodes"]
        graph._next_id = obj["_next_id"]
        graph._next_rid = obj["_next_rid"]
        return graph
