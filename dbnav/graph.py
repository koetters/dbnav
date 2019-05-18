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
    def from_dict(cls,obj):
        return Point(obj["x"],obj["y"])


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
    def from_dict(cls,obj):
        node = Node(obj["sort"])
        node.display = obj["display"]
        node.point = obj["point"]
        return node


class RNode(object):

    def __init__(self, context_id, context_name, endpoints, label):

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
    def from_dict(cls,obj):
        rnode = RNode(obj["context_id"],obj["context_name"],obj["endpoints"],obj["label"])
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

    def add_rnode(self, mva_id, endpoints, pos=None):

        mva = self.pcf.mvas[mva_id]
        scale = mva.scale
        rsort = mva.sort
        assert(mva.scale is not None and len(endpoints) == len(rsort))

        for i, n in enumerate(endpoints):
            if n is None:
                endpoints[i] = self.add_node(rsort[i])
            else:
                assert(self.nodes[n].sort == rsort[i])

        rnode_id = "e" + str(self._next_rid)
        self._next_rid += 1
        self.rnodes[rnode_id] = RNode(mva_id, mva.name, endpoints, scale.top())
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
            self.nodes[node_id].point = Point(x,y)
        else:
            self.rnodes[node_id].point = Point(x,y)

    def merge(self, node_id, target_id):

        node = self.nodes[node_id]
        target = self.nodes[target_id]
        assert(node.sort == target.sort)

        hashtable = {}
        quotient = Graph(self.pcf)

        for x, node in self.nodes.items():
            if x != node_id:
                quotient.nodes[x] = copy.deepcopy(node)

        quotient.nodes[target_id].display |= self.nodes[node_id].display

        for rnode_id, rnode in self.rnodes.items():
            endpoints = [target_id if x == node_id else x for x in rnode.endpoints]
            key = (rnode.context_id, tuple(endpoints))

            if key not in hashtable:
                hashtable[key] = rnode_id
                quotient.rnodes[rnode_id] = RNode(rnode.context_id, rnode.context_name, endpoints, rnode.label)

            else:
                qnode = quotient.rnodes[hashtable[key]]
                scale = self.pcf.mvas[key[0]].scale
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

    def to_dict(self):
        return {
            "pcf": self.pcf,
            "nodes": self.nodes,
            "rnodes": self.rnodes,
            "_next_id": self._next_id,
            "_next_rid": self._next_rid,
        }

    @classmethod
    def from_dict(cls,obj):
        graph = Graph(obj["pcf"])
        graph.nodes = obj["nodes"]
        graph.rnodes = obj["rnodes"]
        graph._next_id = obj["_next_id"]
        graph._next_rid = obj["_next_rid"]
        return graph

    def result_table(self,node_id,rnode_id):

        if rnode_id is None:
            window = [node_id]
            rwindow = []
        else:
            endpoints = self.rnodes[rnode_id].endpoints
            window = endpoints
            rwindow = [rnode_id]

        # passing "self" undoubtedly looks strange; it is a consequence of pcf being an attribute of graph;
        # TODO: rename this class e.g. navigation_state or semiconcept, pass self.graph instead of "self"
        return self.pcf.result_table(self,window,rwindow)


