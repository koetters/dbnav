from dbnav.table import Table


class FormalContext(object):

    def __init__(self, atts=None):
        atts = atts or []
        self.objects = {}
        self.attributes = set(atts)
        self.incidence = set()
        self._next_id = 1

    def add_object(self, obj, atts=None):
        atts = atts or []
        obj_id = "g" + str(self._next_id)
        self._next_id += 1
        self.objects[obj_id] = obj
        for m in atts:
            self.set_incidence(obj_id, m)
        return obj_id

    def add_attribute(self, m):
        self.attributes.add(m)

    def set_incidence(self, obj_id, m):
        self.incidence.add((obj_id, m))

    def unset_incidence(self, obj_id, m):
        self.incidence.remove((obj_id, m))

    def has(self, obj_id, m):
        # According to the Python 3.6 reference, Sect 6.10.2, "x in y" is equivalent to "any(x is e or x==e for e in y)"
        return (obj_id, m) in self.incidence

    def extent(self, atts):
        return [(obj_id, obj) for obj_id, obj in self.objects.items() if all(self.has(obj_id, m) for m in atts)]

    def intent(self, obj_ids):
        return [m for m in self.attributes if all(self.has(obj_id, m) for obj_id in obj_ids)]

    def to_dict(self):
        return {
            "objects": self.objects,
            "attributes": sorted(self.attributes),
            "incidence": sorted(self.incidence),
            "_next_id": self._next_id
        }

    @classmethod
    def from_dict(cls, obj):
        context = FormalContext()
        context.objects = obj["objects"]
        context.attributes = obj["attributes"]
        context.incidence = [tuple(x) for x in obj["incidence"]]
        context._next_id = obj["_next_id"]
        return context


class RelationContext(object):

    def __init__(self, rsort, atts=None):
        atts = atts or []
        self.rsort = rsort
        self.roles = None
        self.objects = {}
        self.attributes = set(atts)
        self.incidence = set()
        self._next_id = 1

    def add_tuple(self, obj_tuple, atts=None):
        atts = atts or []
        tuple_id = "t" + str(self._next_id)
        self._next_id += 1
        self.objects[tuple_id] = ObjectTuple(obj_tuple)
        for m in atts:
            self.set_incidence(tuple_id, m)
        return tuple_id

    def add_attribute(self, att):
        self.attributes.add(att)

    def set_incidence(self, obj_tuple, att):
        self.incidence.add((obj_tuple, att))

    def to_dict(self):
        return {
            "rsort": self.rsort,
            "roles": self.roles,
            "objects": self.objects,
            "attributes": sorted(self.attributes),
            "incidence": sorted(self.incidence),
            "_next_id": self._next_id
        }

    @classmethod
    def from_dict(cls, obj):
        context = RelationContext(obj["rsort"])
        context.roles = obj["roles"]
        context.objects = obj["objects"]
        context.attributes = obj["attributes"]
        context.incidence = [tuple(x) for x in obj["incidence"]]
        context._next_id = obj["_next_id"]
        return context


class ObjectTuple(object):

    def __init__(self, endpoints):
        self.endpoints = tuple(endpoints)

    def to_dict(self):
        return {"endpoints": self.endpoints}

    @classmethod
    def from_dict(cls, obj):
        return ObjectTuple(obj["endpoints"])


class FacetedPowerContextFamily(object):

    def __init__(self, sorts=None):
        sorts = sorts or []
        self.object_context = FormalContext(sorts)
        self.rcontexts = {}

    def add_object(self, obj, atts=None):
        atts = atts or []
        return self.object_context.add_object(obj, atts)

    def add_sort(self, sort):
        self.object_context.add_attribute(sort)

    def add_rcontext(self, context_id, rsort, atts=None):
        atts = atts or []
        self.rcontexts[context_id] = RelationContext(rsort, atts)

    def add_tuple(self, obj_tuple, context_id, atts):
        return self.rcontexts[context_id].add_tuple(obj_tuple, atts)

    def stats(self, rcontext_id):
        pass

    def result_table(self, graph, window):
        if len(graph.rnodes) == 0:
            if len(graph.nodes) == 0:
                return Table([], [[]])
            else:
                assert(len(graph.nodes) == 1)
                node_id, node = next(iter(graph.nodes.items()))
                header = [node_id]
                rows = [self.object_context.extent(node.sort)]
                return Table(header, rows)
        else:
            # targets[node_id] is a list of potential matches (in the pcf) for the given node
            # rtargets[rnode_id] is a list of potential matches (in the pcf) for the given rnode
            targets = {node_id: [] for node_id in graph.nodes.keys()}
            rtargets = {rnode_id: [] for rnode_id in graph.rnodes.keys()}

            for node_id, node in graph.nodes.items():
                targets[node_id] = self.object_context.extent(node.sort)

            for rnode_id, rnode in graph.rnodes.items():
                for rtarget in self.rcontexts[rnode.context_id].extent(rnode.label):
                    # one-liner to ensure that equal endpoints of rnode map to equal endpoints of rtarget
                    if len(set(rnode.endpoints)) < len(set(zip(rnode.endpoints, rtarget.endpoints))):
                        continue
                    # ensure that all endpoints of rtarget can be matched (to some element in the target list)
                    if not all(
                            rtarget.endpoints[i] in targets[rnode.endpoints[i]] for i in range(len(rnode.endpoints))
                    ):
                        continue
                    rtargets[rnode_id].append(rtarget)

            track = list(graph.rnodes.keys())
            counter = {rnode_id: 0 for rnode_id in track}
            morphism = {node_id: None for node_id in graph.nodes}
            stack = [morphism]
            result = []

            index = 0
            while index >= 0:
                rnode_id = track[index]
                morphism = stack[-1]
                if counter[rnode_id] >= len(rtargets[rnode_id]):
                    index = index - 1
                    counter[rnode_id] = 0
                    stack.pop()
                else:
                    rnode = graph.rnodes[rnode_id]
                    rtarget = rtargets[rnode_id][counter[rnode_id]]
                    # this is well-defined because, as ensured above, equality of endpoints is preserved
                    morphism_patch = {rnode.endpoints[i]: rtarget.endpoints[i] for i in range(len(rnode.endpoints))}
                    # if the morphism patch extends (doesn't contradict) the current morphism, it is applied
                    if all(morphism[key] in [None, morphism_patch[key]] for key in morphism_patch):
                        # if all rnodes have been matched, the patched morphism is entered as a result
                        if index == len(track) - 1:
                            result.append(dict(morphism, **morphism_patch))
                            counter[rnode_id] += 1
                        else:
                            index += 1
                            stack.append(dict(morphism, **morphism_patch))
                    else:  # try matching rnode with next rtarget
                        counter[rnode_id] += 1

            header = window
            rows = [[morphism[x] for x in header] for morphism in result]
            return Table(header, rows)

    def to_dict(self):
        return {
            "object_context": self.object_context,
            "rcontexts": self.rcontexts,
        }

    @classmethod
    def from_dict(cls, obj):
        pcf = FacetedPowerContextFamily()
        pcf.object_context = obj["object_context"]
        pcf.rcontexts = obj["rcontexts"]
        return pcf
