import json
from collections import Counter


class Scale(object):

    def pattern_sql(self, label):
        pass

    def stats(self, values):
        pass

    def top(self):
        pass

    def supremum(self):
        pass

    def get_class(self):
        return type(self).__name__

    def to_dict(self):
        return {}

    @classmethod
    def from_dict(cls, obj):
        return cls()


class BooleanScale(Scale):

    def pattern_sql(self, label):
        assert str(label) == "1"  # boolean scale only allows label to be "1"
        return "{0}=1"

    def stats(self, values):
        return {}

    def top(self):
        return "1"

    def supremum(self):
        pass


class PrefixScale(Scale):

    def pattern_sql(self, label):

        assert isinstance(label, str)  # the label is supposed to be a string
        if not label:  # empty string
            return "{0} IS NOT NULL"
        else:
            return "{{0}} LIKE '{0}%'".format(label)

    def stats(self, values):
        freq = [(k, v) for k, v in Counter(values).items()]
        freq.sort(key=lambda x: x[0])
        return {"freq": freq, "count": len(values)}

    def top(self):
        return ""

    def supremum(self):
        pass


class DateIntervalScale(Scale):

    def __init__(self, mindate, maxdate, step):

        self.mindate = mindate
        self.step = step

        if int((maxdate - mindate) % step) == 0:
            self.nbins = int((maxdate - mindate) // step)
            self.maxdate = maxdate
        else:  # if step does not divide (maxdate-mindate), maxdate is increased to a step size
            self.nbins = int((maxdate - mindate) // step) + 1
            self.maxdate = mindate + self.nbins * step

    def pattern_sql(self, label):
        return "{{0}} BETWEEN '{0}-01-01' AND '{1}-12-31'".format(label[0],label[1])

    def stats(self, values):

        histogram = [0] * self.nbins
        data_min = None
        data_max = None

        if values:
            data_min = values[0].year
            data_max = values[0].year
            for d in values:
                v = d.year
                assert(self.mindate <= v <= self.maxdate)
                histogram[int((v-self.mindate) // self.step)] += 1
                data_min = min(data_min, v)
                data_max = max(data_max, v)

        return {"histogram": histogram, "count": len(values), "data_min": data_min, "data_max": data_max,
                "scale_min": self.mindate, "scale_max": self.maxdate}

    def top(self):
        return [self.mindate, self.maxdate]

    def supremum(self):
        pass

    def to_dict(self):
        return {
            "maxdate": self.maxdate,
            "mindate": self.mindate,
            "step": self.step,
        }

    @classmethod
    def from_dict(cls, obj):
        return cls(obj["mindate"], obj["maxdate"], obj["step"])


class ManyValuedAttribute(object):

    def __init__(self, name, sort, datatype, sqldef, roles=None):
        self.name = name
        self.datatype = datatype
        self.sort = sort
        self.sqldef = sqldef
        if roles is None:
            if len(sort) == 1:
                self.roles = ['ATTR']
            else:
                self.roles = ['ARG{0}'.format(i+1) for i, s in enumerate(sort)]
        else:
            self.roles = roles
        self.scale = None

    def sql(self, node_ids):
        return self.sqldef.format(*node_ids)

    def get_class(self):
        return type(self).__name__

    def to_dict(self):
        return {
            "datatype": self.datatype,
            "name": self.name,
            "roles": self.roles,
            "scale": self.scale,
            "sort": self.sort,
            "sqldef": self.sqldef,
        }

    @classmethod
    def from_dict(cls, obj):
        mva = cls(obj["name"], obj["sort"], obj["datatype"], obj["sqldef"], obj["roles"])
        mva.scale = obj["scale"]
        return mva


class DBColumn(ManyValuedAttribute):

    def __init__(self, name, sort, datatype):
        sqldef = "{{0}}.{0}".format(name)
        super().__init__(name, sort, datatype, sqldef)

    @classmethod
    def from_dict(cls, obj):
        mva = cls(obj["name"], obj["sort"], obj["datatype"])
        mva.scale = obj["scale"]
        return mva


class ForeignKey(ManyValuedAttribute):

    def __init__(self, name, sort, columns):
        self.columns = columns
        sqldef = "IF({{0}}.{0}={{1}}.{1},1,0)".format(*columns)
        super().__init__(name, sort, "bool", sqldef)

    def to_dict(self):
        obj = super().to_dict()
        obj["columns"] = self.columns
        return obj

    @classmethod
    def from_dict(cls, obj):
        mva = cls(obj["name"], obj["sort"], obj["columns"])
        mva.scale = obj["scale"]
        return mva


class PowerContextFamily(object):

    def __init__(self, output, user, password, host, database):
        self.output = dict(output)
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.mvas = {}
        self._next_id = 1

    def sorts(self):
        return sorted(self.output.keys())

    def add_column(self, name, sort, datatype):
        mva_id = "m" + str(self._next_id)
        self._next_id += 1
        self.mvas[mva_id] = DBColumn(name, [sort], datatype)
        return mva_id

    def add_foreign_key(self, name, sort1, column1, sort2, column2):
        mva_id = "m" + str(self._next_id)
        self._next_id += 1
        self.mvas[mva_id] = ForeignKey(name, [sort1, sort2], [column1, column2])
        return mva_id

    def add_mva(self, name, sort, datatype, sqldef, roles):
        mva_id = "m" + str(self._next_id)
        self._next_id += 1
        self.mvas[mva_id] = ManyValuedAttribute(name, sort, datatype, sqldef, roles)
        return mva_id

    def delete_mva(self, name):
        print(name)
        print(self.mvas)
        self.mvas = {mva_id: mva for mva_id, mva in self.mvas.items() if mva_id != name}
        print(self.mvas)

    def set_printsql(self, sort, sqldef):
        self.output[sort] = sqldef

    def scale_mva(self, mva_id, scale):
        self.mvas[mva_id].scale = scale

    def print_sql(self, sort, node_id):
        return self.output[sort].format(node_id)

    def mva_sql(self, mva_id, node_ids):
        mva = self.mvas[mva_id]
        return mva.sql(node_ids)

    def pattern_sql(self, mva_id, label, endpoints):
        mva = self.mvas[mva_id]
        sql_term = mva.sql(endpoints)
        return mva.scale.pattern_sql(label).format(sql_term)

    def neighbors(self, sort):

        links = []
        for mvaID, mva in self.mvas.items():
            if mva.scale is not None:
                for i, s in enumerate(mva.sort, 1):
                    if s == sort:
                        links.append({"linkID": mvaID, "roleID": i})
        return links

    def to_dict(self):
        return {
            "output": self.output,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "database": self.database,
            "mvas": self.mvas,
            "_next_id": self._next_id,
        }

    @classmethod
    def from_dict(cls, obj):
        pcf = PowerContextFamily(obj["output"], obj["user"], obj["password"], obj["host"], obj["database"])
        pcf.mvas = dict(obj["mvas"])
        pcf._next_id = obj["_next_id"]
        return pcf
