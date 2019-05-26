from collections import Counter
from dbnav.table import Table
import json
import mysql.connector


class SupremumUndefinedError(Exception):
    pass


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

    def sql(self, node_ids):
        return self.sqldef.format(*node_ids)

    def get_class(self):
        return type(self).__name__

    def to_dict(self):
        return {
            "datatype": self.datatype,
            "name": self.name,
            "roles": self.roles,
            "sort": self.sort,
            "sqldef": self.sqldef,
        }

    @classmethod
    def from_dict(cls, obj):
        mva = cls(obj["name"], obj["sort"], obj["datatype"], obj["sqldef"], obj["roles"])
        return mva


class DBColumn(ManyValuedAttribute):

    def __init__(self, name, sort, datatype):
        sqldef = "{{0}}.{0}".format(name)
        super().__init__(name, sort, datatype, sqldef)

    @classmethod
    def from_dict(cls, obj):
        mva = cls(obj["name"], obj["sort"], obj["datatype"])
        mva.roles = obj["roles"]
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
        mva.roles = obj["roles"]
        return mva


class ScaledContext(object):

    # TODO Both mva_id and mva are stored because they are needed at different parts of the program.
    #  If the mva_id is not stored, then how can we be sure that this mva is equal (or not) to an mva
    #  in DBContextFamily.mvas? After serialization/deserialization, these might be two different objects
    #  (as it is implemented now). And if the mva is not stored, then how can it be retrieved, should the
    #  context really know the context family? Or should the context family act as a proxy for ScaledContext
    #  (i.e. other code never talks to ScaledContext directly)? But that would flatten the API ...
    def __init__(self, mva_id, mva, db_info):
        self.mva_id = mva_id
        self.mva = mva
        self.db_info = db_info

    @property
    def name(self):
        return self.mva.name

    @property
    def sort(self):
        return self.mva.sort

    @property
    def roles(self):
        return self.mva.roles

    def mva_sql(self, args):
        return self.mva.sql(args)

    def pattern_sql(self, label, args):
        raise NotImplementedError

    def stats(self, table, label):
        raise NotImplementedError

    def top(self):
        raise NotImplementedError

    def supremum(self, pattern1, pattern2):
        raise NotImplementedError

    def get_class(self):
        return type(self).__name__

    def to_dict(self):
        return {
            "mva_id": self.mva_id,
            "mva": self.mva,
            "db_info": self.db_info,
        }

    @classmethod
    def from_dict(cls, obj):
        return cls(obj["mva_id"], obj["mva"], obj["db_info"])


class BooleanFacet(ScaledContext):

    def cmd(self):
        return "set_fk_label_view"

    def template(self):
        return "script#fk_label_template"

    def pattern_sql(self, label, args):

        assert str(label) == "1"  # boolean scale only allows label to be "1"
        sqlterm = self.mva_sql(args)
        return "{0}=1".format(sqlterm)

    def stats(self, table, label):
        return {}

    def top(self):
        return "1"

    def supremum(self, pattern1, pattern2):
        pass


class PrefixFacet(ScaledContext):

    def cmd(self):
        return "set_prefix_label_view"

    def template(self):
        return "script#prefix_label_template"

    def pattern_sql(self, label, args):

        assert isinstance(label, str)  # the label is supposed to be a string
        sqlterm = self.mva_sql(args)
        if not label:  # empty string
            return "{0} IS NOT NULL".format(sqlterm)
        return "{0} LIKE '{1}%'".format(sqlterm, label)

    def stats(self, table, label):
        # TODO relying on column [len(self.sort)] to contain the mva-values seems too error-prone
        values = [row[len(self.sort)] for row in table.rows]
        freq = [(k, v) for k, v in Counter(values).items()]
        freq.sort(key=lambda x: x[0])
        return {"freq": freq, "count": len(values), "label": label}

    def top(self):
        return ""

    def supremum(self, pattern1, pattern2):
        pass


class DateIntervalFacet(ScaledContext):

    def cmd(self):
        return "set_interval_label_view"

    def template(self):
        return "script#interval_label_template"

    def pattern_sql(self, label, args):
        sqlterm = self.mva_sql(args)
        label_min, label_max = json.loads(label)
        return "{0} BETWEEN '{1}-01-01' AND '{2}-12-31'".format(sqlterm, label_min, label_max)

    def stats(self, table, label):
        # TODO relying on column [len(self.sort)] to contain the mva-values seems too error-prone
        values = [row[len(self.sort)] for row in table.rows]

        # histogram = [0] * self.nbins
        # data_min = None
        # data_max = None

        # if values:
        #     data_min = values[0].year
        #     data_max = values[0].year
        #     for d in values:
        #         v = d.year
        #         assert(self.mindate <= v <= self.maxdate)
        #         histogram[int((v-self.mindate) // self.step)] += 1
        #         data_min = min(data_min, v)
        #         data_max = max(data_max, v)

        label_min, label_max = json.loads(label)
        # TODO cache boundary interval on the object, or store on rnode, to avoid unnecessary self.top() queries?
        scale_min, scale_max = json.loads(self.top())

        return {"count": len(values), "scale_min": scale_min, "scale_max": scale_max,
                "label_min": label_min, "label_max": label_max}

    def top(self):
        args = ["x{0}".format(i+1) for i in range(len(self.sort))]
        sql_term = self.mva_sql(args)
        from_clause = ", ".join(["{0} AS {1}".format(s,x) for s,x in zip(self.sort, args)])
        query = "SELECT MIN({0}) AS mindate, MAX({0}) AS maxdate FROM {1}".format(sql_term, from_clause)

        cnx = mysql.connector.connect(user=self.db_info.user, password=self.db_info.password,
                                      host=self.db_info.host, database=self.db_info.database)
        cursor = cnx.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        header = [t[0] for t in cursor.description]

        cursor.close()
        cnx.close()

        assert(header[0] == "mindate")
        mindate = row[0].year

        assert(header[1] == "maxdate")
        maxdate = row[1].year

        return json.dumps([mindate, maxdate])

    def supremum(self, pattern1, pattern2):
        pass


class DatabaseInfo(object):

    def __init__(self, user, password, host, database):
        self.user = user
        self.password = password
        self.host = host
        self.database = database

    def to_dict(self):
        return {
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "database": self.database,
        }

    @classmethod
    def from_dict(cls, obj):
        return DatabaseInfo(obj["user"], obj["password"], obj["host"], obj["database"])


class DBContextFamily(object):

    def __init__(self, user, password, host, database):
        self.output = {}
        self.db_info = DatabaseInfo(user, password, host, database)
        self.rcontexts = {}
        self.mvas = {}
        self._next_id = 1

    @property
    def sorts(self):
        return sorted(self.output.keys())

    # There is possible confusion regarding the sort order: we represent sorts by patterns, and pattern1 <= pattern2
    # means that pattern1 represents a supersort (not subsort!!) of pattern2. The rationale is that sorts can be
    # understood as pattern concepts (obtained from the pcf's object context), and then the order on pattern intents
    # is indeed dual to the subsort order (cf. "Pattern structures and their projections" by Ganter & Kuznetsov,
    # where the infimum, there called "similarity operation", is a generalization operation). If patterns are
    # attribute sets (as in standard FCA), then the pattern order is the subset order (with infimum=intersection,
    # supremum=union).
    def sort_leq(self, pattern1, pattern2):
        if pattern1 is None or pattern2 is None:
            return pattern2 is None
        return pattern1 == pattern2

    # cf. the above remark for sort_leq
    def sort_sup(self, pattern1, pattern2):

        if pattern1 is None:
            return pattern2

        elif pattern2 is None:
            return pattern1

        elif pattern1 != pattern2:
            raise SupremumUndefinedError

        return pattern1

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

    # TODO: What happens if the deleted mva is being used by an rcontext?
    def delete_mva(self, name):
        self.mvas = {mva_id: mva for mva_id, mva in self.mvas.items() if mva_id != name}

    def set_printsql(self, sort, sqldef):
        self.output[sort] = sqldef

    def scale_mva(self, mva_id, scaled_context_class):

        scaled_context = None
        if scaled_context_class == "BooleanFacet":
            scaled_context = BooleanFacet(mva_id, self.mvas[mva_id], self.db_info)
        elif scaled_context_class == "DateIntervalFacet":
            scaled_context = DateIntervalFacet(mva_id, self.mvas[mva_id], self.db_info)
        if scaled_context_class == "PrefixFacet":
            scaled_context = PrefixFacet(mva_id, self.mvas[mva_id], self.db_info)

        self.rcontexts[mva_id] = scaled_context

    def print_sql(self, sort, node_id):
        return self.output[sort].format(node_id)

    def neighbors(self, sort):

        links = []
        for rcontext_id, rcontext in self.rcontexts.items():
            for i, s in enumerate(rcontext.sort, 1):
                if s == sort:
                    links.append({"linkID": rcontext_id, "roleID": i})
        return links

    def count_by_sort(self, objects):
        pass

    def _to_sql(self, graph, window, rwindow=None):

        rwindow = rwindow or []

        select = []
        from_ = []
        where = []

        # select clause
        for node_id in window:
            node = graph.nodes[node_id]
            select.append("{0} AS 'node:{1}'".format(self.print_sql(node.sort, node_id), node_id))

        for rnode_id in rwindow:
            rnode = graph.rnodes[rnode_id]
            rcontext = self.rcontexts[rnode.context_id]
            select.append("{0} AS 'rnode:{1}'".format(rcontext.mva_sql(rnode.endpoints), rnode_id))

        #  select clause: if a single subject was given, include its display attributes.
        #  the column name is display:{1}:{2}:{3} where {1} states the string length of {2},
        #  so that {2} and {3} can be reconstructed (even if {2} contains ":").
        #  TODO: to prevent such a hack, the graph could e.g. allow retrieval of {2} and {3} from a "display id"
        if len(window) == 1:
            node = graph.nodes[window[0]]
            for context_id in node.display:
                rcontext = self.rcontexts[context_id]
                select.append("{0} AS 'display:{1}:{2}:{3}'".format(rcontext.mva_sql([window[0]]), len(window[0]),
                                                                 window[0], context_id))

        # from clause
        for node_id, node in graph.nodes.items():
            from_.append("{0} AS {1}".format(node.sort, node_id))

        # where clause
        for rnode in graph.rnodes.values():
            rcontext = self.rcontexts[rnode.context_id]
            condition = rcontext.pattern_sql(rnode.label, rnode.endpoints)
            where.append(condition)

        query = ("SELECT DISTINCT " + ", ".join(select) + " FROM " + ", ".join(from_)
                 + (" WHERE " if where else "") + " AND ".join(where))

        return query

    def result_table(self, graph, window, rwindow):

        # check if the graph is trivial (isolated node).
        # theory-wise, returning an empty table is wrong; but it's convenient
        if len(graph.nodes) == 1 and len(graph.rnodes) == 0 and next(iter(graph.nodes.values())).sort is None:
            return Table([], [])

        query = self._to_sql(graph, window, rwindow)

        #  query the database
        cnx = mysql.connector.connect(user=self.db_info.user, password=self.db_info.password,
                                      host=self.db_info.host, database=self.db_info.database)
        cursor = cnx.cursor()

        cursor.execute(query)
        rows = cursor.fetchall()
        header = [t[0] for t in cursor.description]

        cursor.close()
        cnx.close()

        def rename(colname):

            type_, key = colname.split(":",1)
            assert(type_ in ["node", "rnode", "display"])

            if type_ == "node":
                node = graph.nodes[key]
                return "{0}:{1}".format(node.sort, key)

            elif type_ == "rnode":
                rnode = graph.rnodes[key]
                mva = self.rcontexts[rnode.context_id].mva
                return "{0}({1})".format(mva.name, ",".join(rnode.endpoints))

            # TODO: maybe refactor to get rid of complicated encoding scheme (see remark in _to_sql function)
            elif type_ == "display":
                length1, keys = key.split(":",1)
                length1 = int(length1)
                assert keys[length1] == ":"
                node_id = keys[:length1]
                context_id = keys[length1+1:]
                mva = self.rcontexts[context_id].mva
                return "{0}.{1}".format(node_id, mva.name)

        header = [rename(colname) for colname in header]
        return Table(header, rows)

    def stats(self, sort, table, lock_set):

        if not table.header:
            select = 'SELECT "{0}" AS sort, COUNT(*) AS count FROM {0}'
            parts = [select.format(table_name) for table_name in self.sorts]
            query = " UNION ".join(parts)

            cnx = mysql.connector.connect(user=self.db_info.user, password=self.db_info.password,
                                          host=self.db_info.host, database=self.db_info.database)
            cursor = cnx.cursor()

            # # the information_schema values turned out to be unreliable for InnoDB tables
            # query = "SELECT table_name,table_rows FROM information_schema.tables WHERE table_schema='{0}'"
            cursor.execute(query)
            rows = cursor.fetchall()

            cursor.close()
            cnx.close()

            attributes = []
            object_count = 0

            for row in rows:
                attributes.append({"name": row[0], "attributeID": row[0], "count": int(row[1]),
                                   "selected": False, "disabled": int(row[1]) == 0})
                object_count += int(row[1])

            stats = {"sorts": attributes, "objectCount": object_count}
            return stats

        else:
            attributes = []
            for s in self.sorts:
                if s == sort:
                    attributes.append({"name": s, "attributeID": s, "count": len(table.rows),
                                       "selected": True, "disabled": s in lock_set})
                else:
                    attributes.append({"name": s, "attributeID": s, "count": 0,
                                       "selected": False, "disabled": True})
            attributes.sort(key=lambda x: x["name"])

            stats = {"sorts": attributes, "objectCount": len(table.rows)}
            return stats

    def load_contents(self):
        cnx = mysql.connector.connect(user=self.db_info.user, password=self.db_info.password,
                                      host=self.db_info.host, database=self.db_info.database)

        cursor1 = cnx.cursor()
        query1 = "SELECT column_name,table_name,data_type FROM information_schema.columns WHERE table_schema='{0}'"
        cursor1.execute(query1.format(self.db_info.database))
        header1 = [t[0] for t in cursor1.description]
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
        cursor2.execute(query2.format(self.db_info.database))
        rows2 = cursor2.fetchall()
        cursor2.close()

        cnx.close()

        columns = {}
        assert(header1[0] == "column_name" and header1[1] == "table_name" and header1[2] == "data_type")

        for row in rows1:
            column = row[0]
            sort = row[1]
            columns.setdefault(sort,[]).append(column)

        self.output = {s: [] for s in columns.keys()}

        for column, sort, datatype in rows1:
            self.add_column(column, sort, datatype)

        for keyname, keytype, sort1, column1, sort2, column2 in rows2:
            assert keytype in ["PRIMARY KEY", "FOREIGN KEY"]
            if keytype == "PRIMARY KEY":
                self.output.setdefault(sort1,[]).append(column1)
            else:
                self.add_foreign_key(keyname, sort1, column1, sort2, column2)

        for sort in self.output:
            primary_keys = self.output[sort]
            if len(primary_keys) == 0:
                pk_list = ",', ',".join(["{{0}}.{0}".format(column) for column in columns[sort]])
                self.output[sort] = "CONCAT({0})".format(pk_list)
            elif len(primary_keys) == 1:
                self.output[sort] = "{{0}}.{0}".format(primary_keys[0])
            else:
                pk_list = ",', ',".join(["{{0}}.{0}".format(column) for column in primary_keys])
                self.output[sort] = "CONCAT({0})".format(pk_list)

    def to_dict(self):
        return {
            "output": self.output,
            "db_info": self.db_info,
            "mvas": self.mvas,
            "rcontexts": self.rcontexts,
            "_next_id": self._next_id,
        }

    @classmethod
    def from_dict(cls, obj):
        pcf = DBContextFamily(obj["db_info"].user, obj["db_info"].password,
                              obj["db_info"].host, obj["db_info"].database)
        pcf.output = obj["output"]
        pcf.mvas = obj["mvas"]
        pcf.rcontexts = obj["rcontexts"]
        pcf._next_id = obj["_next_id"]
        return pcf
