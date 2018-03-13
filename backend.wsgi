import re
import os
import json
import cgi
import sqlite3
import mysql.connector
from itertools import izip

class URL(object):

    def __init__(self,pattern,method,schema=[]):
        self.pattern = pattern
        self.method = method
        self.schema = schema

class ObjectEncoder(json.JSONEncoder):

    def default(self,obj):
        try:
            return obj.__dict__
        except AttributeError:
            return json.JSONEncoder.default(self,obj)

class ObjectDecoder(json.JSONDecoder):

    # overriding signature, but should be ok with Liskov substitution principle
    def decode(self,s,schema=None):

        result = json.JSONDecoder.decode(self,s)
        if schema is None:
            return result
        else:
            lst = []
            for i,cls in enumerate(schema):
                if cls is None:
                    lst.append(result[i])
                else:
                    lst.append(cls._fromjson(result[i]))
            return lst

class UnaryScale(object):

    def __init__(self,name,atts):
        self.name = name
        self.atts = dict(atts)

    def add_attribute(self,m,sqldef):
        self.atts[m] = sqldef

    def sql(self,atts,var,col):
        arg = re.sub(r"^[^.]+",var,col)
        where = [ self.atts[m].format(arg) for m in atts ]
        if atts:
            refs = { m: ("def" if m in atts else "none") for m in self.atts }
        else:
            refs = { m: "MIN({0})+MAX({0})".format(p.format(arg)) for m,p in self.atts.items() }
        return (where,refs)

class BinaryScale(object):

    def __init__(self,name,atts):
        self.name = name
        self.atts = dict(atts)

    def add_attribute(self,m,sqldef):
        self.atts[m] = sqldef

    def sql(self,atts,vars,cols):
        args = [ re.sub(r"^[^.]+",x,col) for x,col in izip(vars,cols) ]
        where = [ self.atts[m].format(*args) for m in atts ]
        if atts:
            refs = { m: ("def" if m in atts else "none") for m in self.atts }
        else:
            refs = { m: "MIN({0})+MAX({0})".format(p.format(*args)) for m,p in self.atts.items() }
        return (where,refs)

    def rolesql(self,var,pos,cols):
        sorts = [ col.split(".",1)[0] for col in cols ]
        vars = [ "_{0}".format(i+1) for i in range(len(cols)) ]
        vars[pos-1] = var

        args = [ re.sub(r"^[^.]+",x,col) for x,col in izip(vars,cols) ]
        aliases = [ "{0} AS {1}".format(s,x) for s,x in izip(sorts,vars) if x != var ]
        sqlfrom = ", ".join(aliases)

        refs = { m: "MIN({0})+MAX({0})".format("EXISTS(SELECT * FROM {0} WHERE {1})").format(sqlfrom,p.format(*args)) for m,p in self.atts.items() }
        return refs

# a class for storing the DB scale object. this should probably be merged with the UnaryScale and BinaryScale classes above.
class Scale(object):

    def __init__(self,id,name,arity):
        self.id = id
        self.name = name
        self.arity = arity

    @classmethod
    def _fromjson(cls,obj):
        return Scale(**obj)

class Attribute(object):

    def __init__(self,id,name,sqldef):
        self.id = id
        self.name = name
        self.sqldef = sqldef

    @classmethod
    def _fromjson(cls,obj):
        return Attribute(**obj)

# class for DB bindings object. could perhaps be merged with "binding" class below.
class ContextFamily(object):
    def __init__(self,id,name,host,database):
        self.id = id
        self.name = name
        self.host = host
        self.database = database

    @classmethod
    def _fromjson(cls,obj):
        return ContextFamily(**obj)

class Sort(object):
    def __init__(self,id,name,output):
        self.id = id
        self.name = name
        self.output = output

    @classmethod
    def _fromjson(cls,obj):
        return Sort(**obj)

class SQLColumn(object):
    def __init__(self,id,name,sqltype):
        self.id = id
        self.name = name
        self.sqltype = sqltype

    @classmethod
    def _fromjson(cls,obj):
        return SQLColumn(**obj)

class BoundScale(object):
    def __init__(self,id,scale,args):
        self.id = id
        self.scale = scale
        self.args = args

    @classmethod
    def _fromjson(cls,obj):
        obj["scale"] = Scale._fromjson(obj["scale"])
        return BoundScale(**obj)

class Binding(object):

    def __init__(self,bound_scales):
            self._bindings = dict(bound_scales)

    def __call__(self,sort,role=None):

        scales = []

        if role:
            for prefix,bound_scale in self._bindings.items():
                scale,columns = bound_scale
                if len(columns) > 1 and role <= len(columns) and columns[role-1].startswith(sort+"."):
                    scales.append((prefix,scale,columns))

        else:
            sorts = sort.split("#") if "#" in sort else [sort]
            for prefix,bound_scale in self._bindings.items():
                scale,columns = bound_scale
                if len(sorts) == len(columns) and all(col.startswith(s+".") for s,col in izip(sorts,columns)):
                    scales.append((prefix,scale,columns))

        return scales

    def sorts(self,prefix):
        return [ column.split(".")[0] for column in self._bindings[prefix][1] ]

def get_login():
    return ("dbnav","dbnav")

def solve(graph,bindingId):
    """
    :param graph: A query graph.
    :param bindingId: A ContextFamily ID.
    :return: A dictionary containing query results and refinement options.
    """

    if not graph:
        return dict(header=[],result=[],options={},equalities=[])

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    cursor.execute("SELECT K.id,K.name,K.arity,m.name,m.sqldef FROM Scale as K,ScaleAttribute AS m WHERE m.scale = K.id AND K.id IN (SELECT scale FROM BoundScale WHERE binding = ?)",[str(bindingId)])
    rows1 = cursor.fetchall()

    cursor.execute("SELECT K.id,K.scale,y.arg,y.column FROM BoundScale AS K,BoundScaleArg AS y WHERE y.bound_scale = K.id AND K.binding = ?",[str(bindingId)])
    rows2 = cursor.fetchall()

    cursor.execute("SELECT table_name,column_name FROM BindingOutput WHERE binding = ?",[str(bindingId)])
    rows3 = cursor.fetchall()

    cursor.execute("SELECT name,host,database FROM Binding WHERE id=?",[str(bindingId)])
    binding_name,host,database = cursor.fetchone()

    cursor.close()
    cnx.close()

    scales = {}
    for row in rows1:
        id,name,arity,m,sqldef = row
        if id in scales:
            scales[id].add_attribute(m,sqldef)
        else:
            if arity == 1:
                scales[id] = UnaryScale(name,{m:sqldef})
            if arity == 2:
                scales[id] = BinaryScale(name,{m:sqldef})

    bound_scales = {}
    for row in rows2:
        id,scaleId,arg,column = row
        # this is where the attribute prefixes are chosen.
        prefix = id
        if prefix in bound_scales:
            bound_scales[prefix][1].append((arg,column))

        else:
            bound_scales[prefix] = [scales[scaleId],[(arg,column)]]

    for pair in bound_scales.values():
        pair[1] = [ column for arg,column in sorted(pair[1],key=lambda x:x[0]) ]

    bs2 = {}
    for bs in bound_scales.values():
        colname = bs[1][0].split(".")[1]
        bs2[colname] = bs

    output = {}
    for row in rows3:
        table_name,output_string = row
        output[table_name] = output_string

    def out(t,sort):
        return output[sort].format(t)

    binding = Binding(bs2)

    select = []
    refselect = []
    sqlfrom = []
    where = []

    options = {}
    equalities = []

    for k,v in graph.items():

#        if "#" in k:
#            for bs in context_family.bound_scales(v["sort"]):
#                conditions,refs = bs.sql(v.intent(prefix),k.split("#"))
#                where += conditions



        if "#" in k:
            for prefix,scale,columns in binding(v["sort"]):
                conditions,refs = scale.sql(v["def"].get(prefix,[]), k.split("#"), columns)
                where += conditions

                for m,value in refs.items():
                    if value in ["none","some","all","def"]:
                        options.setdefault(k,{}).setdefault(prefix,[]).append(dict(name=m,grp=value))
                    else:
                        refselect.append("{0} AS '{1}.{2}:{3}'".format(value,k,prefix,m))

        else:

            if v["marked"]:
                select.append("{0} AS {1}".format(out(k,v["sort"]),k))

            sqlfrom.append("{0} AS {1}".format(v["sort"],k))

            for prefix,scale,columns in binding(v["sort"]):
                conditions,refs = scale.sql(v["def"].get(prefix,[]),k,columns[0])
                where += conditions

                for m,value in refs.items():
                    if value in ["none","some","all","def"]:
                        options.setdefault(k,{}).setdefault(prefix,[]).append(dict(name=m,grp=value))
                    else:
                        refselect.append("{0} AS '{1}.{2}:{3}'".format(value,k,prefix,m))

            for role in [1,2]:
                for prefix,scale,columns in binding(v["sort"],role):
                    refs = scale.rolesql(k,role,columns)

                    for m,value in refs.items():
                        if value in ["none","some","all","def"]:
                            options.setdefault(k,{}).setdefault(prefix,[]).append(dict(name=m,grp=value,pos=role,sort=binding.sorts(prefix)))
                        else:
                            refselect.append("{0} AS '{1}.{2}:{3}[{4}]'".format(value,k,prefix,m,role))

    for k1,v1 in graph.items():
        for k2,v2 in graph.items():
            if v1["sort"]==v2["sort"] and "#" not in k1 and k1<k2:
                condition = "{0}={1}".format(out(k1,v1["sort"]),out(k2,v2["sort"]))
                if k2 in graph[k1]["corefs"]:
                    where.append(condition)
                    equalities.append(dict(lhs=k1,rhs=k2,grp="def"))
                else:
                    refselect.append("MIN({0})+MAX({0}) AS '{1}={2}'".format(condition,k1,k2))

    user,password = get_login()
    
    cnx = mysql.connector.connect(user=user,password=password,host=host,database=database)
    cursor = cnx.cursor()

    query = "SELECT DISTINCT " + ", ".join(select) + " FROM " + ", ".join(sqlfrom) + (" WHERE " if where else "") + " AND ".join(where)
    refquery = "SELECT " + ", ".join(refselect) + " FROM " + ", ".join(sqlfrom) + (" WHERE " if where else "") + " AND ".join(where)

    cursor.execute(query)
    rows = cursor.fetchall()
    header = [t[0] for t in cursor.description]

    if refselect:
        cursor.execute(refquery)
        row = cursor.fetchone()
        refheader = [t[0] for t in cursor.description]
        cursor.close()
        cnx.close()

    else:
        refheader = []
        row = []

    groups = ["none","some","all"]
    for field,k in izip(refheader,row):
        if "=" in field:
            lhs,rhs = field.split("=")
            equalities.append(dict(lhs=lhs,rhs=rhs,grp=groups[k]))

        else:
            nodeId,prefix,m,role = re.match(r"([#\w]+).(\w+):(\w+)(?:\[(\w+)\])?",field).groups()
            entry = dict(name=m,grp=groups[k])
            if role:
                entry["pos"]=role
                entry["sort"]=binding.sorts(prefix)
            options.setdefault(nodeId,{}).setdefault(prefix,[]).append(entry)

    result = dict(header=header,result=rows,options=options,equalities=equalities)
    return result

def create_scale(scale):
    """
    :param scale: A Scale proto-object.
    :return: The stored Scale's ID.
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    cursor.execute("INSERT INTO Scale (name,arity) VALUES (?,?)",[scale.name,scale.arity])
    cursor.execute("SELECT last_insert_rowid()")
    id = cursor.fetchone()[0]

    cnx.commit()
    cursor.close()
    cnx.close()

    return id

def create_attribute(m,scaleId):
    """
    :param m: An Attribute proto-object.
    :param scaleId: The Attribute is added to the Scale with the given ID.
    :return id: The stored Attribute's ID.
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    cursor.execute("INSERT INTO ScaleAttribute (scale,name,sqldef) VALUES (?,?,?)",[scaleId,m.name,m.sqldef])
    cursor.execute("SELECT last_insert_rowid()")
    id = cursor.fetchone()[0]

    cnx.commit()
    cursor.close()
    cnx.close()

    return id

def create_binding(cf):
    """
    :param cf: A ContextFamily proto-object.
    :return id: The stored ContextFamily's ID.
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    cursor.execute("INSERT INTO Binding (name,host,database) VALUES (?,?,?)",[cf.name,cf.host,cf.database])
    cursor.execute("SELECT last_insert_rowid()")
    id = cursor.fetchone()[0]

    cnx.commit()
    cursor.close()
    cnx.close()

    return id

def create_bound_scale(bs,bindingId):
    """
    :param bs: A BoundScale proto-object.
    :param bindingId: The BoundScale is associated to the Context Family with the given ID.
    :return: The stored BoundScale's ID.
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    cursor.execute("INSERT INTO BoundScale (scale,binding) VALUES (?,?)",[bs.scale.id,bindingId])
    cursor.execute("SELECT last_insert_rowid()")
    id = cursor.fetchone()[0]
    values = [ [id,arg,column] for arg,column in bs.args ]
    cursor.executemany("INSERT INTO BoundScaleArg (bound_scale,arg,column) VALUES (?,?,?)",values)

    cnx.commit()
    cursor.close()
    cnx.close()

    return id

def read_scales():
    """
    :return: List of all Scales.
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cursor.execute("SELECT id,name,arity FROM Scale ORDER BY arity,name")
    rows = cursor.fetchall()

    cursor.close()
    cnx.close()

    return [ Scale(*row) for row in rows ]

def read_attributes(id):
    """
    :param id: A Scale ID.
    :return: List of the Scale's Attributes.
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    cursor.execute("SELECT m.id,m.name,m.sqldef FROM Scale AS K,ScaleAttribute AS m WHERE K.id = ? AND m.scale = K.id ORDER BY m.name",[id])
    rows = cursor.fetchall()

    cursor.close()
    cnx.close()

    return [ Attribute(*row) for row in rows ]

def read_bindings():
    """
    :return: List of all Context Families.
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cursor.execute("SELECT id,name,host,database FROM Binding ORDER BY name,host,database")
    rows = cursor.fetchall()

    cursor.close()
    cnx.close()

    return [ ContextFamily(*row) for row in rows ]

def read_bound_scales(bindingId):
    """
    :param bindingId: A ContextFamily ID.
    :return: List of all BoundScales for the given ContextFamily.
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    query="SELECT K.id,S.id,S.name,S.arity,y.arg,y.column FROM Scale AS S,BoundScale AS K,BoundScaleArg AS y " \
          + "WHERE K.scale=S.id AND K.binding=? AND K.id=y.bound_scale"
    cursor.execute(query,[bindingId])
    rows = cursor.fetchall()

    cursor.close()
    cnx.close()

    bound_scales = {}
    for row in rows:
        id = row[0]
        if id in bound_scales:
            bound_scales[id].args.append(row[4:6])
        else:
            bound_scales[id] = BoundScale(row[0],Scale(*row[1:4]),[row[4:6]])

    for bs in bound_scales.values():
        bs.args = sorted(bs.args,key=lambda x:x[0])

    return sorted([ bs for bs in bound_scales.values() ],key=lambda x:x.scale.name)

def read_sorts(id):
    """
    Returns all table names (aka sorts) in the context family's underlying database. Table names are read both from the database itself and from an
    app-specific configuration database which stores info about the context family. If a table name in the configuration database can't be found
    in the context family's underlying database (this is expected if the schema has changed), the corresponding sort is flagged invalid by
    assigning it an empty string ID. Otherwise, the sort ID is the table_name.

    :param id: A Context Family ID.
    :return: List of available Sorts.
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    cursor.execute("SELECT host,database FROM Binding WHERE id = ?",[id])
    host,database = cursor.fetchone()

    cursor.execute("SELECT id,table_name,column_name FROM BindingOutput WHERE binding = ?",[id])
    rows = cursor.fetchall()

    cursor.close()
    cnx.close()


    cnx = None
    cursor = None
    tables = []

    try:
        user,password = get_login()
        cnx = mysql.connector.connect(user=user,password=password,host=host,database=database)
        cursor = cnx.cursor()
        cursor.execute("SHOW TABLES")
        tables = [ t[0] for t in cursor.fetchall() ]

    except mysql.connector.InterfaceError:
        return []

    finally:
        if cursor is not None:
            cursor.close()
        if cnx is not None:
            cnx.close()

    stored_sorts = [ Sort(row[1],*row[1:]) for row in rows ]
    stored_tables = [ s.name for s in stored_sorts ]

    other_tables = list(set(tables)-set(stored_tables))
    other_sorts = [ Sort(t,t,"") for t in other_tables ]

    invalid_tables = list(set(stored_tables)-set(tables))
    for s in stored_sorts:
        if s.name in invalid_tables:
            s.id = ""

    return sorted(stored_sorts + other_sorts,key=lambda s:s.name)

def table_columns(bindingId,table):
    """
    :param bindingId: A Context Family ID.
    :param table: A table name in the context family's underlying database.
    :return: List of all table columns and their SQL types.
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cursor.execute("SELECT host,database FROM Binding WHERE id = ?",[bindingId])
    host,database = cursor.fetchone()

    cursor.close()
    cnx.close()

    cnx = None
    cursor = None
    rows = []

    try:
        user,password = get_login()
        cnx = mysql.connector.connect(user=user,password=password,host=host,database="information_schema")
        cursor = cnx.cursor()
        cursor.execute("SELECT column_name,data_type FROM columns WHERE table_schema=%(db)s AND table_name=%(table)s",dict(db=database,table=table))
        rows = cursor.fetchall()

    # dealing with bogus hostnames
    except mysql.connector.InterfaceError:
        return []

    finally:
        if cursor is not None:
            cursor.close()
        if cnx is not None:
            cnx.close()

    return [ SQLColumn(row[0],*row) for row in rows ]

def update_scale(scale):
    """
    :param scale: A Scale object.
    :return: The Scale's ID (if update was successful).
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    cursor.execute("UPDATE Scale SET name=?,arity=? WHERE id=?",[scale.name,scale.arity,scale.id])

    cnx.commit()
    cursor.close()
    cnx.close()

    return scale.id

def update_attribute(m):
    """
    :param m: An Attribute object.
    :return: The Attribute's ID (if update was successful).
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    cursor.execute("UPDATE ScaleAttribute SET name=?,sqldef=? WHERE id=?",[m.name,m.sqldef,m.id])

    cnx.commit()
    cursor.close()
    cnx.close()

    return m.id

def update_binding(cf):
    """
    :param cf: A ContextFamily object.
    :return: The ContextFamily's ID (if update was successful).
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    cursor.execute("UPDATE Binding SET name=?,host=?,database=? WHERE id=?",[cf.name,cf.host,cf.database,cf.id])

    cnx.commit()
    cursor.close()
    cnx.close()

    return cf.id

def update_sort(sort,bindingId):
    """
    :param sort: A Sort object.
    :param bindingId: The ID of the ContextFamily which defines the sort.
    :return: The Sort's ID (if update was successful).
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    query = "INSERT OR REPLACE INTO BindingOutput (id,binding,table_name,column_name) VALUES "\
            + "((SELECT id FROM BindingOutput WHERE binding=? AND table_name=?),?,?,?);"
    cursor.execute(query,[bindingId,sort.name,bindingId,sort.name,sort.output])

    cnx.commit()
    cursor.close()
    cnx.close()

    return sort.id

def delete_scale(id):
    """
    :param id: A Scale ID.
    :return: The same Scale ID (if deletion was successful).
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cursor.execute("DELETE FROM Scale WHERE id=?",[str(id)])
    cursor.execute("DELETE FROM ScaleAttribute WHERE scale=?",[str(id)])

    cnx.commit()
    cursor.close()
    cnx.close()

    return id

def delete_attribute(id):
    """
    :param id: An Attribute ID.
    :return: The same Attribute ID (if deletion was successful).
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cursor.execute("DELETE FROM ScaleAttribute WHERE id=?",[str(id)])

    cnx.commit()
    cursor.close()
    cnx.close()

    return id

def delete_binding(id):
    """
    :param id: A ContextFamily ID.
    :return: The same ContextFamily ID (if deletion was successful).
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cursor.execute("DELETE FROM Binding WHERE id=?",[str(id)])
    cursor.execute("DELETE FROM BoundScaleArg WHERE bound_scale IN (SELECT id FROM BoundScale WHERE binding=?)",[str(id)])
    cursor.execute("DELETE FROM BoundScale WHERE binding=?",[str(id)])
    cursor.execute("DELETE FROM BindingOutput WHERE binding=?",[str(id)])

    cnx.commit()
    cursor.close()
    cnx.close()

    return id


def delete_bound_scale(id):
    """
    :param id: A BoundScale ID.
    :return: The same BoundScale ID (if deletion was successful).
    """
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cursor.execute("DELETE FROM BoundScale WHERE id=?",[str(id)])
    cursor.execute("DELETE FROM BoundScaleArg WHERE bound_scale=?",[str(id)])

    cnx.commit()
    cursor.close()
    cnx.close()

    return id

urls = [
    # GET methods

    URL(r'read_scales/?$',read_scales),
    URL(r'read_attributes/?$',read_attributes,["id"]),
    URL(r'read_bindings/?$',read_bindings),
    URL(r'read_bound_scales/?$',read_bound_scales,["id"]),
    URL(r'read_sorts/?$',read_sorts,["id"]),
    URL(r'table_columns/?$',table_columns,["bindingId","table"]),

    # POST methods

    URL(r'create_scale/?$',create_scale,[Scale]),
    URL(r'update_scale/?$',update_scale,[Scale]),
    URL(r'delete_scale/?$',delete_scale,[None]),

    URL(r'create_attribute/?$',create_attribute,[Attribute,None]),
    URL(r'update_attribute/?$',update_attribute,[Attribute]),
    URL(r'delete_attribute/?$',delete_attribute,[None]),

    URL(r'create_binding/?$',create_binding,[ContextFamily]),
    URL(r'update_binding/?$',update_binding,[ContextFamily]),
    URL(r'delete_binding/?$',delete_binding,[None]),

    URL(r'create_bound_scale/?$',create_bound_scale,[BoundScale,None]),
    URL(r'delete_bound_scale/?$',delete_bound_scale,[None]),

    URL(r'update_sort/?$',update_sort,[Sort,None]),

    # called as a POST method despite no data being changed (payload is the query graph in JSON)
    URL(r'solve/?$',solve,[None,None]),
]

# Set content-type header even if no content is returned! The reason is Firefox bug 521301.
def dispatch(environ,start_response):
    path = environ.get('PATH_INFO','').lstrip('/')
    type = environ['REQUEST_METHOD']

    # index.html
    if re.search(r'^$',path):
        basedir = os.path.dirname(os.path.realpath(__file__))
        h = open(os.path.join(basedir,"index.html"),"r")
        content = h.read()
        h.close()

        start_response("200 OK",[("Content-type","text/html")])
        return [content]

    # static content
    match = re.search(r'script/(.+)$',path)
    if match is not None:
        basedir = os.path.dirname(os.path.realpath(__file__))
        filename = match.groups()[0]
        h = open(os.path.join(basedir,"script",filename),"r")
        content = h.read()
        h.close()

        start_response("200 OK",[("Content-type","text/plain")])
        return [content]

    url = None
    for _url in urls:
        match = re.search(_url.pattern,path)
        if match is not None:
            url = _url
            break

    # error 404
    if url is None:
        start_response("404 NOT FOUND",[("Content-type","text/plain")])
        return ["Not Found"]

    # json api
    elif type == 'GET':
        get = cgi.parse_qs(environ["QUERY_STRING"])
        args = [ cgi.escape(get[param][0]) for param in url.schema ]
        output = ObjectEncoder().encode(url.method(*args))
        headers = [("Content-type","application/json"),("Content-Length",str(len(output)))]
        start_response("200 OK",headers)
        return [output]

    elif type == 'POST':
        n = int(environ.get("CONTENT_LENGTH",0))
        data = environ['wsgi.input'].read(n)
        args = ObjectDecoder().decode(data,url.schema)
        output = ObjectEncoder().encode(url.method(*args))
        headers = [("Content-type","application/json"),("Content-Length",str(len(output)))]
        start_response("200 OK",headers)
        return [output]


def application(environ,start_response):
    return dispatch(environ,start_response)

if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    srv = make_server('0.0.0.0',8081,application)
    srv.serve_forever()



