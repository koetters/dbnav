import re
import os
import json
import cgi
import sqlite3
import mysql.connector
from itertools import izip

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

def _solve(data):

    data = json.loads(data)
    graph = data["graph"]
    bindingId = data["binding"]

    if not graph:
        return dict(header=[],result=[],options={},equalities=[])

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    cursor.execute("SELECT K.id,K.name,K.arity,m.name,m.sqldef FROM Scale as K,ScaleAttribute AS m WHERE m.scale = K.id AND K.id IN (SELECT scale FROM BoundScale WHERE binding = ?)",[str(bindingId)])
    rows1 = cursor.fetchall()

    cursor.execute("SELECT K.name,K.scale,y.arg,y.column FROM BoundScale AS K,BoundScaleArg AS y WHERE y.bound_scale = K.id AND K.binding = ?",[str(bindingId)])
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
        name,scaleId,arg,column = row
        if name in bound_scales:
            bound_scales[name][1].append((arg,column))

        else:
            bound_scales[name] = [scales[scaleId],[(arg,column)]]

    for pair in bound_scales.values():
        pair[1] = [ column for arg,column in sorted(pair[1],key=lambda x:x[0]) ]

    output = {}
    for row in rows3:
        table_name,column_name = row
        output[table_name] = "{{0}}.{0}".format(column_name)

    def out(t,sort):
        return output[sort].format(t)

    binding = Binding(bound_scales)

    select = []
    refselect = []
    sqlfrom = []
    where = []

    options = {}
    equalities = []

    for k,v in graph.items():

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

    print(query)
    print(refquery)

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

def _get_columns(bindingId,host,database):

    cnx = None
    cursor = None
    rows = []

    try:
        user,password = get_login()
        cnx = mysql.connector.connect(user=user,password=password,host=host,database="information_schema")
        cursor = cnx.cursor()
        cursor.execute("SELECT table_name,column_name FROM columns WHERE table_schema=%(db)s",dict(db=database))
        rows = cursor.fetchall()

    except mysql.connector.InterfaceError:
        return []

    finally:
        if cursor is not None:
            cursor.close()
        if cnx is not None:
            cnx.close()

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cursor.execute("SELECT table_name,column_name FROM BindingOutput WHERE binding=?",[bindingId])
    output_columns = cursor.fetchall()

    cursor.close()
    cnx.close()

    rows = [ (table,column,(table,column) in output_columns) for table,column in rows ]
    return rows

def _get_sorts(bindingId):

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cursor.execute("SELECT name,host,database FROM Binding WHERE id=?",[bindingId])
    row = cursor.fetchone()

    cursor.close()
    cnx.close()

    name = row[0]
    host = row[1]
    database = row[2]
    user,password = get_login()

    cnx = mysql.connector.connect(user=user,password=password,host=host,database=database)
    cursor = cnx.cursor()
    cursor.execute("SHOW TABLES")
    rows = cursor.fetchall()
    sorts = [t[0] for t in rows]

    cursor.close()
    cnx.close()

    return sorts

def _get_scales():

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cursor.execute("SELECT id,name,arity FROM Scale")
    rows = cursor.fetchall()

    cursor.close()
    cnx.close()

    return rows

def _describe_scale(id):

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    cursor.execute("SELECT m.id,m.name,m.sqldef FROM Scale AS K,ScaleAttribute AS m WHERE K.id = ? AND m.scale = K.id",[id])
    rows = cursor.fetchall()

    cursor.close()
    cnx.close()

    return rows


def _get_bindings():

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cursor.execute("SELECT id,name,host,database FROM Binding")
    rows = cursor.fetchall()

    cursor.close()
    cnx.close()

    return rows

def _describe_binding(id):

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    query = "SELECT K.id,K.name,K.scale,y.arg,y.column FROM BoundScale AS K, BoundScaleArg AS y WHERE K.binding=? AND K.id=y.bound_scale"
    cursor.execute(query,[id])
    rows = cursor.fetchall()

    cursor.close()
    cnx.close()

    bound_scales = {}
    for row in rows:
        bound_scales.setdefault(row[0],{"name":row[1],"scale":row[2],"bindings":[]})["bindings"].append([row[3],row[4]])

    return bound_scales

def _write_bound_scale(data):
    bound_scale = json.loads(data)
    id = bound_scale["id"]
    name = bound_scale["name"]
    scaleId = bound_scale["scaleId"]
    bindingId = bound_scale["bindingId"]
    bindings = bound_scale["bindings"]

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    if id == "":
        cursor.execute("INSERT INTO BoundScale (name,scale,binding) VALUES (?,?,?)",[name,scaleId,bindingId])
        cursor.execute("SELECT last_insert_rowid()")
        id = cursor.fetchone()[0]
        values = [ [id,arg,column] for arg,column in bindings.items() ]
        cursor.executemany("INSERT INTO BoundScaleArg (bound_scale,arg,column) VALUES (?,?,?)",values)
    else:
        cursor.execute("UPDATE BoundScale SET name=? WHERE id=?",[name,id])
        for arg,column in bindings.items():
            cursor.execute("UPDATE BoundScaleArg SET column=? WHERE bound_scale=? AND arg=?",[column,id,arg])

    cnx.commit()
    cursor.close()
    cnx.close()

    return id

def _write_scale(data):
    scale = json.loads(data)
    id = scale["id"]
    name = scale["name"]
    arity = scale["arity"]

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    if id == "":
        cursor.execute("INSERT INTO Scale (name,arity) VALUES (?,?)",[name,arity])
        cursor.execute("SELECT last_insert_rowid()")
        id = cursor.fetchone()[0]
    else:
        cursor.execute("UPDATE Scale SET name=?,arity=? WHERE id=?",[name,arity,id])

    cnx.commit()
    cursor.close()
    cnx.close()

    return id

def _write_binding(name,host,database,id=""):

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    if id == "":
        cursor.execute("INSERT INTO Binding (name,host,database) VALUES (?,?,?)",[name,host,database])
        cursor.execute("SELECT last_insert_rowid()")
        id = cursor.fetchone()[0]
    else:
        cursor.execute("UPDATE Binding SET name=?,host=?,database=? WHERE id=?",[name,host,database,id])

    cnx.commit()
    cursor.close()
    cnx.close()

    return id

def _delete_scale(data):

    id = str(json.loads(data))

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cnx.execute("DELETE FROM Scale WHERE id=?",[id])
    cnx.execute("DELETE FROM ScaleAttribute WHERE scale=?",[id])

    cnx.commit()
    cursor.close()
    cnx.close()

def _delete_attribute(data):

    id = str(json.loads(data))

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cnx.execute("DELETE FROM ScaleAttribute WHERE id=?",[id])

    cnx.commit()
    cursor.close()
    cnx.close()

def _delete_binding(data):

    id = str(json.loads(data))

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cnx.execute("DELETE FROM Binding WHERE id=?",[id])
    cnx.execute("DELETE FROM BoundScaleArg WHERE bound_scale IN (SELECT id FROM BoundScale WHERE binding=?)",[id])
    cnx.execute("DELETE FROM BoundScale WHERE binding=?",[id])
    cnx.execute("DELETE FROM BindingOutput WHERE binding=?",[id])

    cnx.commit()
    cursor.close()
    cnx.close()

def _delete_bound_scale(data):

    id = str(json.loads(data))

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cnx.execute("DELETE FROM BoundScale WHERE id=?",[id])
    cnx.execute("DELETE FROM BoundScaleArg WHERE bound_scale=?",[id])

    cnx.commit()
    cursor.close()
    cnx.close()

def _write_attribute(scale,name,sqldef,id=""):

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()

    if id == "":
        cursor.execute("INSERT INTO ScaleAttribute (scale,name,sqldef) VALUES (?,?,?)",[scale,name,sqldef])
        cursor.execute("SELECT last_insert_rowid()")
        id = cursor.fetchone()[0]
    else:
        cursor.execute("UPDATE ScaleAttribute SET name=?,sqldef=? WHERE id=?",[name,sqldef,id])

    cnx.commit()
    cursor.close()
    cnx.close()

    return id

def _add_output_column(data):
    output_column = json.loads(data)
    binding = output_column["binding"]
    table = output_column["table"]
    column = output_column["column"]

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cursor.execute("INSERT INTO BindingOutput (binding,table_name,column_name) VALUES (?,?,?)",[binding,table,column])
    cnx.commit()

    cursor.close()
    cnx.close()

def _remove_output_column(data):
    output_column = json.loads(data)
    binding = output_column["binding"]
    table = output_column["table"]
    column = output_column["column"]

    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cursor.execute("DELETE FROM BindingOutput WHERE binding=? AND table_name=? AND column_name=?",[binding,table,column])
    cnx.commit()

    cursor.close()
    cnx.close()

def index(environ,start_response):

    basedir = os.path.dirname(os.path.realpath(__file__))
    h = open(os.path.join(basedir,"index.html"),"r")
    content = h.read()
    h.close()

    start_response("200 OK",[("Content-type","text/html")])
    return [content]

def get_columns(environ,start_response):

    params = cgi.parse_qs(environ["QUERY_STRING"])
    bindingId = cgi.escape(params["binding"][0])
    host = cgi.escape(params["host"][0])
    database = cgi.escape(params["database"][0])

    status = "200 OK"
    output = json.dumps(_get_columns(bindingId,host,database))
    headers = [("Content-type","application/json"),("Content-Length",str(len(output)))]
    start_response(status,headers)
    return [output]

def solve(environ,start_response):

    status = "200 OK"
    n = int(environ.get("CONTENT_LENGTH",0))
    data = environ['wsgi.input'].read(n)
    output = json.dumps(_solve(data))
    headers = [("Content-type","application/json"),("Content-Length",str(len(output)))]
    start_response(status,headers)
    return [output]

def get_sorts(environ,start_response):

    params = cgi.parse_qs(environ["QUERY_STRING"])
    bindingId = cgi.escape(params["binding"][0])

    status = "200 OK"
    output = json.dumps(_get_sorts(bindingId))
    headers = [("Content-type","application/json"),("Content-Length",str(len(output)))]
    start_response(status,headers)
    return [output]

# this is not very elegant. the idea is to combine two Ajax requests into one.
# better method: compute initial state server-side (no Ajax request at all then)
def get_sorts_and_bindings(environ,start_response):

    status = "200 OK"
    bindings = _get_bindings()
    if bindings:
        bindingId = bindings[0][0]
        sorts = _get_sorts(bindingId)
    else:
        bindingId = ""
        sorts = []
    output = json.dumps(dict(bindingId=bindingId,sorts=sorts,bindings=bindings))
    headers = [("Content-type","application/json"),("Content-Length",str(len(output)))]
    start_response(status,headers)
    return [output]

def get_scales(environ,start_response):
    status = "200 OK"
    output = json.dumps(_get_scales())
    headers = [("Content-type","application/json"),("Content-Length",str(len(output)))]
    start_response(status,headers)
    return [output]

def describe_scale(environ,start_response):

    params = cgi.parse_qs(environ["QUERY_STRING"])
    id = cgi.escape(params["id"][0])

    status = "200 OK"
    output = json.dumps(_describe_scale(id))
    headers = [("Content-type","application/json"),("Content-Length",str(len(output)))]
    start_response(status,headers)
    return [output]

def write_scale(environ,start_response):

    n = int(environ.get("CONTENT_LENGTH",0))
    data = environ['wsgi.input'].read(n)
    output = json.dumps(_write_scale(data))
    start_response("200 OK",[("Content-type","application/json")])
    return [output]

def delete_scale(environ,start_response):

    n = int(environ.get("CONTENT_LENGTH",0))
    data = environ['wsgi.input'].read(n)
    _delete_scale(data)
    start_response("200 OK",[("Content-type","text/plain")])
    return ["Success"]

def delete_attribute(environ,start_response):

    n = int(environ.get("CONTENT_LENGTH",0))
    data = environ['wsgi.input'].read(n)
    _delete_attribute(data)
    start_response("200 OK",[("Content-type","text/plain")])
    return ["Success"]

def delete_binding(environ,start_response):

    n = int(environ.get("CONTENT_LENGTH",0))
    data = environ['wsgi.input'].read(n)
    _delete_binding(data)
    start_response("200 OK",[("Content-type","text/plain")])
    return ["Success"]

def delete_bound_scale(environ,start_response):

    n = int(environ.get("CONTENT_LENGTH",0))
    data = environ['wsgi.input'].read(n)
    _delete_bound_scale(data)
    start_response("200 OK",[("Content-type","text/plain")])
    return ["Success"]

def get_bindings(environ,start_response):
    status = "200 OK"
    output = json.dumps(_get_bindings())
    headers = [("Content-type","application/json"),("Content-Length",str(len(output)))]
    start_response(status,headers)
    return [output]

def describe_binding(environ,start_response):

    params = cgi.parse_qs(environ["QUERY_STRING"])
    id = cgi.escape(params["id"][0])

    status = "200 OK"
    output = json.dumps(_describe_binding(id))
    headers = [("Content-type","application/json"),("Content-Length",str(len(output)))]
    start_response(status,headers)
    return [output]

def write_attribute(environ,start_response):

    params = cgi.parse_qs(environ["QUERY_STRING"])
    id = cgi.escape(params.get("id",[""])[0])
    scale = cgi.escape(params["scale"][0])
    name = cgi.escape(params["name"][0])
    sqldef = cgi.escape(params["sqldef"][0])
    output = json.dumps(_write_attribute(scale,name,sqldef,id))

    headers = start_response("200 OK",[("Content-type","application/json")])
    return [output]

def write_binding(environ,start_response):

    params = cgi.parse_qs(environ["QUERY_STRING"])
    id = cgi.escape(params.get("id",[""])[0])
    name = cgi.escape(params["name"][0])
    host = cgi.escape(params["host"][0])
    database = cgi.escape(params["database"][0])
    output = json.dumps(_write_binding(name,host,database,id))

    # Setting content-type header even though no content is returned. The reason is Firefox bug 521301.
    start_response("200 OK",[("Content-type","application/json")])
    return [output]

def write_bound_scale(environ,start_response):

    n = int(environ.get("CONTENT_LENGTH",0))
    data = environ['wsgi.input'].read(n)
    output = json.dumps(_write_bound_scale(data))
    start_response("200 OK",[("Content-type","application/json")])
    return [output]

def add_output_column(environ,start_response):

    n = int(environ.get("CONTENT_LENGTH",0))
    data = environ['wsgi.input'].read(n)
    _add_output_column(data)
    start_response("200 OK",[("Content-type","text/plain")])
    return ["Success"]

def remove_output_column(environ,start_response):

    n = int(environ.get("CONTENT_LENGTH",0))
    data = environ['wsgi.input'].read(n)
    _remove_output_column(data)
    start_response("200 OK",[("Content-type","text/plain")])
    return ["Success"]

def not_found(environ,start_response):
    start_response("404 NOT FOUND",[("Content-type","text/plain")])
    return ["Not Found"]

urls = [
    (r'^$',index),
    (r'get_columns/?$',get_columns),
    (r'solve/?$',solve),
    (r'get_scales/?$',get_scales),
    (r'describe_scale/?$',describe_scale),
    (r'write_scale/?$',write_scale),
    (r'delete_scale/?$',delete_scale),
    (r'write_attribute/?$',write_attribute),
    (r'delete_attribute/?$',delete_attribute),
    (r'get_sorts/?$',get_sorts),
    (r'get_bindings/?$',get_bindings),
    (r'get_sorts_and_bindings/?$',get_sorts_and_bindings),
    (r'write_binding/?$',write_binding),
    (r'describe_binding/?$',describe_binding),
    (r'delete_binding/?$',delete_binding),
    (r'write_bound_scale/?$',write_bound_scale),
    (r'delete_bound_scale/?$',delete_bound_scale),
    (r'add_output_column/?$',add_output_column),
    (r'remove_output_column/?$',remove_output_column),
]

def application(environ,start_response):
    path = environ.get('PATH_INFO','').lstrip('/')
    for regex,callback in urls:
        match = re.search(regex,path)
        if match is not None:
            return callback(environ,start_response)

    return not_found(environ,start_response)

if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    srv = make_server('localhost',8081,application)
    srv.serve_forever()
