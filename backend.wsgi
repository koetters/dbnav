import re
import os
import json
import mysql.connector
from itertools import izip
import sqlite3

class UnaryScale(object):

    def __init__(self,name,atts):
        self.name = name
        self.atts = dict(atts)

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

class Bindings(object):

    def __init__(self):
        self._bindings = {
            "pubdate": (centuryScale,["book.publication_date"]),
            "nationality": (countryScale,["author.nationality"]),
            "birthdate": (centuryScale,["author.date_of_birth"]),
            "bookauthor": (foreignKeyScale,["book.author","author.name"]),
        }

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

centuryScale = UnaryScale("century", {
    "19th": "{0} BETWEEN '1800-01-01' AND '1899-12-31'",
    "20th": "{0} BETWEEN '1900-01-01' AND '1999-12-31'",
    "21st": "{0} BETWEEN '2000-01-01' AND '2099-12-31'",
})

countryScale = UnaryScale("country", {
    "USA": "{0}='American'",
    "GB": "{0}='British'",
})

foreignKeyScale = BinaryScale("foreignKey", {
    "author": "{0}={1}",
})

bindings = Bindings()

output = {
    "author": "{0}.name",
    "book": "{0}.title"
}

def out(t,sort):
    return output[sort].format(t)

def dbinfo():
    basedir = os.path.dirname(os.path.realpath(__file__))
    cnx = sqlite3.connect(os.path.join(basedir,"db.sqlite"))
    cursor = cnx.cursor()
    cursor.execute("SELECT * FROM connection")
    header = [ t[0] for t in cursor.description ]
    row = cursor.fetchone()

    cursor.close()
    cnx.close()

    return dict(zip(header,row))

def _solve(data):

    graph = json.loads(data)
    if not graph:
        return json.dumps(dict(header=[],result=[],options={},equalities=[]))

    select = []
    refselect = []
    sqlfrom = []
    where = []

    options = {}
    equalities = []

    for k,v in graph.items():

        if "#" in k:
            for prefix,scale,columns in bindings(v["sort"]):
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

            for prefix,scale,columns in bindings(v["sort"]):
                conditions,refs = scale.sql(v["def"].get(prefix,[]),k,columns[0])
                where += conditions

                for m,value in refs.items():
                    if value in ["none","some","all","def"]:
                        options.setdefault(k,{}).setdefault(prefix,[]).append(dict(name=m,grp=value))
                    else:
                        refselect.append("{0} AS '{1}.{2}:{3}'".format(value,k,prefix,m))

            for role in [1,2]:
                for prefix,scale,columns in bindings(v["sort"],role):
                    refs = scale.rolesql(k,role,columns)

                    for m,value in refs.items():
                        if value in ["none","some","all","def"]:
                            options.setdefault(k,{}).setdefault(prefix,[]).append(dict(name=m,grp=value,pos=role,sort=bindings.sorts(prefix)))
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

    info = dbinfo()
    cnx = mysql.connector.connect(**info)
    cursor = cnx.cursor()

    query = "SELECT DISTINCT " + ", ".join(select) + " FROM " + ", ".join(sqlfrom) + (" WHERE " if where else "") + " AND ".join(where)
    cursor.execute(query)
    rows = cursor.fetchall()
    header = [t[0] for t in cursor.description]

    refquery = "SELECT " + ", ".join(refselect) + " FROM " + ", ".join(sqlfrom) + (" WHERE " if where else "") + " AND ".join(where)
    cursor.execute(refquery)
    row = cursor.fetchone()
    refheader = [t[0] for t in cursor.description]
    cursor.close()
    cnx.close()

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
                entry["sort"]=bindings.sorts(prefix)
            options.setdefault(nodeId,{}).setdefault(prefix,[]).append(entry)

    response = json.dumps(dict(header=header,result=rows,options=options,equalities=equalities))
    return(response)

def _get_sorts():

    info = dbinfo()
    cnx = mysql.connector.connect(**info)
    cursor = cnx.cursor()
    cursor.execute("SHOW TABLES")
    rows = cursor.fetchall()
    sorts = [t[0] for t in rows]

    cursor.close()
    cnx.close()

    return json.dumps(sorts)

def index(environ,start_response):

    basedir = os.path.dirname(os.path.realpath(__file__))
    h = open(os.path.join(basedir,"index.html"),"r")
    content = h.read()
    h.close()

    start_response("200 OK",[("Content-type","text/html")])
    return [content]

def solve(environ,start_response):

    n = int(environ.get("CONTENT_LENGTH",0))

    status = "200 OK"
    # output = ["%s: %s" % (key,value) for key,value in sorted(environ.items())]
    # output = "\n".join(output)
    data = environ['wsgi.input'].read(n)
    output = _solve(data)
    headers = [("Content-type","application/json"),("Content-Length",str(len(output)))]
    start_response(status,headers)
    return [output]

def get_sorts(environ,start_response):
    status = "200 OK"
    output = _get_sorts()
    headers = [("Content-type","application/json"),("Content-Length",str(len(output)))]
    start_response(status,headers)
    return [output]

def not_found(environ,start_response):
    start_response("404 NOT FOUND",[("Content-type","text/plain")])
    return ["Not Found"]

urls = [
    (r'^$',index),
    (r'solve/?$',solve),
    (r'get_sorts/?$',get_sorts)
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
