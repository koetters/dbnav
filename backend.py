import re
import os
import json
import mysql.connector

def _solve(data):

    user = "root"
    password = "password"
    host = "localhost"
    database = "literature_pcf"

    graph = json.loads(data)
    if not graph:
        return json.dumps(dict(header=[],result=[],options={},equalities=[]))

    nodes = {}
    rnodes = {}

    for k,v in graph.items():
        if "#" in k:
            rnodes[k] = v
        else:
            nodes[k] = v

    def name(str):
        return "t"+str.replace("#","_")

    def inv_name(str):
        return str[1:].replace("_","#")

    cnx = mysql.connector.connect(user=user,password=password,host=host,database=database)
    cursor = cnx.cursor()

    sql0 = "SELECT table_name,column_name FROM information_schema.columns "\
           "WHERE table_schema='{0}' AND column_name NOT RLIKE '^(p[0-9]*|id)$'".format(database)
    cursor.execute(sql0)
    rows = cursor.fetchall()

    signature = {}
    for row in rows:
        sort,m = row
        if sort in signature:
            signature[sort] += [m]
        else:
            signature[sort] = [m]

    # sql1 = "SELECT table_name,column_name,referenced_table_name,referenced_column_name FROM information_schema.key_column_usage " \
    #        "WHERE table_schema='{0}' AND referenced_table_schema='{0}'".format(database)
    # cursor.execute(sql1)
    # rows = cursor.fetchall()

    select = ["{0} AS {1}".format(name(k)+".id",name(k)) for k,v in nodes.items() if v["marked"]]
    select2 = ["MIN({0}.{1}), MAX({0}.{1})".format(name(k),m) for k,v in graph.items() for m in signature[v["sort"]]]
    select3 = ["MIN({0}.id={1}.id), MAX({0}.id={1}.id)".format(name(k1),name(k2)) for k1 in nodes.keys() for k2 in nodes.keys() if k1<k2]
    sqlfrom = ["{0} AS {1}".format(v["sort"],name(k)) for k,v in graph.items()]
    where1 = ["{0}.p{1}={2}.id".format(name(s),str(j+1),name(k)) for s in rnodes for j,k in enumerate(s.split("#"))]
    where2 = ["{0}.{1}=1".format(name(k),m) for k,v in graph.items() for m in v["def"]]
    where3 = ["{0}.id={1}.id".format(name(k1),name(k2)) for k1,v in nodes.items() for k2 in v["corefs"] if k1<k2]
    where = where1 + where2 + where3

    sql2 = "SELECT DISTINCT " + ", ".join(select) + " FROM " + ", ".join(sqlfrom) + (" WHERE " if where else "") + " AND ".join(where)
    cursor.execute(sql2)
    rows = cursor.fetchall()
    header = [inv_name(t[0]) for t in cursor.description]

    sql3 = "SELECT " + ", ".join(select2+select3) + " FROM " + ", ".join(sqlfrom) + (" WHERE " if where else "") + " AND ".join(where)
    cursor.execute(sql3)
    row = cursor.fetchone()
    header2 = [t[0] for t in cursor.description]
    cursor.close()
    cnx.close()

    options = {}
    equalities = []

    for i in range(0,len(header2),2):

        match = re.search(r"MIN\(([^.]+)\.id=([^.]+)\.id\)",header2[i])
        if match:
            nodeId1 = inv_name(match.group(1))
            nodeId2 = inv_name(match.group(2))

            k = row[i]+row[i+1]
            f = ["none","some","all"]
            grp = f[k]

            if nodeId1 in graph[nodeId2]["corefs"]:
                grp = "def"

            if grp != "none":
                equalities += [ dict(lhs=nodeId1,rhs=nodeId2,grp=grp) ]

        else:
            match = re.search(r"MIN\(([^.]+)\.([^)]+)\)",header2[i])
            nodeId = inv_name(match.group(1))
            m = match.group(2)

            k = row[i]+row[i+1]
            f = ["none","some","all"]
            grp = f[k]

            if m in graph[nodeId]["def"]:
                grp = "def"

            if "$" in m:
                sort,rest = m.split("$")
                rel,pos = rest.rsplit("_",1)
                entry = dict(name=rel,pos=pos,sort=sort,grp=grp)
            else:
                entry = dict(name=m,grp=grp)

            if nodeId in options:
                options[nodeId] += [entry]
            else:
                options[nodeId] = [entry]

    response = json.dumps(dict(header=header,result=rows,options=options,equalities=equalities))
    return(response)

def _get_sorts():

    user = "root"
    password = "password"
    host = "localhost"
    database = "literature_pcf"

    cnx = mysql.connector.connect(user=user,password=password,host=host,database=database)
    cursor = cnx.cursor()

    sql0 = "SELECT table_name,column_name FROM information_schema.columns "\
           "WHERE table_schema='{0}' AND column_name='id'".format(database)
    cursor.execute(sql0)
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
