import os
import difflib
from dbnav import serialization
from dbnav.graph import Graph, Point

def run():
    basedir = os.path.dirname(os.path.realpath(__file__))
    fname = os.path.join(basedir,os.pardir,"resources","bindings","Literature.json")
    with open(fname) as fp:
        json1 = fp.read()

    pcf = serialization.loads(json1)

    json2 = serialization.dumps(pcf)
    print("Test1: PowerContextfamily JSON decode/encode")
    if json1 == json2:
        print("*** Success ***")
    else:
        print("*** Failure ***")
        iter_diff = difflib.unified_diff(json1.splitlines(True),json2.splitlines(True),lineterm="\n")
        for chunk in iter_diff:
            print(chunk, end="")

    graph1 = Graph(pcf)
    x1 = graph1.add_node("Author",Point(100,100))
    graph1.nodes[x1].display.add("m2")
    graph1.nodes[x1].display.add("m3")
    graph1.add_rnode("m5",[x1],Point(200,200))
    json3 = serialization.dumps(graph1)
    graph2 = serialization.loads(json3)
    json4 = serialization.dumps(graph2)
    print("Test2: Graph JSON decode/encode")
    if json3 == json4:
        print("*** Success ***")
    else:
        print("*** Failure ***")
        iter_diff = difflib.unified_diff(json3.splitlines(True),json4.splitlines(True),lineterm="\n")
        for chunk in iter_diff:
            print(chunk, end="")



if __name__ == "__main__":
    run()
