import os
import difflib
from dbnav import serialization
from dbnav.graph import Graph, Point
from dbnav.faceted_pcf import FacetedPowerContextFamily


def test1():
    basedir = os.path.dirname(os.path.realpath(__file__))
    fname = os.path.join(basedir, os.pardir, "resources", "bindings", "Literature.json")
    with open(fname) as fp:
        json1 = fp.read()

    pcf = serialization.loads(json1)

    json2 = serialization.dumps(pcf)
    print("Test1.1: DBContextfamily JSON decode/encode")
    if json1 == json2:
        print("*** Success ***")
    else:
        print("*** Failure ***")
        iter_diff = difflib.unified_diff(json1.splitlines(True), json2.splitlines(True), lineterm="\n")
        for chunk in iter_diff:
            print(chunk, end="")

    graph1 = Graph(pcf)
    x1 = graph1.add_node("Author", Point(100, 100))
    graph1.nodes[x1].display.add("m2")
    graph1.nodes[x1].display.add("m3")
    graph1.add_rnode("m5", [x1], Point(200, 200))
    json3 = serialization.dumps(graph1)
    graph2 = serialization.loads(json3)
    json4 = serialization.dumps(graph2)
    print("Test1.2: Graph JSON decode/encode")
    if json3 == json4:
        print("*** Success ***")
    else:
        print("*** Failure ***")
        iter_diff = difflib.unified_diff(json3.splitlines(True), json4.splitlines(True), lineterm="\n")
        for chunk in iter_diff:
            print(chunk, end="")


def test2():

    pcf = FacetedPowerContextFamily(["Author", "Book"])
    a1 = pcf.add_object("Lewis Carroll", ["Author"])
    a2 = pcf.add_object("Virginia Woolf", ["Author"])
    a3 = pcf.add_object("Douglas Adams", ["Author"])
    a4 = pcf.add_object("Neil Gaiman", ["Author"])
    a5 = pcf.add_object("J. K. Rowling", ["Author"])
    a6 = pcf.add_object("Stephen King", ["Author"])
    a7 = pcf.add_object("Dan Brown", ["Author"])
    b1 = pcf.add_object("Alice in Wonderland", ["Book"])
    b2 = pcf.add_object("To the Lighthouse", ["Book"])
    b3 = pcf.add_object("Hitchhiker's Guide", ["Book"])
    b4 = pcf.add_object("Harry Potter 7", ["Book"])
    b5 = pcf.add_object("The Casual Vacancy", ["Book"])
    b6 = pcf.add_object("Trigger Warning", ["Book"])
    b7 = pcf.add_object("The Shining", ["Book"])
    b8 = pcf.add_object("Doctor Sleep", ["Book"])
    b9 = pcf.add_object("The Da Vinci Code", ["Book"])
    b10 = pcf.add_object("Inferno", ["Book"])

    pcf.add_rcontext("nationality", ["Author"], atts=["GB", "USA"])
    pcf.add_tuple([a1], "nationality", ["GB"])
    pcf.add_tuple([a2], "nationality", ["GB"])
    pcf.add_tuple([a3], "nationality", ["GB"])
    pcf.add_tuple([a4], "nationality", ["GB"])
    pcf.add_tuple([a5], "nationality", ["GB"])
    pcf.add_tuple([a6], "nationality", ["USA"])
    pcf.add_tuple([a7], "nationality", ["USA"])

    pcf.add_rcontext("DOB", ["Author"], atts=["19C", "20C", "21C"])
    pcf.add_tuple([a1], "DOB", ["19C"])
    pcf.add_tuple([a2], "DOB", ["19C"])
    pcf.add_tuple([a3], "DOB", ["20C"])
    pcf.add_tuple([a4], "DOB", ["20C"])
    pcf.add_tuple([a5], "DOB", ["20C"])
    pcf.add_tuple([a6], "DOB", ["20C"])
    pcf.add_tuple([a7], "DOB", ["20C"])

    pcf.add_rcontext("pubdate", ["Book"], atts=["19C", "20C", "21C"])
    pcf.add_tuple([b1], "pubdate", ["19C"])
    pcf.add_tuple([b2], "pubdate", ["20C"])
    pcf.add_tuple([b3], "pubdate", ["20C"])
    pcf.add_tuple([b4], "pubdate", ["21C"])
    pcf.add_tuple([b5], "pubdate", ["21C"])
    pcf.add_tuple([b6], "pubdate", ["21C"])
    pcf.add_tuple([b7], "pubdate", ["20C"])
    pcf.add_tuple([b8], "pubdate", ["21C"])
    pcf.add_tuple([b9], "pubdate", ["21C"])
    pcf.add_tuple([b10], "pubdate", ["21C"])

    pcf.add_rcontext("wrote", ["Author", "Book"], atts=["wrote", "age\u226430", "age\u226440", "age\u226450"])
    pcf.add_tuple([a1, b1], "wrote", ["wrote", "age\u226440", "age\u226450"])
    pcf.add_tuple([a2, b2], "wrote", ["wrote", "age\u226450"])
    pcf.add_tuple([a3, b3], "wrote", ["wrote", "age\u226430", "age\u226440", "age\u226450"])
    pcf.add_tuple([a4, b6], "wrote", ["wrote"])
    pcf.add_tuple([a5, b4], "wrote", ["wrote", "age\u226450"])
    pcf.add_tuple([a5, b5], "wrote", ["wrote", "age\u226450"])
    pcf.add_tuple([a6, b7], "wrote", ["wrote", "age\u226430", "age\u226440", "age\u226450"])
    pcf.add_tuple([a6, b8], "wrote", ["wrote"])
    pcf.add_tuple([a7, b9], "wrote", ["wrote", "age\u226440", "age\u226450"])
    pcf.add_tuple([a7, b10], "wrote", ["wrote", "age\u226450"])

    json1 = serialization.dumps(pcf)
    pcf2 = serialization.loads(json1)
    json2 = serialization.dumps(pcf2)

    print("Test2.1: PowerContextfamily JSON decode/encode")
    if json1 == json2:
        print("*** Success ***")
    else:
        print("*** Failure ***")
        iter_diff = difflib.unified_diff(json1.splitlines(True), json2.splitlines(True), lineterm="\n")
        for chunk in iter_diff:
            print(chunk, end="")


if __name__ == "__main__":
    test2()
