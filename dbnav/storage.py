import os
from dbnav.serialization import dump, load


class Storage(object):

    available_scales = {
        "int": [],
        "varchar": ["PrefixScale"],
        "date": ["DateIntervalScale"],
        "bool": ["BooleanScale"],
    }

    @staticmethod
    def ls():
        basedir = os.path.dirname(os.path.realpath(__file__))
        bindings = []
        for fname in os.listdir(os.path.join(basedir, "resources", "bindings")):
            name, extension = os.path.splitext(fname)
            if extension == ".json":
                bindings.append(name)
        return {"bindings": bindings}

    @staticmethod
    def read(pcf):
        basedir = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(basedir, "resources", "bindings", "{0}.json".format(pcf)), 'r') as infile:
            pcf = load(infile)
        return pcf

    @staticmethod
    def write(pcf, fname):
        basedir = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(basedir, "resources", "bindings", "{0}.json".format(fname)), 'w') as fp:
            dump(pcf,fp)
