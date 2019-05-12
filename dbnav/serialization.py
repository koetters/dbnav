import json
import datetime
import importlib


def _object_hook(obj):

    if "_cls" not in obj:
        return obj

    elif obj["_cls"] == "date":
        isodate = obj["_val"]
        return datetime.datetime.strptime(isodate, "%Y-%m-%d").date()

    # the datetime parsing below implements the functionality of datetime.fromisoformat in the Python 3.7 module
    # (which is not used itself because Python 3.7 is still recent). according to the documentation there,
    # a datetime string of the form YYYY-MM-DD[*HH[:MM[:SS[.fff[fff]]]]][+HH:MM[:SS[.ffffff]]] has to be parsed
    elif obj["_cls"] == "datetime":
        isodate = obj["_val"]
        d = {
            "year": int(isodate[0:4]),
            "month": int(isodate[5:7]),
            "day": int(isodate[8:10]),
        }
        isotime = isodate[11:]
        if isotime:
            if "+" in isotime:
                isotime, offset = isotime.split("+")
                tzsign = 1
            elif "-" in isotime:
                isotime, offset = isotime.split("-")
                tzsign = -1
            else:
                offset = None
                tzsign = None

            d["hour"] = int(isotime[0:2])
            isotime = isotime[3:]
            if isotime:
                d["minute"] = int(isotime[0:2])
                isotime = isotime[3:]
                if isotime:
                    d["second"] = int(isotime[0:2])
                    isotime = isotime[3:]
                    if isotime:
                        d["microsecond"] = 1000 * int(isotime[0:3])
                        isotime = isotime[3:]
                        if isotime:
                            d["microsecond"] += int(isotime[0:3])

            if offset:
                z = {
                    "hours": int(offset[0:2]),
                    "minutes": int(offset[3:5]),
                }
                offset = offset[6:]
                if offset:
                    z["seconds"] = int(offset[0:2])
                    offset = offset[3:]
                    if offset:
                        z["microseconds"] = int(offset[0:6])

                offset = datetime.timedelta(**z)
                d["tzinfo"] = datetime.timezone(tzsign * offset)

        return datetime.datetime(**d)

    elif obj["_cls"] == "set":
        return set(obj["_val"])

    else:
        module = importlib.import_module(obj["_mod"])
        cls = getattr(module, obj["_cls"])
        return cls.from_dict(obj)


def _json_default(obj):

    if isinstance(obj, datetime.date):
        return {
            "_cls": "date",
            "_val": obj.isoformat(),
        }

    elif isinstance(obj, datetime.datetime):
        return {
            "_cls": "datetime",
            "_val": obj.isoformat(),
        }

    elif isinstance(obj, set):
        return {
            "_cls": "set",
            "_val": list(obj),
        }

    elif hasattr(obj, "to_dict"):
        d = obj.to_dict()
        d.update({
            "_cls": type(obj).__name__,
            "_mod": type(obj).__module__,
        })
        return d

    else:
        raise TypeError("Unserializable object {0} of type {1}".format(obj,type(obj)))


def dump(obj, fp):

    json.dump(obj, fp, default=_json_default, sort_keys=True, indent=4)


def dumps(obj):

    return json.dumps(obj, default=_json_default, sort_keys=True, indent=4)


def load(fp):

    return json.load(fp, object_hook=_object_hook)


def loads(string):

    return json.loads(string,object_hook=_object_hook)
