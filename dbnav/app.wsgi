import re
import os
import cgi
from dbnav.control import Control
from dbnav.serialization import dumps, loads


class URL(object):

    def __init__(self, pattern, method, schema=[], slash=True):
        self.pattern = pattern
        self.method = method
        self.schema = schema
        self.slash = slash


class HttpResponse(object):

    def __init__(self, content, content_type="text/html", status="200 OK"):
        self.content = content.encode("utf-8")
        self.content_type = content_type
        self.status = status


class HttpResponseRedirect(object):

    def __init__(self, url):
        self.content = b"1"
        self.status = "302 Found"
        self.url = url


class JsonResponse(object):

    def __init__(self, content, content_type="application/json", status="200 OK"):
        self.content = dumps(content).encode("utf-8")
        self.content_type = content_type
        self.status = status


def index():

    basedir = os.path.dirname(os.path.realpath(__file__))
    h = open(os.path.join(basedir, "resources", "server", "html", "index.html"), "r")
    content = h.read()
    h.close()

    return HttpResponse(content)


def css(path):

    basedir = os.path.dirname(os.path.realpath(__file__))
    h = open(os.path.join(basedir, "resources", "server", "css", path), "r")
    content = h.read()
    h.close()

    return HttpResponse(content, content_type="text/css")


def scripts(path):

    basedir = os.path.dirname(os.path.realpath(__file__))
    h = open(os.path.join(basedir, "resources", "server", "scripts", path), "r")
    content = h.read()
    h.close()

    return HttpResponse(content, content_type="text/javascript")


########### HTTP #################

def ajax(cmd, args, state):
    ctrl = Control(state)
    getattr(ctrl, cmd)(**args)
    views = ctrl.render()
    return JsonResponse({"state": ctrl.state, "views": views})


# Set content-type header even if no content is returned! The reason is Firefox bug 521301.
def application(environ, start_response):

    path = environ.get('PATH_INFO', '')
    request_method = environ['REQUEST_METHOD']

    urls = [
        URL(r'^$', index),
        URL(r'^css/(?P<path>.+)$', css, slash=False),
        URL(r'^scripts/(?P<path>.+)$', scripts, slash=False),
        URL(r'^ajax$', ajax),
    ]

    trailing_slash = path.endswith("/")
    path = path.lstrip("/").rstrip("/")

    url = None
    for _url in urls:
        match = re.search(_url.pattern, path)
        if match is not None:
            url = _url
            break

    # error 404
    if url is None:
        start_response("404 NOT FOUND", [("Content-type", "text/plain")])
        return [b"Not Found"]

    elif url.slash and not trailing_slash:
        start_response("301 Moved Permanently", [("Location", "/"+path+"/")])
        return [b"1"]

    elif request_method == 'GET':
        # cgi.parse_qs deprecated, urlparse.parse_qs can be used instead
        get = cgi.parse_qs(environ["QUERY_STRING"])
        args = {param: cgi.escape(get[param][0]) for param in url.schema}
        args.update(match.groupdict())
        response = url.method(**args)

    elif request_method == 'POST':
        n = int(environ.get("CONTENT_LENGTH", 0))
        data = environ['wsgi.input'].read(n)
        args = loads(data)
        args.update(match.groupdict())
        response = url.method(**args)

    if isinstance(response, JsonResponse):
        headers = [("Content-type", response.content_type), ("Content-Length", str(len(response.content)))]
        start_response(response.status, headers)
        return [response.content]

    elif isinstance(response, HttpResponseRedirect):
        headers = [("Location", response.url)]
        start_response(response.status, headers)
        return [response.content]

    else:  # HttpResponse
        headers = [("Content-type", response.content_type)]
        start_response(response.status, headers)
        return [response.content]


if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    srv = make_server('0.0.0.0', 8081, application)
    srv.serve_forever()
