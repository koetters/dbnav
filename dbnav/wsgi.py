import re
import os
import tempfile
import urllib
import pickle
from http.cookies import SimpleCookie
from dbnav.control import Control
from dbnav.serialization import dumps, loads


class URL(object):

    def __init__(self, pattern, handler, slash=True):
        self.pattern = pattern
        self.handler = handler
        self.slash = slash


class HttpRequest(object):

    def __init__(self, environ):
        self.method = environ['REQUEST_METHOD'].upper()
        self.GET = dict(urllib.parse.parse_qsl(environ.get('QUERY_STRING','')))
        n = int(environ.get('CONTENT_LENGTH') or 0)
        self.body = environ['wsgi.input'].read(n)
        self.COOKIES = SimpleCookie(environ.get('HTTP_COOKIE',''))

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
        self.cookies = SimpleCookie()


def index(request):

    basedir = os.path.dirname(os.path.realpath(__file__))
    h = open(os.path.join(basedir, "resources", "server", "html", "index.html"), "r")
    content = h.read()
    h.close()

    return HttpResponse(content)


def css(request, filename):

    basedir = os.path.dirname(os.path.realpath(__file__))
    h = open(os.path.join(basedir, "resources", "server", "css", filename), "r")
    content = h.read()
    h.close()

    return HttpResponse(content, content_type="text/css")


def scripts(request, filename):

    basedir = os.path.dirname(os.path.realpath(__file__))
    h = open(os.path.join(basedir, "resources", "server", "scripts", filename), "r")
    content = h.read()
    h.close()

    return HttpResponse(content, content_type="text/javascript")


def ajax(request):
    data = loads(request.body)

    try:
        session = request.COOKIES["session"].value
        with open(session,'rb') as file:
            ctrl = pickle.load(file)

    except (KeyError, FileNotFoundError):
        fd, session = tempfile.mkstemp(prefix="granada_session_data_")
        os.close(fd)
        ctrl = Control({})

    getattr(ctrl, data["cmd"])(**data["args"])
    views = ctrl.render()
    response = JsonResponse({"views": views})

    with open(session,'wb') as file:
        pickle.dump(ctrl, file)
        response.cookies["session"] = session

    return response


# Set content-type header even if no content is returned! The reason is Firefox bug 521301.
def application(environ, start_response):

    path = environ.get('PATH_INFO', '')

    urls = [
        URL(r'^$', index),
        URL(r'^css/(?P<filename>.+)$', css, slash=False),
        URL(r'^scripts/(?P<filename>.+)$', scripts, slash=False),
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

    if url.slash and not trailing_slash:
        start_response("301 Moved Permanently", [("Location", "/"+path+"/")])
        return [b"1"]

    kwargs = match.groupdict()
    request = HttpRequest(environ)
    response = url.handler(request, **kwargs)

    if isinstance(response, JsonResponse):
        headers = [("Content-type", response.content_type),
                   ("Content-Length", str(len(response.content)))]
        for morsel in response.cookies.values():
            headers.append(("Set-Cookie", morsel.output(header='')))
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
