from WSGIApp import WSGISessionApp, WSGIHandler, Request, Response, Application
from pesto.session.filesessionmanager import FileSessionManager
import pesto

#from webob import Request, Response

#
# Demo starts here
#

class MyApp(WSGISessionApp):
    pass
    
class MyHandler(WSGIHandler):

    def hello(self, req, relpath, **args):
        resp = Response("hello")
        resp.content_type = "text/plain"
        return resp

    def dump(self, req, relpath, **args):
    
        session = req.environ['pesto.session']
        resp = Response()
        resp.write("""
            <html>
            <body>
            <pre>
            my path = %s
            relpath = %s
            query = %s
            session = %s
            """ % (self.MyPath, relpath, req.query_string, session.items()))
        keys = req.environ.keys()
        keys.sort()
        for k in keys:
            resp.write('%s = %s\n' % (k, req.environ[k]))
        resp.write("""
            </pre>
            </body>
            </html>""")
        #session['a'] = 'bb'
        return resp
        
    def set(self, req, relpath, **args):
        sess = self.getSessionData()
        for k, v in args.items():
            sess[k] = v
        sess.save()
        self.redirect("./show")

    def show(self, req, relpath, **args):
        sess = self.getSessionData()
        txt = "Is new: %s\n" % (sess.is_new,)
        txt += "Id: %s\n" % (sess.session_id,)
        txt += "Data:\n"
        txt += "\n".join(["%s = %s" % (k,v) for k, v in sess.items()])
        resp = Response(txt)
        resp.content_type = "text/plain"
        return resp
        
        resp = Response()
        
        
application = Application(MyApp, MyHandler)
