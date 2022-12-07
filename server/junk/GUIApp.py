from webpie import WebPieHandler, Response
from  ConfigParser import ConfigParser
from ConDB import ConDB
import time, sys, hashlib, os, random
from datetime import datetime, timedelta, tzinfo
from timelib import text2datetime, epoch
from DataBrowser import DataBrowser
from DataPlotter import DataPlotter
from GUI_Version import GUI_Version
    
try:
    from API_Version import API_Version
except:
    API_Version = "2.3"
    
def dtfmt(x, fmt):
    return x.strftime(fmt) if x else ''
    
def as_number(x):
    if not x:   return ''
    e = epoch(x)
    if e == int(e): e = int(e)
    return e
    
def as_json(x):
    return '"Date(%s,%s,%s,%s,%s,%s)"' % (
        x.year, x.month-1, x.day, x.hour, x.minute, x.second)
        
def nones_to_nulls(x):
    if type(x) == type([]) or type(x) == type(()):
        return [z if z != None else "null" for z in x ]
    elif x == None: return "null"
    else:   return x

           
class GUIApp(WSGIApp):

    def __init__(self, request, rootclass):
        WSGIApp.__init__(self, request, rootclass)
        self.Config = ConfigFile(request, envVar = 'CON_DB_CFG')
        self.DB = DBConnection(self.Config)
        try:    self.Title = self.Config.get('GUI','page_title')
        except: self.Title = "Conditions Database"
        
    def JinjaGlobals(self):
        return {    "GLOBAL_Title":     self.Title,
                    "GLOBAL_GUI_Version":   GUI_Version, 
                    "GLOBAL_API_Version":   API_Version  
                    }
            
    def db(self):
        return self.DB.connection()
        
    def destroy(self):
        self.DB.disconnect()
        self.DB = None
        
    def init(self, root):
        self.initJinja2(tempdirs=[os.path.dirname(__file__)],
            filters = {
            "dtfmt":    dtfmt,
            "as_number":    as_number,
            "nones_to_nulls":    nones_to_nulls,
            "as_json":    as_json
            }
        )



class GUIHandler(WSGIHandler):
    def __init__(self, req, app):
        WSGIHandler.__init__(self, req, app)
        #self.DB = app.db()
        self.B = DataBrowser(req, app)
        #self.P = DataPlotter(req, app)
        
    def hello(self, req, relpath, **args):
        #print req
        return Response("hello: x=%s" % (req.GET.get('x'),))
    
    def index(self, req, relpath, **args):
        return self.render_to_response("table_index.html")
        
       
        


