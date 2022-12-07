from webpie import WPApp, WPHandler, Response
from wsdbtools import ConnectionPool
from configparser import ConfigParser
from ConDB import ConDB
import time, sys, hashlib, os, random, traceback
from datetime import datetime, timedelta, tzinfo
from timelib import text2datetime, epoch
from threading import RLock, Lock, Condition
import threading, re
from trace import Tracer

from py3 import to_bytes, to_str
from signature import signature

from GUI_Version import GUI_Version
from API_Version import API_Version
from DataBrowser import DataBrowser

class   ConfigFile(ConfigParser):
    def __init__(self, path=None, envVar=None):
        ConfigParser.__init__(self)
        path = path or os.environ.get(envVar)
        if path:
            self.read(path)

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
        
def format_tv(x, numeric):
    if numeric: return as_number(x)
    else:   return dtfmt(x, "%Y-%m-%d&nbsp;%H:%M:%S&nbsp;%Z")
        
def nones_to_nulls(x):
    if type(x) == type([]) or type(x) == type(()):
        return [z if z != None else "null" for z in x ]
    elif x == None: return "null"
    else:   return x

           
class ConDBServerApp(WPApp):

    def __init__(self, rootclass, config_file=None):
        WPApp.__init__(self, rootclass)
        self.Config = cfg = ConfigFile(path=config_file, envVar = 'CON_DB_CFG')
        self.ServerPassword = self.Config.get('Server','password')
        try:    self.Title = self.Config.get('GUI','title')
        except: self.Title = "Conditions Database"
        try:
            self.CacheTTL = int(self.Config.get('Server', 'CacheTTL'))
        except:
            self.CacheTTL = 3600   # 1 hour
        try:
            self.TaggedCacheTTL = int(self.Config.get('Server', 'TaggedCacheTTL'))
        except:
            self.TaggedCacheTTL = 3600*24*7   # 7 days
       
        #
        # Init DB connection pool 
        #
        self.Host = None
        self.DBName = cfg.get('Database','name')
        self.User = cfg.get('Database','user')
        self.Password = cfg.get('Database','password')

        self.Port = None
        try:    
            self.Port = int(cfg.get('Database','port'))
        except:
            pass
            
        self.Host = None
        try:    
            self.Host = cfg.get('Database','host')
        except:
            pass
            
        self.Namespace = 'public'
        try:    
            self.Namespace = cfg.get('Database','namespace')
        except:
            pass
            
        connstr = "dbname=%s user=%s password=%s" % \
            (self.DBName, self.User, self.Password)
        if self.Port:   connstr += " port=%s" % (self.Port,)
        if self.Host:   connstr += " host=%s" % (self.Host,)
        self.ConnPool = ConnectionPool(postgres=connstr, idle_timeout=5)
        
        self.initJinjaEnvironment(
            tempdirs=[os.environ["JINJA_TEMPLATES_LOCATION"]],
            filters = {
                "format_tv":    format_tv,
                "dtfmt":    dtfmt,
                "as_number":    as_number,
                "nones_to_nulls":    nones_to_nulls,
                "as_json":    as_json
            },
            globals = 
            {    "GLOBAL_Title":     self.Title,
                 "GLOBAL_GUI_Version":   GUI_Version, 
                 "GLOBAL_API_Version":   API_Version  
            }
        )
            
    def db(self):
        conn = self.ConnPool.connect()
        #print("App.db(): connection:", id(conn), conn)
        return ConDB(connection = conn)
        

class ConDBHandler(WPHandler):
    def __init__(self, req, app):
        WPHandler.__init__(self, req, app)
        self.B = DataBrowser(req, app)
        
    def probe(self, req, relpath, **args):
        try:    
            c = self.App.db().cursor()
            c.execute("select 1")
            tup = c.fetchone()
            if tup[0] == 1:
                return Response("OK")
            else:
                raise ValueError("Data mismatch. Expected (1,), got %s" % (tup,))
        except:
            return "Probe error: %s" % (traceback.format_exc(),), 500

    def version(self, req, relpath, **args):
        return '{ "GUI":"%s", "API":"%s", "Version":"%s_%s" }\n' % \
                    (GUI_Version, API_Version, GUI_Version, API_Version) \
                ,"text/json"
    
    def dataTupleToCSV(self, tup):
        text_values = []
        for v in tup:
            if isinstance(v, str):
                if ',' in v or '\n' in v:
                    v = '"' + v.replace('"', '""') + '"'
            elif isinstance(v, list):
                    # assume list of numerics
                    v = '"' + str(v) + '"'
            else:
                    v = str(v)
            text_values.append(v)
        return ','.join(text_values)
        
    def csv_iterator_from_iter(self, table, data):
        columns = table.Columns
        first_line = True
        extra_tv = False
        for tup in data:
            if first_line:
                extra_tv = len(tup) == 4
                if extra_tv:
                    yield 'channel,tv,tv_end,'+','.join(columns)+'\n'
                else:
                    yield 'channel,tv,'+','.join(columns)+'\n'
                first_line = False

            c, tv, values = tup[0], tup[1], tup[2]
            vtxt = self.dataTupleToCSV(values)
            if extra_tv:
                tv_end = tup[3]
                tv_end = '%.3f' % (epoch(tv_end),) if tv_end != None else ''
                yield '%d,%.3f,%s,%s\n' % (c, epoch(tv), tv_end, vtxt)
            else:
                yield '%d,%.3f,%s\n' % (c, epoch(tv), vtxt)
                
    
    def mergeLines(self, iter, maxlen=10000):
        buf = []
        total = 0
        for l in iter:
            n = len(l)
            if n + total > maxlen:
                yield ''.join(buf)
                buf = []
                total = 0
            buf.append(l)
            total += n
        if buf:
            yield ''.join(buf)

    def parseConditions(self, args):
        lst = args.get("where", "")
        if not lst: return []
        if type(lst) != type([]):
            lst = [lst]
        parse_re = re.compile("(?P<name>\w*?)(?P<sign>(\<=|\<|=|\>=|\>|\!=))(?P<value>-?\d+(\.\d+)?)")
        conditions = []
        for item in lst:
            words = item.split(',')
            for w in words:
                #print "word: <%s>" % (w,)
                m = parse_re.match(w)
                if m:
                    name = m.group("name")
                    value = m.group("value")
                    sign = m.group("sign")
                    try:    value = int(value)
                    except:
                        try:    value = float(value)
                        except: pass
                    conditions.append((name, sign, value))
        #print "parseConditions:", conditions
        return conditions

    def sortTuples(self, data, sort_spec):
        # sorts data in place !
        # sort_spec: col1,col2
        #        or: col1
        # column can be either "tv" or "channel"
        #
        # data is assumed to be [(channel, tv, ...), ...]
        #
        
        if sort_spec == 'tv':
            data.sort(lambda x, y:  cmp(x[1], y[1]))
        elif sort_spec == 'channel':
            data.sort(lambda x, y:  cmp(x[0], y[0]))
        elif sort_spec == 'channel,tv':
            data.sort(lambda x, y:  cmp(x[0], y[0]) or cmp(x[1], y[1]))
        elif sort_spec == 'tv,channel':
            data.sort(lambda x, y:  cmp(x[1], y[1]) or cmp(x[0], y[0]))
        return data        

    def filterAndSort(self, table, data, conditions, sort, channel_ranges):
        # data is [(channel, tv, (data,...)),...]
        # conditions is [(column name, op, value),...]

        if not (conditions or sort or channel_ranges):
            return data
        
        columns = table.columns()
        coldict = { "channel":1,    "tv":2  }
        for i, c in enumerate(columns):
            coldict[c] = i+3        # reserve index 1 for channel and 2 for tv

        if conditions:
            if type(data) != type([]):  data = list(data)
            # translate conditions
                
            conds = []
            for c, op, value in conditions:
                conds.append((coldict[c], op, value))            
            
            data.sort(lambda x,y: cmp(x[0], y[0]) or cmp(x[1], y[1]))    # sort by channel then tv
            filtered = []
            for i, (channel, tv, values) in enumerate(data):
                next_tv = None
                if i < len(data)-1 and data[i+1][0] == channel:   next_tv = data[i+1][1]
                for ci, op, fv in conds:
                    if ci == 1:     v = channel
                    elif ci == 2:   v = tv
                    else:
                        v = values[ci-3]
                    ok = True
                    if op == '=':   ok = v == fv
                    elif op == '<': ok = v < fv
                    elif op == '<=': ok = v <= fv
                    elif op == '>': ok = v > fv
                    elif op == '>=': ok = v >= fv
                    elif op == '!=': ok = v != fv
                    
                    if ok:  filtered.append((channel, tv, values))      #, next_tv))
            data = filtered

        if channel_ranges and channel_ranges != [(None, None)]:
            out = []
            for channel, tv, values in data:
                for c0, c1 in channel_ranges:
                    if (c0 is None or channel >= c0) and (c1 is None or channel <= c1):
                        out.append((channel, tv, values))
                        break
            data = out
        
        if sort:
            if not isinstance(data, list):  data = list(data)
            sort_columns = sort.split(",")
            
            class sort_function:
                def __init__(self, indexes):
                    self.Indexes = indexes
                    
                def __call__(self, x, y):
                    ret = 0
                    for i in self.Indexes:
                        if i == 1:  ret = cmp(x[0], y[0])
                        elif i == 2:  ret = cmp(x[1], y[1])
                        else:       ret = cmp(x[2][i-3], y[2][i-3])
                        if ret: break
                    return ret
                    
            indexes = [coldict[c] for c in sort_columns]
            
            #print "indexes=", indexes
            
            data.sort(sort_function(indexes))
            
        return data
            
    def getData(self, table, t=None, t0=None, t1=None, 
                    cr=None, 
                    channels=None,
                    rtime = None,
                    iter="no",
                    sort=None,
                    tag = None, **args):
        
        
        #print "getData(%s,%s,%s,%s,%s)" % (table, t, t0, t1, args)
        
        if rtime and tag:
            raise ValueError("Can not specify both rtime and tag")

        conditions = self.parseConditions(args)

        data_type = args.get('type') or None
        if data_type:   data_type = str(data_type)

        if channels is None:    channels = cr
        
        channel_ranges = []
        cmin, cmax = None, None
        
        if channels:
            for segment in channels.split(","):
                c01 = segment.split("-", 1)
                if len(c01) < 2:
                    c01 = [c01[0], c01[0]]
                c0, c1 = c01
                c0 = c0 or None                 # convert blanks to None
                c1 = c1 or None
                try:    c0 = int(c0)
                except: pass                    # either None or string
                try:    c1 = int(c1)
                except: pass                    # either None or string
                if (c0, c1) != (None, None):
                    channel_ranges.append((c0, c1))
                    if cmin is None:    cmin = c0
                    if cmax is None:    cmax = c1
                    if c0 is not None:  cmin = min(cmin, c0)
                    if c1 is not None:  cmax = max(cmax, c1)

        channel_ranges = channel_ranges or None
        global_range = (cmin, cmax) if (cmin or cmax) else None

        if t != None:
            lines = self.getAtTime(table, t, data_type=data_type, tag = tag,
                            rtime=rtime, channel_range = global_range,
                            conditions = conditions)
        else:
            #print "calling getInterval..."
            #print sort
            if iter == "yes" and not sort:
                lines = self.getIntervalIter(table, t0, t1, data_type=data_type, 
                                tag = tag,
                                rtime=rtime, channel_range = global_range,
                                conditions = conditions)
            else:
                lines = self.getInterval(table, t0, t1, data_type=data_type, 
                                tag = tag,
                                rtime=rtime, channel_range = global_range,
                                conditions = conditions, sort = sort)

        #lines = list(lines)
        #print "getData: len(lines)=%d" % (len(lines),)

        #print "channel ranges:", channel_ranges

        lines = self.filterAndSort(table, lines, conditions, sort, channel_ranges)

        return lines

    #def get(self, req, relpath, table=None, cache="yes", t=None, t0=None, t1=None, 
    #                cr=None, 
    #                rtime = None,
    #                columns=None, iter="no",
    #                sort=None,
    #                tag = None, **args):
    
    def get(self, req, relpath, table=None, columns=None, **args):
        #print "get(%s,%s,%s)" % (table, t0, t1)


        columns = columns.split(',')
        table_name = table
        table = self.App.db().table(table, columns)
        if not table.exists():
            return Response("Table %s does not exist" % (table_name,), status=404)
            
        
        lines = self.getData(table, **args)
                    
        if lines == None:
            lines = []

        #lines = list(lines)
        #print "get: len(lines)=%d" % (len(lines),)

        lines = self.csv_iterator_from_iter(table, lines)

        #resp = Response(content_type='text/plain', 
        #    app_iter = self.mergeLines(lines))
        out = ''.join(lines)
        #print "get: output length=", len(out)
        resp = Response(out, content_type='text/plain')
        cache_ttl = self.App.CacheTTL
        if "tag" in args:
            cache_ttl = self.App.TaggedCacheTTL
        cache_ttl = int(random.uniform(cache_ttl * 0.9, cache_ttl * 1.2)+0.5)
        resp.cache_expires(cache_ttl)
        return resp

    def getAtTime(self, table, t, tag=None, rtime=None, data_type=None, 
                    conditions = [],
                    channel_range = None, **args):
        # returns iterator [(channel, tv, (data,...)),...]
        t = text2datetime(t)
        if not tag and rtime:    
            rtime = text2datetime(rtime)
        else:
            rtime =  None
        
        data = table.getDataIter(t, tag=tag, tr=rtime, data_type=data_type,
                    channel_range = channel_range, conditions = conditions)
        return ((tup[0], tup[1], tup[2:]) for tup in data)

    def getIntervalIter(self, table, t0, t1, tag=None, rtime=None, data_type=None, 
                        channel_range = None, conditions = [], **args):
        t0 = text2datetime(t0)
        t1 = text2datetime(t1)
        if not tag and rtime:    rtime = text2datetime(rtime)
        data = table.getDataIntervalIter(t0, t1, tag=tag, tr=rtime, data_type=data_type,
                        channel_range = channel_range, conditions = conditions)
        return data
        
    def getInterval(self, table, t0, t1, sort=None, **args):
        out = list(self.getIntervalIter(table, t0, t1, **args))
        if sort:
            out = self.sortTuples(out, sort)
        return out

    def parseTuple(self, line):
        out = []
        while line:
            line = line.strip()
            if line[0] == '"':
                # find closing quote
                i = 1
                found = None
                while i < len(line) and found is None:
                    i = line.find('"', i)
                    if i >= 0:
                        if i+1 < len(line) and line[i+1] == '"':
                            i += 2
                        else:
                            found = i
                if found is None:
                    # no closing quote ??
                    raise ValueError("Error parsing CSV line [%s]: can not find closing quote" % (line,))
                word = line[:found+1]           # include opening and closing quotes
                rest = line[found+1:].strip()   # this will be either empty, or something starting with comma
            else:
                i = line.find(',')
                if i >= 0:
                    word = line[:i]
                    rest = line[i:].strip()
                else:
                    word = line
                    rest = ""
            word = word.strip()
            # at this point, word is either quoted, and then repeating double-quotes need to be un-doubled
            # or it is not quoted
            
            value = None
            
            if word[0] == '"':
                assert word[-1] == '"'
                word = word[1:-1].replace('""', '"')
            
            if word:
                if word[0] == '[':
                    word = word.strip()
                    assert word[-1] == ']'
                    # assume this is list of numbers. List of strings is not supported !
                    words = word[1:-1].split(",")
                    lst = []
                    for w in words:
                        w = w.strip()
                        try:    v = int(w)
                        except: 
                            try:    v = float(w)
                            except: 
                                v = w       # text
                        lst.append(v)
                    value = lst
                else:
                    try:    value = int(word)
                    except: 
                        try:    value = float(word)
                        except: value = word    # text
            out.append(value)
            assert not rest or rest[0] == ','
            line = rest[1:]     # skip comma
                
        return tuple(out)   


    def authenticateSignature(self, req, data):
        salt = req.headers['X-Salt']
        sig = req.headers['X-Signature']
        table = req.GET['table']
        digest = signature(self.App.ServerPassword, salt, req.query_string, data)
        return digest == sig
        
    def put(self, req, relpath, table=None, **args):
        if req.method != 'POST':
            resp = Response()
            resp.status = 400
            return resp

        if "X-Signature" in req.headers:
            check = self.authenticateSignature(req, req.body)
            if not check:
                resp = Response("Signature forged")
                resp.status = 400
                return resp
        else:
            resp = Response("Authentication required")
            resp.status = 400
            return resp
        
        input = to_str(req.body).split("\n")
        tolerances = None
        header = input[0].strip()
        columns = [x.strip() for x in header.split(',')]
        if len(columns) < 3 or \
                columns[0].lower() != 'channel' or \
                columns[1].lower() != 'tv':
            resp = Response("Invalid header line in the CSV input.")
            resp.status = 400
            return resp
                
        columns = columns[2:]
        #print 'columns: ', columns
        table = self.App.db().table(table, columns)
        data = []
        for i in range(1, len(input)):
            line = input[i].strip()
            if not line:    continue
            #print("line:", line)
            if line.startswith('tolerance,,'):
                tolerances = self.parseTuple(line[len('tolerance,,'):])
                continue

            tup = self.parseTuple(line)
            channelid = tup[0]
            tv = text2datetime(tup[1])
            #print tv
            values = tup[2:]
            #print "data: ", channelid, tv, values
            data.append((channelid, tv, values))
            
        if not data:
            return Response("OK", status=204)

        #print "len(data)=", len(data)

        types = []
        for t in req.GET.getall('type'):
            for x in t.split(','):
                x = x.strip()
                if not x in types:
                    types.append(x)

        #print "types=", types

        if not types:
            #print("data:")
            #for line in data:
            #    print(line)
            table.addData(data, tolerances)
        else:
            for t in types:
                table.addData(data, tolerances, data_type=t)
        return Response("OK")
        
    def patch(self, req, relpath, table=None, tend=None, **args):
        if req.method != 'POST':
            resp = Response()
            resp.status = 400
            return resp


        tend = text2datetime(tend)
        input = req.body_file.readlines()
        if "X-Signature" in req.headers:
            check = self.authenticateSignature(req, input)
            if not check:
                resp = Response("Signature forged")
                resp.status = 400
                return resp
        else:
            resp = Response("Authentication required")
            resp.status = 400
            return resp
            
        header = input[0].strip()
        columns = [x.strip() for x in header.split(',')]
        if len(columns) < 3 or \
                columns[0].lower() != 'channel' or \
                columns[1].lower() != 'tv':
            resp = Response("Invalid header line in the CSV input.")
            resp.status = 400
            return resp
                
        columns = columns[2:]
        #print 'columns: ', columns
        table = self.App.db().table(table, columns)
        data = []
        for i in range(1, len(input)):
            line = input[i].strip()
            tup = self.parseTuple(line.split(','))
            channelid = tup[0]
            tv = text2datetime(tup[1])
            #print tv
            values = tup[2:]
            #print "data: ", channelid, tv, values
            data.append((channelid, tv, values))

        #print "len(data)=", len(data)

        types = []
        for t in req.GET.getall('type'):
            for x in t.split(','):
                x = x.strip()
                if not x in types:
                    types.append(x)

        #print "types=", types

        if not types:
            table.patch(data, tend)
        else:
            for t in types:
                table.patch(data, tend, data_type=t)
                
        return Response("OK")
        
    
    def tag(self, req, relpath, table=None, tag=None, 
                copy_from=None, override='no', **args):     
        table = self.App.db().table(table, [])
        if copy_from:
            table.copyTag(copy_from, tag, override = override == 'yes')
        else:
            table.tag(tag, override = override == 'yes')
        return Response("OK")
        
    def snapshot(self, req, relpath, table=None, t=None, prefill=True, **args):
        prefill = prefill != "no"
        t = text2datetime(t)
        table = self.App.db().table(table, [])
        s = table.createSnapshot(t, prefill=prefill)
        return Response("OK")
        
    def index(self, req, relpath, **args):
        return self.render_to_response("index.html")
        

def create_application(config_file=None):
    return ConDBServerApp(ConDBHandler, config_file)
    
if "CON_DB_CFG" in os.environ:
    application = ConDBServerApp(ConDBHandler, os.environ["CON_DB_CFG"])

if __name__ == "__main__":
    import sys, getopt
    opts, args = getopt.getopt(sys.argv[1:], "c:")
    opts = dict(opts)
    config = opts.get("-c")
    if config:
        print("Using config file:", config)
    print("Starting HTTP server at port 8888...") 
    application = ConDBServerApp(ConDBHandler, config)
    application.run_server(8888)
        
       
        


