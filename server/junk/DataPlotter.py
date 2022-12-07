from webpie import WebPieHandler, Response
from dbdig import DbDig
from timelib import epoch, text2datetime

class DataPlotter(WebPieHandler):

    def __init__(self, req, app, path):
        WebPieHandler.__init__(self, req, app, path)
        self.DB = app.db()
        self.DBConn = self.DB.connect()

    def index(self, req, relpath, namespace="public", **args):
        # get all the folders
        dig = DbDig(self.DBConn)
        namespaces = dig.nspaces()
        namespaces.sort()
        db_tables = dig.tables(namespace) or []
        db_tables.sort()
        condb_tables = []
        for t in db_tables:
            if t.endswith("_snapshot"):
                t = "%s.%s" % (namespace, t[:-len("_snapshot")])
                #print "table:",t
                t = self.DB.tableFromDB(t)
                #print "table from db:",t
                if t:   condb_tables.append(t)
        for t in condb_tables:
            #print "table:",t.Name
            t.snapshotList = t.snapshots()
            t.snapshotCount = len(t.snapshotList)
        return self.render_to_response("table_index.html",
            namespace = namespace,
            namespaces = namespaces,
            tables = condb_tables)
            
    def plot_table(self, req, relpath, table = None, tag = "", data_type = None, 
                        column = None, namespace = "public", time_as_int = "yes",
                        channels = None, t0 = None, t1 = None, do_plot = "no",
                        **args):

        dig = DbDig(self.DBConn)
        namespaces = dig.nspaces()
        tables = self.DB.tables(namespace)
        tables = [t.Name.split('.')[-1] for t in tables]

        do_plot = do_plot == "yes"
        table_selected = table
        type_selected = None
        tags = []
        data_types = []
        columns = []
        chan_range = []
        tag_selected = None
        if table_selected:
            tag_selected = tag or None
            type_selected = data_type or None
            t = self.DB.tableFromDB(namespace+"."+table)
            columns = t.columns()
            tags = t.tags()
            data_types = t.dataTypes()
            if channels:
                chan_range = channels.split(":", 1)
                if len(chan_range) == 2:
                    chan_range = (int(chan_range[0]), int(chan_range[1]))
                else:
                    chan_range = (int(chan_range[0]), int(chan_range[0]))
                chan_range = range(chan_range[0], chan_range[1]+1)
        
        data_url = ""
        
        if do_plot and table_selected:
            data_url = "./data?table=%s.%s&column=%s&t0=%s&t1=%s&channels=%s" % (namespace, table, column, t0, t1, channels)
            #print data_url
            if tag_selected:    data_url += "&tag=%s" % (tag_selected,)
            if data_type:       data_url += "&data_type=%s" % (data_type,)
        
        return self.render_to_response("plot_table.html",
                    data_url = data_url,
                    do_plot = do_plot,
                    time_as_int = time_as_int == "yes",
                    namespaces = namespaces,
                    namespace = namespace,
                    table = table_selected,
                    tables = tables,
                    columns = columns,
                    channels = channels,    chan_range = chan_range,
                    tag_selected = tag_selected,
                    column = column,
                    tags = tags, data_types = data_types, data_type = data_type,
                    t0 = t0 or '',    t1 = t1 or '')
                    
    def data(self, req, relpath, table = None, column = None, t0 = None, t1 = None,
                tag = None, channels = None, data_type = None, **args):
        t = self.DB.table(table, [column])
        t0 = text2datetime(t0)
        t1 = text2datetime(t1)
        #print t0, t1
        if channels != None:
            channels = channels.split(":", 1)
            if len(channels) == 2:
                channels = (int(channels[0]), int(channels[1]))
            else:
                channels = (int(channels[0]), int(channels[0]))
        data = t.getDataInterval(t0, t1, tag=tag, data_type=data_type,
                    channel_range = channels)
        data.sort(lambda x, y: cmp(x[1], y[1]) or cmp(x[0], y[0])) # by tv, then by channel
        chan_list = range(channels[0], channels[1]+1)
        data_out = []
        last_t = None
        last_tup = [None] * len(chan_list)
        this_tup = last_tup[:]
        #print data
        for c, tv, vals in data:
            if last_t != tv:
                # new or first point
                if last_t != None:
                    # new point
                    data_out.append((last_t, epoch(last_t), last_tup[:]))
                    data_out.append((last_t, epoch(last_t), this_tup[:]))
                    last_tup = this_tup
                    this_tup = last_tup[:]
                last_t = tv
            i = chan_list.index(c)
            this_tup[i] = vals[0]
            #print epoch(tv), "   last tup:", last_tup, "   this tup:", this_tup
        data_out.append((last_t, epoch(last_t), this_tup))
        data_out.append((t1, epoch(t1), this_tup))
        resp = self.render_to_response("plot_data.json", channels = chan_list, data = data_out, table = table)
        resp.content_type = "text/json"
        return resp
