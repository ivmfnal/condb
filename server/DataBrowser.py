import time
from webpie import WPHandler, Response
from dbdig import DbDig
from timelib import epoch, text2datetime
from urllib.parse import unquote

class DataBrowser(WPHandler):

    def index(self, req, relpath, namespace="public", **args):
        # get all the folders
        db = self.App.db()
        tables = db.tables(namespace)
        for t in tables:
            #print "table:",t.Name
            try:
                ns = t.Namespace        # for compatibility with older API versions
            except AttributeError:
                t.Namespace, t.TableName = tuple(t.Name.split(".", 1))
        return self.render_to_response("table_index.html",
            namespace = namespace,
            namespaces = db.namespaces(),
            tables = tables)
            
    def show_table(self, req, relpath, table = None, show_shadowed = "", tag = "", data_type = None, 
                        numeric_time = "no",
                        namespace = "public", **args):
        numeric_time = numeric_time == "on"
        show_shadowed = show_shadowed == "on"
        tag_selected = tag or None
        type_selected = data_type or None
        namespace_table = table
        if not '.' in namespace_table:
            namespace_table = namespace + "." + table
        else:
            namespace, table = tuple(namespace_table.split('.', 1))
        db = self.App.db()
        t = db.tableFromDB(namespace_table)
        columns = t.columns()
        tags = t.tags()
        data_types = t.dataTypes()
        snapshots = t.snapshots(tag = tag_selected, data_type = type_selected)
        snapshots = sorted(snapshots, key=lambda s: s.Tr)
        for i, s in enumerate(snapshots):
            shadowed = False
            s.latestUpdateTime = s.latestUpdate()
            s.tagList = sorted(s.tags())
            s.shadowed = False
            s.shadowSnapshot = None

            s.NChannels, cmin, cmax, tvmin, tvmax = s.summary()

            s.ChannelRange = (cmin, cmax)
            s.TvRange = (tvmin, tvmax)

            for s1 in snapshots[i+1:]:
                if s1.DataType == s.DataType and s1.Tv <= s.Tv:
                    s.shadowed = True
                    s.shadowSnapshot = s1.Id
                    break
                if s1.DataType == s.DataType and s.latestUpdateTime and s1.Tv <= s.latestUpdateTime:
                    s.partiallyShadowed = True
                    s.shadowSnapshot = s1.Id
            #print(s.Id, s.shadowed, s.shadowSnapshot)
        return self.render_to_response("show_table.html", show_shadowed = show_shadowed,
            numeric_time = numeric_time,
            namespace = namespace,
            table = table, snapshots = snapshots[::-1], 
            tags = tags, tag_selected = tag_selected,
            data_types = data_types, type_selected = type_selected,
            columns = columns
            )
        
            
    def plot_table(self, req, relpath, namespace="public", table=None, **args):

        db = self.App.db()
        namespaces = db.namespaces()
        
        tag = req.POST.get("tag", "")
        data_type = req.POST.get("data_type")
        
        column = req.POST.get("column")
        time_as = req.POST.get("time_as", "number")
        channels = req.POST.get("channels")
        t0 = req.POST.get("t0") or 0
        t1 = req.POST.get("t1") or None
        do_plot = req.POST.get("do_plot", "no")

        tables = [t.Name.split('.')[-1] for t in db.tables(namespace)]
        
        time_as_int = time_as == "number"
        do_plot = do_plot == "yes"
        
        table_selected = table
        tags = []
        data_types = []
        columns = []
        chan_range = []
        tag_selected = None
        data_type = data_type or None
        tag_selected = tag or None
        db = self.App.db()
        
        type_selected = data_type
        
        if table_selected:
            t = db.tableFromDB(namespace+"."+table)
            columns = t.columns()
            tags = t.tags()
            data_types = [dt for dt in t.dataTypes() if dt]
            if channels:
                chan_range = channels.split(":", 1)
                if len(chan_range) == 2:
                    chan_range = (int(chan_range[0]), int(chan_range[1]))
                else:
                    chan_range = (int(chan_range[0]), int(chan_range[0]))
                chan_range = list(range(chan_range[0], chan_range[1]+1))
        
        data_url = ""
        
        if do_plot and table_selected:
            data_url = "./table_data?table=%s.%s&column=%s&t0=%s&t1=%s&channels=%s" % (namespace, table, column, t0, t1, channels)
            #print data_url
            if tag_selected:    data_url += "&tag=%s" % (tag_selected,)
            if type_selected:   data_url += "&data_type=%s" % (type_selected,)
        
        return self.render_to_response("plot_table.html",
                    data_url = data_url,
                    do_plot = do_plot,
                    time_as_int = time_as_int,
                    namespaces = namespaces,
                    namespace = namespace,
                    table = table_selected,
                    tables = tables,
                    columns = columns,
                    channels = channels,    chan_range = chan_range,
                    tag_selected = tag_selected,
                    column = column,
                    tags = tags, data_types = data_types, data_type = data_type,
                    t0 = t0 if t0 != None else '',    t1 = t1 if t1 != None else '',
                    dt0 = text2datetime(t0) if t0 is not None else None, 
                    dt1 = text2datetime(t1) if t1 is not None else None)
                    
    def table_data(self, req, relpath, table = None, column = None, t0 = None, t1 = None,
                tag = None, channels = None, data_type = None, **args):
        db = self.App.db()
        t = db.table(table, [column])
        t0 = t0 or 0
        t1 = t1 or time.time()
        t0 = text2datetime(t0)
        t1 = text2datetime(t1)
        if channels:
            channels = channels.split(":", 1)
            if len(channels) == 2:
                channels = (int(channels[0]), int(channels[1]))
            else:
                channels = (int(channels[0]), int(channels[0]))
        else:
            channels = None
        data_type = data_type or None
            
        #print("t0/t1:%s/%s, channels:%s, tag:%s, data_type:%s" % (t0, t1, channels, tag, data_type))
            
        data = t.getDataInterval(t0, t1, tag=tag, data_type=data_type,
                    channel_range = channels)
        #print("t0/t1:%s/%s data:%d" % (t0,t1,len(data)))
                    
        #data.sort(lambda x, y: cmp(x[1], y[1]) or cmp(x[0], y[0])) # by tv, then by channel
        data = sorted(data, key=lambda x: (x[1], x[0]))
        existing_channels = {}
        for c, tv, vals in data:
            existing_channels[c] = 1
        chan_list = sorted(existing_channels.keys())
        data_out = []
        if data:
            last_t = None
            last_tup = [None] * len(chan_list)
            if data[0][1] > t0:
                # query interval begins before first data point
                data_out.append((t0, epoch(t0), last_tup[:]))
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
        resp = self.render_to_response("table_data.json", channels = chan_list, data = data_out, table = table,
                t0 = t0, t1 = t1)
        resp.content_type = "text/json"
        return resp
        
            
                       
