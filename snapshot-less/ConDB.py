import psycopg2, sys, time, datetime
import io
from timelib import epoch

from trace import Tracer
from dbdig import DbDig

def cursor_iterator(c):
    tup = c.fetchone()
    while tup:
        yield tup
        tup = c.fetchone()

class dict_with_default:
    def __init__(self, default = None):
        self.Default = default
        self.Dict = {}

    def __str__(self):
        return "dict_with_default(%s, %s)" % (self.Default, self.Dict)
        
    def __getitem__(self, key):
        if key in self.Dict:      return self.Dict[key]
        else:                   return self.Default
        
    def get(self, key, default=None):
        if key in self.Dict:   return self.Dict[key]
        else:                   return default
        
    def __getattr__(self, x):
        return getattr(self.Dict, x)

def format_conditions(conditions):
    lst = []
    for c, op, v in conditions:
        lst.append("(%s %s '%s')" % (c, op, v))
    return " and ".join(lst)

class ConDB:
    def __init__(self, connstr=None, connection=None):
        self.Conn = connection
        self.ConnStr = connstr
        
    def connect(self):
        if self.Conn == None:
            self.Conn = psycopg2.connect(self.ConnStr)
        return self.Conn
    
    def cursor(self):
        conn = self.connect()
        return conn.cursor()
        
    def table(self, name, columns):
        return CDTable(self, name, columns)
        
    def tableFromDB(self, name):
        t = CDTable(self, name, [])
        try:    t.readDataColumnsFromDB()
        except ValueError:
            #print "tableFromDB(%s)" % (name,), sys.exc_type, sys.exc_value
            return None
        return t
        
    def namespaces(self):
        dig = DbDig(self.Conn)
        return dig.nspaces()

    def createTable(self, name, column_types, owner=None,
                    grants = {}, drop_existing=False):
        t = CDTable.create(self, name, column_types, owner, 
                    grants, drop_existing)
        return t

    def execute(self, table, sql, args=()):
        #print "DB.execute(%s, %s, %s)" % (table, sql, args)
        table_no_ns = table.split('.')[-1]
        sql = sql.replace('%t', table)
        sql = sql.replace('%T', table_no_ns)
        c = self.cursor()
        #print "executing: <%s>, %s" % (sql, args)
        t0 = time.time()
        #print("ConDB.execute: sql:", sql, "\n      args:", args)
        c.execute(sql, args)
        #print "executed. t=%s" % (time.time() - t0,)
        return c

    def copy_from(self, title, data, table_template, columns):
        table = table_template.replace('%t', title)
        c = self.cursor()
        #print "copy_from(data=%s, \ntable=%s,\ncolumns=%s" % (
        #        data, table, columns)
        c.copy_from(data, table, columns=columns)
        return c
        
    def disconnect(self):
        if self.Conn:   self.Conn.close()
        self.Conn = None

    def tables(self, namespace = "public"):
        dig = DbDig(self.Conn)
        db_tables = dig.tables(namespace) or []
        db_tables_set = set(db_tables)
        db_tables.sort()
        condb_tables = []

        for t in db_tables:
            if t.endswith("_snapshot"):
                tn = t[:-len("_snapshot")]
                if (tn+"_update") in db_tables_set and (tn+"_tag") in db_tables_set:
                        t = "%s.%s" % (namespace, tn)
                        t = self.tableFromDB(t)
                        if t:   condb_tables.append(t)
        return condb_tables

class CDSnapshot:

    def __init__(self, table, snapshot_id, tv = None, tr = None):
        self.Table = table
        self.Id = snapshot_id
        self.Tv = tv
        self.Tv_end = None
        self.Tr = tr
        self.Flags = 0
        self.Deleted = None
        self.Data = None
        self.Updates = None
        self.DataType = None
        self.InfoFetched = False     
        self.T = Tracer()
        
    def __str__(self):
        return '<CDSnapshot id=%s type=%s tr=%s tv=%s(%s) tvend=%s deleted=%s>' % \
            (self.Id, self.DataType, self.Tr, self.Tv, epoch(self.Tv), 
                self.Tv_end or "-",
                self.Deleted)

    __repr__ = __str__

    @staticmethod
    def create(table, tv, data_type, tv_end = None):
        c = table.execute("""insert into %t_snapshot(__tv, __type, __tv_end)
                values(%s, %s, %s);
                select lastval();""", (tv, data_type, tv_end))
        sid = c.fetchone()[0]
        c.execute("commit")
        s = CDSnapshot(table, sid)
        s.fetchInfo()
        return s
        
    def fetchInfo(self):
        if not self.InfoFetched:
            #print "Fetching"
            #print self.Table
            c = self.Table.execute("""select __tv, __tv_end, __tr, 
                            __deleted, __type
                    from %t_snapshot
                    where __id=%s""", (self.Id,))
            #print "Fetched"
            self.Tv, self.Tv_end, self.Tr, self.Deleted, self.DataType = c.fetchone()    
            self.InfoFetched = True
        return self

    def getData(self, channel_range = None, conditions = []):  
        # returns [(channel, tv, data, ...)]
        #print 'CDSnapshot.getData(): self.Data=%s' % (self.Data,)
        
        #print "snapshot.getData()..."
        
        self.T['getData'].begin()
        
        #if not self.Data:
        #    self.Data = self.Table.getCachedSnapshotData(self.Id)
        
        #print "self.Data=", self.Data
        
        self.T['getData/fetch'].begin()
        self.fetchInfo()
        data_columns = self.Table.columns()
        columns = ','.join(["__channel"] + data_columns)
        channels = ""
        if channel_range:
            channels = " and __channel between %s and %s " % channel_range
        cond_where = format_conditions(conditions)
        if cond_where:  cond_where = " and " + cond_where
        #print cond_where
        sql = "select " + columns + """ from %t_snapshot_data
                                where __snapshot_id = %s""" + channels + cond_where
        #print "snapshot.getData: sql = %s" % (sql,)
        try:    c = self.Table.execute(sql, (self.Id,))
        except:
            self.Table.execute("rollback")
            return
        #print "snapshot.getData: executed"
        self.T['getData'].end()
        #print "returning from getData with iterator"
        tup = c.fetchone()
        while tup:
            yield (tup[0], self.Tv) + tup[1:]
            tup = c.fetchone()

    def getDataAsDict(self, channel_range = None, conditions = []):
        # returns {channel:(tv, data)}
        d = {}
        for tup in self.getData(channel_range = channel_range, conditions = conditions):
            channel = tup[0]
            tv = tup[1]
            d[channel] = (tv, tup[2:])
        return d

    def getUpdateCount(self):
        sql = """select count(*) from %t_update
                    where __snapshot_id=%s"""
        c = self.Table.execute(sql, (self.Id,))
        tup = c.fetchone()
        if not tup: return 0
        return tup[0]
        
    def getAllUpdates(self):
        # returns {channel:[(tv, (data,...)),...]}
        
        self.T['getAllUpdates'].begin()
        if self.Updates == None:
            self.T['getAllUpdates/fetch'].begin()
            #print 'getAllUpdates: getting updates from the DB..'
            data_columns = self.Table.columns()
            columns = ','.join(["__channel, __tv, __tr"] + data_columns)
            sql = "select distinct on (__channel, __tv) " + columns + """ from %t_update
                                    where __snapshot_id = %s
                                    order by __channel, __tv, __tr desc"""
            c = self.Table.execute(sql, (self.Id,))
            out = {}
            #print "getAllUpdates: making dictionary..."

            last_channel, last_tv, last_tr = None, None, None
            for tup in cursor_iterator(c):
                channel, tv, tr = tup[:3]
                if None in (last_channel, last_tv, last_tr) or channel > last_channel or tv > last_tv or tr > last_tr:
                    last_channel, last_tv, last_tr = channel, tv, tr
                    old_tuples = out.setdefault(channel, []) # {tv -> tuples}
                    old_tuples.append((tv, tup[3:]))
            #print "getAllUpdates: done making dictionary"
            self.Updates = out
            self.T['getAllUpdates/fetch'].end()
        self.T['getAllUpdates'].end()
        return self.Updates
        
    def summary(self):
        sql = """select min(__tv), max(__tv) from %t_update where __snapshot_id = %s"""
        c = self.Table.execute(sql, (self.Id,))
        tup = c.fetchone()
        tvmin, tvmax = None, None
        if tup:
            tvmin, tvmax = tup
        
        sql = """select min(__channel), max(__channel) from %t_update where __snapshot_id = %s"""
        c = self.Table.execute(sql, (self.Id,))
        tup = c.fetchone()
        cmin, cmax = None, None
        if tup:
            cmin, cmax = tup

        sql = """select count(*) from (select distinct __channel from %t_update where __snapshot_id = %s) as tmp"""
        c = self.Table.execute(sql, (self.Id,))
        tup = c.fetchone()
        nc = 0 if tup is None else tup[0]
        
        return nc, cmin, cmax, tvmin, tvmax
            
        
        
    def _____channelStats(self, tr = None, channel_range = None):
        # returns list: [ (channel, tvmin, tvmax) ]
        
        tr_where = ""
        if tr:  tr_where = " and __tr < '%s'" % (tr.strftime("%Y-%m-%d %H:%M:%S"),)
        channels = ""
        if channel_range:
            channels = " and __channel between %s and %s " % channel_range
        sql = """select __channel, min(__tv), max(__tv) from %t_update
                                where __snapshot_id = %s """ + tr_where + channels + \
                                " group by __channel order by __channel"
        c = self.Table.execute(sql, (self.Id,))
        return [(ch, tvmin, tvmax) for ch, tvmin, tvmax in cursor_iterator(c)]
        
    def checkOverlap(self, tmin, data):
    
        tmax = self.latestUpdate()
        if tmax == None or tmax < tmin: return False, False

        overlap = True         # min(data tv) < max(updates tv)
    
        updates = self.getAllUpdates()  # returns {channel:[(tv, (data,...)),...]}

        shadow = False          # for at least one channel, data tv < updates tv

        min_data_tv = tmin
        max_update_tv = None

        for channel, tv_data, tup in data:
            lst = updates.get(channel, [])
            for tv_upd, x in lst:
                if tv_data <= tv_upd:
                    shadow = True
                    break
            if shadow:  break
                    
                    
        return overlap, shadow

    def shadowed(self, t):
        self.fetchInfo()
        t_last = self.latestUpdate()
        return t_last != None and t_last >= t
            
    def getUpdatesForChannel(self, channel):
        upd = self.getAllUpdates()
        return upd.get(channel, [])
        
    def getValues___(self, channel, t):
        # returns (data,...), tv
        self.T['getValues'].begin()
        tup = None
        tv = self.Tv
        for t in self.getData(channel_range = (channel, channel)):
            if t[0] == channel:
                tv = t[1]
                tup = t[2:]
                break
        upd = self.getUpdatesForChannel(channel)
        for utv, utup in upd:
            if utv > t: break
            tup = utup
            tv = utv
        self.T['getValues'].end()
        return tup, tv            

    def getUpdatesInRange(self, tmin, tmax, tr, channel_range = None, conditions = []):
        # returns updates where tv <= tmax
        # returns {channel:[(tv,data),...]}
        
        if self.Tv_end != None and self.Tv_end < tmax:
            tmax = self.Tv_end
        
        self.fetchInfo()
        data_columns = self.Table.columns()
        columns = ','.join(["__channel, __tv, __tr"] + data_columns)
        tr_where = ""
        if tr:  tr_where = "and __tr < '%s'" % (tr.strftime("%Y-%m-%d %H:%M:%S"),)
        channels = ""
        if channel_range:
            channels = " and __channel between %s and %s " % channel_range
        cond_where = ""
        if conditions:
            for name, op, value in conditions:
                cond_where += " and %s %s '%s'" % (name, op, value)
        sql = "select " + columns + """ from %t_update
                                where __snapshot_id = %s
                                    and __tv <= %s """ + tr_where + channels + cond_where + \
                                    " order by __channel, __tv, __tr desc"""
        c = self.Table.execute(sql, (self.Id, tmax))
        dict = {}
        last_channel, last_tv, last_tr = None, None, None
        for tup in cursor_iterator(c):
            channel, tv, tr = tup[:3]
            #print ("getUpdatesInRange: got (%s, %s, %s)" % (channel, tv, tr))
            if last_channel is None or channel > last_channel or tv > last_tv or tr > last_tr:
                last_channel, last_tv, last_tr = channel, tv, tr
                lst = dict.get(channel, None)
                #print tmin, tmin.tzinfo, tv, tv.tzinfo
                if tmin.tzinfo == None and tv.tzinfo != None:
                    tmin = tmin.replace(tzinfo = tv.tzinfo)
                #print tmin, tv
                if not lst or tv <= tmin:
                    lst = []
                    dict[channel] = lst
                #print "getUpdatesInRange: appending", (tv, tup[3:])
                lst.append((tv, tup[3:]))
        return dict
        

    def getUpdatesForTime(self, tv, tr = None, channel_range = None, conditions = []):
        # returns iterator [(channel,tv,data,...),...]
        #print "getUpdatesForTime: Updates: %s" % (self.Updates,)
        if not self.Updates:
            return self.getUpdatesForTimeFromDB(tv, tr = tr, 
                                channel_range = channel_range, conditions = conditions)
        else:
            return self.getUpdatesForTimeFromCache(tv, tr = tr, 
                                channel_range = channel_range, conditions = conditions)

    def getUpdatesForTimeFromCache(self, tv, tr = None, channel_range = None, conditions = []):
        # returns iterator [(channel,tv,data,...),...]
        for channel, lst in self.Updates.items():
            last_tv, last_data = None, None
            for t, data in lst:
                if t <= tv:
                    last_tv = t
                    last_data = data
            if not last_tv is None:
                if channel_range is None or (
                        channel_range[0] <= channel and channel <= channel_range[1]):
                    yield (channel, last_tv) + last_data

    def getUpdatesForTimeFromDB(self, tv, tr = None, channel_range = None, conditions = []):
        # returns iterator [(channel,tv,data,...),...]
        self.fetchInfo()
        data_columns = self.Table.columns()
        columns = ','.join(["__channel, __tv"] + data_columns)
        tr_where = ""
        if tr:  tr_where = "and __tr < '%s'" % (tr.strftime("%Y-%m-%d %H:%M:%S"),)
        channels = ""
        if channel_range:
            channels = " and __channel between %s and %s " % channel_range
        cond_where = ""
        if conditions:
            for name, op, value in conditions:
                cond_where += " and %s %s '%s'" % (name, op, value)
        sql = "select distinct on (__channel) " + columns + """ from %t_update
                                where __snapshot_id = %s
                                    and __tv <= %s """ + tr_where + channels + cond_where + \
                                    " order by __channel, __tr desc, __tv desc"""
        c = self.Table.execute(sql, (self.Id, tv))
        
        #print "getUpdatesForTimeFromDB: sql: %s\n      got %d rows" % (sql, len(c.fetchall()),)
        
        return cursor_iterator(c)
        
    def getValues(self, t):
        # returns {channel: (tv, data),...}
        data = self.getDataAsDict()
        updates = self.getUpdatesForTime(t)
        
        #debug
        updates = list(updates)
        #print "updates for time: %s: %s" % (t, updates)
        
        for tup in updates:
            channel = tup[0]
            tv = tup[1]
            data[channel] = (tv, tup[2:])
        return data
            
    def getValuesIter(self, t, tr = None, channel_range = None, conditions = []):
        # returns [(channel, tv, data, ...),...]
        #print "s.getValuesIter(t=%s)" % (t,)
        data = self.getDataAsDict(channel_range=channel_range, conditions=conditions)
        #print "s.getValuesIter: data: %s" % (data,)
        
        updates = self.getUpdatesForTime(t, tr=tr, channel_range=channel_range, conditions=conditions)

        updates = list(updates)
        #print "s.getValuesIter: updates: %s" % (updates,)     
        
        for tup in updates:
            channel = tup[0]
            if channel_range is None or (
                    channel_range[0] <= channel and channel <= channel_range[1]):
                tv = tup[1]
                if channel in data:   del data[channel]
                #print "s.getValuesIter: yielding ", tup
                yield tup
        for channel, (tv, values) in data.items():
            if channel_range is None or (
                    channel_range[0] <= channel and channel <= channel_range[1]):
                #print "s.getValuesIter: yielding ", (channel, tv) + values
                yield (channel, tv) + values

            
    def invalidateCache(self):
        self.Data = None
        self.Updates = None
            
    def purgeUpdates(self, updates, tolerances):
        #
        # IMPORTANT: updates must be sorted by channel and then by tv
        #
        # updates: [(channel, tv, (data, ...)),...]
        # tolerances: (tolerance,...)
        #
        # we can assume that self.get() returned True, i.e. all texisting updates
        # are in the past compared to incoming updates

        #if not tolerances:  return updates
        
        self.T['purgeUpdates'].begin()
        #print('purgeUpdates: tolerances:', tolerances)

        latest_update = self.latestUpdate()
        #print("purgeUpdates: latest_update:", latest_update)
        
        my_data_dict = self.getValues(latest_update)
        
        #print latest_update, my_data_dict
        
        new_updates = []
        #print 'purgeUpdates: %d updates' % (len(updates),)

        last_chan = None
        last_values = None
        last_tv = None
        
        #print("purgeUpdates: updates:")
        #for upd in updates:
        #    print("   ", upd)
        
        for upd in updates:
            chn, tv, data = upd
            if chn != last_chan:
                last_chan = chn
                last_tv, last_values = my_data_dict.get(chn, (None, None))

            close = False
            if last_values:
                close = True
                if tolerances:
                    for j, x in enumerate(last_values):
                        new = data[j]
                        if x != new:
                            close = False
                            if type(x) in (type(1), type(1.0)):
                                t = tolerances[j]
                                #print 'tolerance[%d] = %s (%s)' % (
                                #        j, t, type(t))
                                if t and t > 0:
                                    close = abs(x-new) <= t
                        if not close:   break
                else:
                    close = last_values == data
            if not close:
                last_values = data
                last_tv = tv
                new_updates.append(upd)
        self.T['purgeUpdates'].end()
        #print "pudates purged from %s to %s" % (len(updates), len(new_updates))
        return new_updates    
        
    def purgeUpdates__(self, updates, tolerances):
        # updates: [(channel, tv, (data, ...)),...]
        # tolerances: (tolerance,...)
        #
        # we can assume that self.get() returned True, i.e. all texisting updates
        # are in the past compared to incoming updates

        #if not tolerances:  return updates
        
        self.T['purgeUpdates'].begin()
        #print 'purgeUpdates: getData()..'
        #print 'purgeUpdates: getAllUpdates()..'
        
        my_updates = self.getAllUpdates()       # returns {channel:[(tv, (data,...)),...]}
        new_updates = []
        #print 'purgeUpdates: %d updates' % (len(updates),)
        
        my_data_dict = self.getDataAsDict()
        
        for upd in updates:
            chn, tv, data = upd
            my_tv, my_values = my_data_dict.get(chn, None)
            for upd_tv, upd_data in my_updates.get(chn, []):
                if upd_tv <= tv:    
                    my_values = upd_data
                    my_tv = tv
            
            close = False
            if my_values:
                close = True
                for j, x in enumerate(my_values):
                    new = data[j]
                    if x != new:
                        close = False
                        if type(x) in (type(1), type(1.0)) and tolerances:
                            t = tolerances[j]
                            #print 'tolerance[%d] = %s (%s)' % (
                            #        j, t, type(t))
                            if t and t > 0:
                                close = abs(x-new) <= t
                    if not close:   break
            if not close:
                new_updates.append(upd)
        self.T['purgeUpdates'].end()
        #print "pudates purged from %s to %s" % (len(updates), len(new_updates))
        return new_updates    
        
    def addUpdates(self, updates, tolerances):
        #
        # IMPORTANT: updates must be sorted by channel and then by tv
        #
        # updates: [(channel, tv, (data, ...)),...]
        self.fetchInfo()
        self.T['addUpdates'].begin()
        
        # debug:
        #updates = list(updates)
        #print ('updates before purge:', len(updates), "tolerances:", tolerances)

        updates = self.purgeUpdates(updates, tolerances)
        #print ('updates after purge:', len(updates))

        data_columns = self.Table.columns()
        format = ["%s" % (self.Id,), "%d", "%s"] + ["%s"]*len(data_columns)
        format = '\t'.join(format)
        lines = []
        for channel, tv, data in updates:
            line = format % ((channel, tv) + data)
            lines.append(line)
        #print 'updates lines:', len(lines)    
        data = io.StringIO('\n'.join(lines))
        #print 'insering..'
        #print "addUpdates: data: %s" % (lines,)
        c = self.Table.copy_from(data, "%t_update", 
            ['__snapshot_id', '__channel', '__tv']+data_columns)
        c.execute("commit")
        self.T['addUpdates'].end()

    def copyUpdatesFrom(self, s, tmin):
        data_columns = self.Table.columns()
        columns = ','.join(["__channel, __tv, __tr"] + data_columns)
        format = ["%s" % (self.Id,), "%d", "%s", "%s"] + ["%s"]*len(data_columns)
        format = '\t'.join(format)
        lines = []
        sql = "select distinct on (__channel, __tv) " + columns + """ from %t_update
                                where __snapshot_id = %s and __tv >= %s
                                order by __channel, __tv, __tr desc"""
        c = self.Table.execute(sql, (s.Id, tmin))
        tup = c.fetchone()
        while tup:
            lines.append(format % tup)
            if self.Updates:
                channel = tup[0]
                tv = tup[1]
                values = tup[3:]
                lst = self.Updates.get(channel, None)
                if not lst:
                    lst = []
                    self.Updates[channel] = lst
                lst.append((tv, values))
            tup = c.fetchone()
                
        lines = io.StringIO('\n'.join(lines))
        c = self.Table.copy_from(lines, "%t_update", 
            ['__snapshot_id', '__channel', '__tv', '__tr'] + data_columns)
        
    def addData__(self, data):
        # data: {channel:(data,...)}
        self.T['addData'].begin()
        data_columns = self.Table.columns()
        #print "CDSnapshot.addData(): data_columns=", data_columns
        #k = data.keys()[0]
        #print data[k]
        format = ["%s" % (self.Id,), "%d"] + ["%s"]*len(data_columns)
        format = '\t'.join(format)
        #print "format=<%s>" % (format,)
        dmp = []
        for channel, values in data.items():
            #print "channel, values=", channel, values
            dmp.append(format % ((channel,)+values))
        dmp = io.StringIO('\n'.join(dmp))
        c = self.Table.copy_from(dmp, "%t_snapshot_data", 
            ['__snapshot_id', '__channel']+data_columns)
        c.execute("commit")
        if not self.Data:   self.Data = {}
        self.T['addData'].end()
        self.Data.update(data)
        
    def addData(self, data):
        # data: [(channel, (data, ...)),...]
        data = list(data)
        #print "s.addData: %s" % (data,)
        data_iterator = ((channel, self.Tv, values) for channel, values in data)
        self.addUpdates(data_iterator, [])
        
    def tags(self):
        c = self.Table.execute("""select __tag_name
                                from %t_tag_snapshot
                                where __snapshot_id = %s""", (self.Id,))
        return [x[0] for x in c.fetchall()]
        
        
    def stats(self):
        return self.T.stats()
        
    def printStats(self):
        return self.T.printStats()
        
    def latestUpdate(self):
        c = self.Table.execute("""select max(__tv) from %t_update
                where __snapshot_id=%s""", (self.Id,))
        tup = c.fetchone()
        #print("Snapshot.latestUpdate: my id:", self.Id, "  tup:", tup)
        return tup and tup[0]

    def updateCount(self):
        c = self.Table.execute("""select count(*) from %t_update
                where __snapshot_id = %s""", (self.Id,))
        tup = c.fetchone()
        return tup[0]

class Segment:
    
    def __init__(self, s = None, tv = None, tend = None, snapshot = None):
        if snapshot == None:
            self.Snapshot = s
            self.Tv = tv
            self.Tend = tend
        else:
            self.Snapshot = snapshot
            self.Tv = snapshot.Tv
            self.Tend = snapshot.Tv_end
            
    def __str__(self):
        return "<Segment %s %s...%s (%s...%s)>" % (
                    self.Snapshot, self.Tv, self.Tend if self.Tend != None else "infinity",
                        epoch(self.Tv), epoch(self.Tend) if self.Tend != None else "")
                    
    __repr__ = __str__

    def overlap(self, t0, t1):
        # computes overlap with the given time segment (t1 can be None)
        # returns list of tuples (ta, tb, flag). 
        # flag = 1 if in current segment but not in the given interval
        # flag = 2 if in given interval but not in the current segment
        # flag = 3 if in both
        
        lst = [self.Tv, t0]
        if self.Tend != None:   lst.append(self.Tend)
        if t1 != None:          lst.append(t1)
        lst.sort()
        if (t1 == None or self.Tend == None):    lst.append(None)
        out = []
        for i, ta in enumerate(lst[:-1]):
            tb = lst[i+1]
            if ta != tb:
                flag = 0
                if tb == None:  
                    if type(ta) in (type(1), type(1.0)):
                        t = ta + 1
                    else:
                        t = ta + datetime.timedelta(seconds=1)
                else:           t = ta + (tb - ta)/2
                if t >= self.Tv and (self.Tend == None or t < self.Tend):
                    flag = 1
                if t >= t0 and (t1 == None or t < t1):
                    flag += 2
                
                if flag:
                    out.append((ta, tb, flag))
                    
        return out
            
    def clone(self):
        return Segment(self.Snapshot, self.Tv, self.Tend)
       
    def shadow(self, snapshot):
        segments = self.overlap(snapshot.Tv, snapshot.Tv_end)
        return [Segment(self.Snapshot, t0, t1) for t0, t1, flag in segments 
            if flag == 1 and (t1 == None or t1 > t0)]
        
                        
class CDTable:

    MAXUPDATES = 1000000

    CreateTables = """
create table %t_tag
(
    __name        text    primary key,
    __tr          double precision,
    __created     timestamp with time zone default current_timestamp,
    __comment     text    default ''
);

create table %t_update
(
    __tv                      double precision,
    __tr                      double precision,   
    __channel                 int,
    __data_type               text,
    primary key (__tv, __tr, __channel, __data_type)
    %d
);

create index %t_update_tr on %t_update(__tr);
create index %t_update_data_type on %t_update(__data_type);
"""


    DropTables = """
        drop table %t_tag_snapshot;
        drop table %t_tag cascade;
        drop table %t_snapshot_data;
        drop table %t_update;
        drop table %t_snapshot cascade;
    """

    StructureColumns = ["__channel", "__tv", "__tr", "__data_type"]
    
    def __init__(self, db, name, columns):
        self.Name = name
        self.Columns = columns
        self.AllColumns = self.StructureColumns + columns
        self.DB = db
        words = name.split(".",1)
        if len(words) == 2:
            self.TableName = words[1]
            self.Namespace = words[0]
        else:
            self.TableName = words[0]
            self.Namespace = ""

    def readDataColumnsFromDB(self):
        dig = DbDig(self.DB.connect())
        words = self.Name.split('.')
        ns = 'public'
        name = self.Name
        if len(words) > 1:
            ns = words[0]
            name = words[1]
        columns = dig.columns(ns, self.Name + "_update")
        if not columns:
            raise ValueError("Not a conditions DB table (update table not found)")
        #print "readDataColumnsFromDB(%s): columns: %s" % (self.Name, columns)
        columns = [x[0] for x in columns]
        for c in ("__snapshot_id","__tv","__channel","__tr"):
            if c in columns:
                columns.remove(c)
        self.Columns = columns
        if not self.validate():
            self.Columns = []
            raise ValueError("Not a conditions DB table (verification failed)")
        
    def columns(self):
        return self.Columns

    def execute(self, sql, args=()):
        #print "Table.execute(%s, %s)" % (sql, args)
        return self.DB.execute(self.Name, sql, args)

    def copy_from(self, data, table, columns):
        return self.DB.copy_from(self.Name, data, table, columns)

    @staticmethod
    def create(db, name, column_types, owner, grants = {}, drop_existing=False):
        columns = [c for c,t in column_types]
        t = CDTable(db, name, columns)
        t.createTables(column_types, owner, grants, drop_existing)
        return t

    def tableNames(self):
        #return [self.Name + "_" + s for s in ("snapshot", "tag", "tag_snapshot", 
        #            "snapshot_data", "update")]

        return [self.Name + "_" + s for s in ("tag", "update")]

    def dataTableNames(self):
        #return [self.Name + "_" + s for s in ("snapshot_data", "update")]
        return [self.Name + "_" + s for s in ("update",)]

    def validate(self):
        # check if all necessary tables exist and have all the columns
        c = self.DB.cursor()
        for t in self.tableNames():
            try:    c.execute("select * from %s limit 1" % (t,))
            except: 
                c.execute("rollback")
                return False
        try:    c.execute("select __type from %s_snapshot limit 1" % (self.Name,))
        except: 
            c.execute("rollback")
            return False
        if self.Columns:
            columns = ','.join(self.Columns)
            for t in self.dataTableNames():
                try:    c.execute("select %s from %s limit 1" % (columns, t))
                except: 
                    c.execute("rollback")
                    return False
        return True
        
    exists = validate       # alias
            
    def createTables(self, column_types, owner = None, grants = {}, 
                    drop_existing=False):
        exists = True
        c = self.DB.cursor()
        try:    
            c.execute("""select * from %s_snapshot limit 1""" % (self.Name,))
        except: 
            c.execute("rollback")
            exists = False
        if exists and drop_existing:
            self.execute(self.DropTables)
            exists = False
        if not exists:
            c = self.DB.cursor()
            if owner:
                c.execute("set role %s" % (owner,))
            columns = ",".join(["%s %s" % (n,t) for n,t in column_types])
            sql = self.CreateTables.replace("%d", columns)
            self.execute(sql)
            read_roles = ','.join(grants.get('r',[]))
            if read_roles:
                grant_sql = """grant select on 
                        %t_tag,
                        %t_update
                        to """ + read_roles         # + %t_snapshot_data,
                #print grant_sql
                self.execute(grant_sql)
            write_roles = ','.join(grants.get('w',[]))
            if write_roles:
                grant_sql = """grant insert, delete, update on 
                        %%t_tag,
                        %%t_update
                        to %(roles)s; 
                    grant all on %%t_snapshot___id_seq to %(roles)s;""" % {'roles':write_roles}     # +%%t_snapshot_data,
                #print grant_sql
                self.execute(grant_sql)
            c.execute("commit")
                        
    def tags(self):
        c = self.execute("""select __name from %t_tag order by __name""", ())
        return [x[0] for x in c.fetchall()]
        
    def dataTypes(self):
        c = self.execute("""select distinct __data_type from %t_update order by __data_type""", ())
        return [x[0] for x in c.fetchall()]

    def split_update_tuple(self, tup):
        n_struct = len(self.StructureColumns)
        struct, data = tup[:n_struct], tup[n_struct:]
        structure_values = {cn:v for cn, v in zip(self.StructureColumns, struct_values)}
        return structure_values, data

    def getData(self, t, tag=None, tr=None, data_type=None, channel_range=None, conditions=[]):
        # returns iterator [(channel, tv, data, ...)] unsorted
        #print "getData: tr=%s" % (tr,)

        if data_type is not None:
            # merge with common data
            common_data = self.getDataIter(t, tag, tr, None, channel_range = channel_range,
                    conditions = conditions)
            for tup in common_data:
                yield tup

        data_columns = self.Columns
        all_columns = self.all_columns(prefix="u", as_text=True)

        params = {
            "tv":   t,
            "tag":  tag,
            "tr":   tr,
            "data_type": data_type,
            "min_channel": channel_range[0] if channel_range else None,
            "max_channel": channel_range[1] if channel_range else None
        }

        if tag is not None:
            c = self.execute(f"""
                select {all_columns} from %t_update u, %t_tag t
                    where u.__tv <= %(tv)s
                        and t.__tr <= t.__tr
                        and t.__name = %(tag)s
                        and (%(data_type)s is null or u.__data_type = %(data_type)s)
                        and (%(min_channel)s is null or u.__channel >= %(min_channel)s)
                        and (%(max_channel)s is null or u.__channel <= %(max_channel)s)
                    order by u.__channel, u.__tv, u.__tr desc
            """, params)
        else:
            c = self.execute(f"""
                select {all_columns} from %t_update u
                    where u.__tv <= %(tv)s
                        and (%(tr)s is null or t.__tr <= %(tr)s)
                        and (%(data_type)s is null or u.__data_type = %(data_type)s)
                        and (%(min_channel)s is null or u.__channel >= %(min_channel)s)
                        and (%(max_channel)s is null or u.__channel <= %(max_channel)s)
                    order by u.__channel, u.__tv, u.__tr desc
            """, params)
        
        last_tr = 0
        last_channel = None
        for tup in cursor_iterator(c):
            structure_values, data = self.split_update_tuple(tup)
            channel, tr, tv = structure_values["__channel"], structure_values["__tr"], structure_values["__tv"]
            if channel != last_channel:
                last_channel = channel
                last_tr = tr
                yield tup
            elif tr >= last_tr:
                yield tup
                last_tr = tr

    def getDataInterval(self, t1, t2, tag=None, tr=None, data_type=None, channel_range=None,
                    conditions = []):
                    
        # initial data
        yield from self.getDataIter(self, t1, tag=tag, tr=tr, data_type=data_type, channel_range=channel_range, conditions=[]):
        
        data_columns = self.Columns
        all_columns = self.all_columns(prefix="u", as_text=True)

        params = {
            "tv1":   t1,
            "tv2":   t2,
            "tag":  tag,
            "tr":   tr,
            "data_type": data_type,
            "min_channel": channel_range[0] if channel_range else None,
            "max_channel": channel_range[1] if channel_range else None
        }
        
        if tag is not None:
            c = self.execute(f"""
                select {all_columns} from %t_update u, %t_tag t
                    where u.__tv > %(tv1)s
                        and u.__tv <= %(tv2)s
                        and t.__tr <= t.__tr
                        and t.__name = %(tag)s
                        and (%(data_type)s is null or u.__data_type = %(data_type)s)
                        and (%(min_channel)s is null or u.__channel >= %(min_channel)s)
                        and (%(max_channel)s is null or u.__channel <= %(max_channel)s)
                    order by u.__channel, u.__tv, u.__tr desc
            """, params)
        else:
            c = self.execute(f"""
                select {all_columns} from %t_update u
                    where u.__tv > %(tv1)s
                        and u.__tv <= %(tv2)s
                        and (%(tr)s is null or t.__tr <= %(tr)s)
                        and (%(data_type)s is null or u.__data_type = %(data_type)s)
                        and (%(min_channel)s is null or u.__channel >= %(min_channel)s)
                        and (%(max_channel)s is null or u.__channel <= %(max_channel)s)
                    order by u.__channel, u.__tv, u.__tr desc
            """, params)
        
        last_tr = 0
        last_channel = None
        for tup in cursor_iterator(c):
            structure_values, data = self.split_update_tuple(tup)
            channel, tr, tv = structure_values["__channel"], structure_values["__tr"], structure_values["__tv"]
            if channel != last_channel:
                last_channel = channel
                last_tr = tr
                yield tup
            elif tr >= last_tr:
                yield tup
                last_tr = tr

    def addData(self, data, data_type=None):
        # data: [(channel, tv, (data, ...)),...]
        csv_rows = []
        tr = time.time()
        for channel, tv, payload in data:
            row = [str(x) for x in (channel, tv, tr, data_type) + tuple(payload)]
            csv_rows.append("\t".join(row))
            
        csv = io.StringIO('\n'.join(rows_csv))
        self.copy_from(csv, "%t_update", ["__channel", "__tv", "__tr", "__data_type"] + self.Columns)
                    
    def tag(self, tag, comment="", override=False, tr=None):
        
        tr = tr or time.time()
        if override:
            c = self.execute("""
                insert into %t_tag(__tr, __name, __comment)
                    values(%s, %s, %s)
                on conflict(__name)
                do update
                    set __tr = %s, __comment = %s
                        where __name = %s
            """, (tr, tag, comment, tr, comment, tag))
        else:
            c = self.execute("""
                insert into %t_tag(__tr, __name, __comment)
                    values(%s, %s, %s)
            """, (tr, tag, comment))
        c.execute("commit")

    def copyTag(self, tag, new_tag, comment="", override=False):
        c = self.execute("select __tr from %t_tag where __name=%s", (tag,))
        tup = c.fetchone()
        tr = None
        if tup: tr = tup[0]
        if tr is not None:
            self.tag(new_tag, comment=comment, override=override, tr=tr)
