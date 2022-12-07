import psycopg2, sys
import cStringIO

from trace import Tracer
from dbdig import DbDig

class dict_with_default:
    def __init__(self, default = None):
        self.Default = default
        self.Dict = {}

    def __str__(self):
        return "dict_with_default(%s, %s)" % (self.Default, self.Dict)
        
    def __getitem__(self, key):
        if self.Dict.has_key(key):      return self.Dict[key]
        else:                   return self.Default
        
    def get(self, key, default=None):
        if self.Dict.has_key(key):   return self.Dict[key]
        else:                   return default
        
    def __getattr__(self, x):
        return getattr(self.Dict, x)

class ConDB:
    def __init__(self, connstr):
        self.Conn = None
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
        
    def createTable(self, name, column_types, owner=None,
                    grants = {}, drop_existing=False):
        t = CDTable.create(self, name, column_types, owner, 
                    grants, drop_existing)
        return t

    def execute(self, table, sql, args=()):
        #print "DB.execute(%s, %s, %s)" % (table, sql, args)
        sql = sql.replace('%t', table)
        c = self.cursor()
        #print "executing: <%s>" % (sql,)
        c.execute(sql, args)
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
        return '<CDSnapshot id=%s type=%s tr=%s tv=%s deleted=%s>' % \
            (self.Id, self.DataType, self.Tr, self.Tv, self.Deleted)

    __repr__ = __str__

    @staticmethod
    def create(table, tv, data_type):
        c = table.execute("""insert into %t_snapshot(__tv, __type)
                values(%s, %s);
                select lastval();""", (tv, data_type))
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

    def getData(self, channel_range = None):  
        # returns {channel:(data,...)}, tv
        #print 'CDSnapshot.getData(): self.Data=%s' % (self.Data,)
        self.T['getData'].begin()
        
        #if not self.Data:
        #    self.Data = self.Table.getCachedSnapshotData(self.Id)
        
        if not self.Data:  
            self.T['getData/fetch'].begin()
            self.fetchInfo()
            data_columns = self.Table.columns()
            columns = ','.join(["__channel"] + data_columns)
            channels = ""
            if channel_range:
                channels = " and __channel between %s and %s " % channel_range
            sql = "select " + columns + """ from %t_snapshot_data
                                    where __snapshot_id = %s""" + channels
            #print sql
            c = self.Table.execute(sql, (self.Id,))
            tup = c.fetchone()
            dct = {}
            while tup:
                #print 'data tuple: %s' % (tup,)
                dct[tup[0]] = tup[1:]
                tup = c.fetchone()
            if not channel_range:   self.Data = dict
            data = dct
            self.T['getData/fetch'].end()
        else:
            if channel_range:
                data = {}
                for c in range(channel_range[0], channel_range[1]+1):
                    data[c] = self.Data.get(c)
            else:
                data = self.Data
        #self.Table.cacheSnapshotData(self.Id, data)
        #data.update(self.Data)
        self.T['getData'].end()
        return data, self.Tv

    def getUpdateCount(self):
        sql = """select count(*) from %t_update
                    where __snapshot_id=%s"""
        c = self.Table.execute(sql, (self.Id,))
        tup = c.fetchone()
        if not tup: return 0
        return tup[0]
        
    def getUpdates(self, tv, tr, channel_range = None):
        # returns {channel:(data,...)},{channel:tv}
        self.fetchInfo()
        data_columns = self.Table.columns()
        columns = ','.join(["__channel, __tv"] + data_columns)
        tr_where = ''
        if tr:  tr_where = "and __tr < '%s'" % (tr.strftime("%Y-%m-%d %H:%M:%S"),)
        channels = ""
        if channel_range:
            channels = " and __channel between %s and %s " % channel_range
        sql = "select distinct on (__channel) " + \
                        columns + \
                        """ from %t_update
                                where __snapshot_id = %s
                                    and __tv <= %s """ + \
                                    tr_where + channels + \
                            " order by __channel, __tv desc, __tr desc"""
        c = self.Table.execute(sql, (self.Id, tv))
        tup = c.fetchone()
        data = {}
        validity = {}
        while tup:
            channel, tv = tup[:2]
            data[channel] = tup[2:]
            validity[channel] = tv
            tup = c.fetchone()
        return data, validity
        
    def getAllUpdates(self):
        # returns {channel:[(tv, (data,...)),...]}
        
        self.T['getAllUpdates'].begin()
        if self.Updates == None:
            self.T['getAllUpdates/fetch'].begin()
            #print 'getAllUpdates: getting updates from the DB..'
            data_columns = self.Table.columns()
            columns = ','.join(["__channel, __tv"] + data_columns)
            sql = "select distinct on (__channel, __tv) " + columns + """ from %t_update
                                    where __snapshot_id = %s
                                    order by __channel, __tv, __tr desc"""
            c = self.Table.execute(sql, (self.Id,))
            out = {}
            tup = c.fetchone()
            while tup:
                channel, tv = tup[:2]
                if not out.has_key(channel):
                    out[channel] = []           # {tv -> tuple}
                old_tuples = out[channel]
                old_tuples.append((tv, tup[2:]))
                tup = c.fetchone()
            self.Updates = out
            self.T['getAllUpdates/fetch'].end()
        self.T['getAllUpdates'].end()
        return self.Updates

    def getUpdatesForChannel(self, channel):
        upd = self.getAllUpdates()
        return upd.get(channel, [])
        
    def getValues(self, channel, t):
        self.T['getValues'].begin()
        data0, tv0 = self.getData()
        tup0 = data0.get(channel, None)
        upd = self.getUpdatesForChannel(channel)
        tup = tup0
        tv = tv0
        for utv, utup in upd:
            if utv > t: break
            tup = utup
            tv = utv
        self.T['getValues'].end()
        return tup, tv            
        
    def getUpdatesInRange(self, tmin, tmax, tr, channel_range = None):
        # returns {channel:[(tv,data),...]}
        self.fetchInfo()
        data_columns = self.Table.columns()
        columns = ','.join(["__channel, __tv"] + data_columns)
        tr_where = ""
        if tr:  tr_where = "and __tr < '%s'" % (tr.strftime("%Y-%m-%d %H:%M:%S"),)
        channels = ""
        if channel_range:
            channels = " and __channel between %s and %s " % channel_range
        sql = "select distinct on (__channel, __tv)" + columns + """ from %t_update
                                where __snapshot_id = %s
                                    and __tv <= %s """ + tr_where + channels + \
                                    " order by __channel, __tv, __tr desc"""
        c = self.Table.execute(sql, (self.Id, tmax))
        tup = c.fetchone()
        dict = {}
        while tup:
            channel, tv = tup[:2]
            lst = dict.get(channel, None)
            #print tmin, tmin.tzinfo, tv, tv.tzinfo
            if tmin.tzinfo == None and tv.tzinfo != None:
                tmin = tmin.replace(tzinfo = tv.tzinfo)
            #print tmin, tv
            if not lst or tv <= tmin:
                lst = []
                dict[channel] = lst
            lst.append((tv, tup[2:]))
            tup = c.fetchone()
        return dict
        
    def invalidateCache(self):
        self.Data = None
        self.Updates = None
            
    def addData(self, data):
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
        dmp = cStringIO.StringIO('\n'.join(dmp))
        c = self.Table.copy_from(dmp, "%t_snapshot_data", 
            ['__snapshot_id', '__channel']+data_columns)
        c.execute("commit")
        if not self.Data:   self.Data = {}
        self.T['addData'].end()
        self.Data.update(data)

    def purgeUpdates(self, updates, tolerances):
        # updates: [(channel, tv, (data, ...)),...]
        # tolerances: (tolerance,...)
        self.T['purgeUpdates'].begin()
        #print 'purgeUpdates: getData()..'
        my_data, tv0 = self.getData()
        #print 'purgeUpdates: getAllUpdates()..'
        my_updates = self.getAllUpdates()
        new_updates = []
        #print 'purgeUpdates: %d updates' % (len(updates),)
        for upd in updates:
            chn, tv, data = upd
            my_values, my_tv = self.getValues(chn, tv)
            #print 'old: %s %s' % (chn, my_values)
            #print 'upd: %s %s' % (chn, data)
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
        # updates: {channel:[(tv, data),]}
        # or [(channel, tv, (data,...)), ]
        self.fetchInfo()
        self.T['addUpdates'].begin()
        #print 'updates before purge:', len(updates), "tolerances:", tolerances
        updates = self.purgeUpdates(updates, tolerances)
        #print 'updates after purge:', len(updates)
        data_columns = self.Table.columns()
        format = ["%s" % (self.Id,), "%d", "%s"] + ["%s"]*len(data_columns)
        format = '\t'.join(format)
        lines = []
        for channel, tv, data in updates:
            line = format % ((channel, tv) + data)
            lines.append(line)
        #print 'updates lines:', len(lines)    
        data = cStringIO.StringIO('\n'.join(lines))
        #print 'insering..'
        c = self.Table.copy_from(data, "%t_update", 
            ['__snapshot_id', '__channel', '__tv']+data_columns)
        c.execute("commit")
        self.T['addUpdates'].end()
        
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
        return tup and tup[0]

    def updateCount(self):
        c = self.Table.execute("""select count(*) from %t_update
                where __snapshot_id = %s""", (self.Id,))
        tup = c.fetchone()
        return tup[0]

        
class CDTable:

    MAXUPDATES = 1000000

    CreateTables = """
create table %t_snapshot
(   
    __id          serial primary key,
    __tv          timestamp with time zone,
    __tv_end      timestamp with time zone,
    __tr          timestamp with time zone default current_timestamp,
    __deleted     boolean default 'false',
    __type        text
);

create table %t_tag
(
    __name        text    primary key,
    __created     timestamp with time zone default current_timestamp,
    __comment     text    default ''
);

create table %t_tag_snapshot
(
    __snapshot_id int  references %t_snapshot(__id) on delete cascade,
    __tag_name    text    references %t_tag(__name) on delete cascade,
    primary key (__snapshot_id, __tag_name)
);

create table %t_snapshot_data
(
    __snapshot_id int         references %t_snapshot(__id) on delete cascade,
    __channel     int,
    %d,
    primary key(__snapshot_id, __channel)
);

create table %t_update
(
    __snapshot_id int         references %t_snapshot(__id) on delete cascade,
    __tv                      timestamp with time zone,
    __tr                      timestamp with time zone default current_timestamp,   
    __channel                 int,
    %d,
    primary key(__snapshot_id, __tv, __channel)
);"""

    DropTables = """
        drop table %t_tag_snapshot;
        drop table %t_tag cascade;
        drop table %t_snapshot_data;
        drop table %t_update;
        drop table %t_snapshot cascade;
    """

    def __init__(self, db, name, columns):
        self.Name = name
        self.Columns = columns
        self.DB = db

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
            raise ValueError, "Not a conditions DB table (update table not found)"
        #print "readDataColumnsFromDB(%s): columns: %s" % (self.Name, columns)
        columns = [x[0] for x in columns]
        for c in ("__snapshot_id","__tv","__channel","__tr"):
            if c in columns:
                columns.remove(c)
        self.Columns = columns
        if not self.validate():
            self.Columns = []
            raise ValueError, "Not a conditions DB table (verification failed)"
        
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
        return [self.Name + "_" + s for s in ("snapshot", "tag", "tag_snapshot", 
                    "snapshot_data", "update")]

    def dataTableNames(self):
        return [self.Name + "_" + s for s in ("snapshot_data", "update")]

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
                        %t_snapshot,
                        %t_tag,
                        %t_tag_snapshot,
                        %t_snapshot_data,
                        %t_update
                        to """ + read_roles
                #print grant_sql
                self.execute(grant_sql)
            write_roles = ','.join(grants.get('w',[]))
            if write_roles:
                grant_sql = """grant insert, delete, update on 
                        %%t_snapshot,
                        %%t_tag,
                        %%t_tag_snapshot,
                        %%t_snapshot_data,
                        %%t_update
                        to %(roles)s; 
                    grant all on %%t_snapshot___id_seq to %(roles)s;""" % {'roles':write_roles}
                #print grant_sql
                self.execute(grant_sql)
            c.execute("commit")
                        
    def purgeShadowedSnapshots(self, lst):
        # lst: [snapshot, ...]
        # make sure snapshots are ordered by tr
        lst.sort(lambda x,y:    cmp(x.Tr, y.Tr))
        out = []
        n = len(lst)
        for i, s in enumerate(lst):
            copy = True
            j = i + 1
            while j < n:
                if lst[j].Tv <= s.Tv:
                    copy = False
                    break
                j += 1
            if copy:
                out.append(s)
        #print "purged: ", lst
        return out


    def snapshots(self, tag=None, tr=None, data_type=None):
        #print "snapshots: name=%s" % (self.Name,)
        type_where = ''
        if data_type != None:
            type_where = " and __type = '%s' " % (data_type,)
            
        if tag != None:
            c = self.execute("""select s.__id 
                            from %t_snapshot s, %t_tag_snapshot t
                            where not __deleted
                                and s.__id = t.__snapshot_id
                                and t.__tag_name = %s """ +
                                type_where + 
                            "order by s.__tr", (tag,))      
        elif tr != None:      
            c = self.execute("""select __id from %t_snapshot
                            where not __deleted
                                and __tr <= %s """ + type_where + 
                            "order by __tr", (tr,))
        else:        
            c = self.execute("""select __id from %t_snapshot
                            where not __deleted """ +
                                type_where + 
                            "order by __tr", ())
                            
        snapshots = []
        for tup in c.fetchall():
            snapshot = CDSnapshot(self, tup[0])
            snapshot.fetchInfo()
            snapshots.append(snapshot)
        
        return snapshots

    def findSnapshot(self, t, tag=None, tr=None, data_type=None):
        type_where = " and __type is null "
        if data_type != None:
            type_where = " and __type = '%s' " % (data_type,)
            
        #print type_where
            
        if tag != None:
            c = self.execute("""select s.__id 
                            from %t_snapshot s, %t_tag_snapshot t
                            where not __deleted
                                and (s.__tv_end is null or s.__tv_end > %s)
                                and s.__tv <= %s
                                and s.__id = t.__snapshot_id
                                and t.__tag_name = %s """ +
                                type_where + 
                            "order by s.__tr desc limit 1", (t, t, tag))      
        elif tr != None:      
            c = self.execute("""select __id from %t_snapshot
                            where not __deleted
                                and (__tv_end is null or __tv_end > %s)
                                and __tv <= %s
                                and __tr <= %s """ + type_where + 
                            "order by __tr desc limit 1", (t, t, tr))
        else:        
            c = self.execute("""select __id from %t_snapshot
                            where not __deleted
                                and (__tv_end is null or __tv_end > %s)
                                and __tv <= %s """ +
                                type_where + 
                            "order by __tr desc limit 1", (t, t))
        
        tup = c.fetchone()
        if not tup: return None
        snapshot = CDSnapshot(self, tup[0])
        snapshot.fetchInfo()
        return snapshot

    def findSnapshots(self, t1, t2, tag=None, tr=None, data_type=None):
        #print "findSnapshots(%s, %s, %s, %s, %s)" % (t1, t2, tag, tr, data_type)
        s0 = self.findSnapshot(t1, tag=tag, tr=tr, data_type=data_type)
        
        #print "s0=", s0

        type_where = " and __type is null "
        if data_type != None:
            type_where = " and __type = '%s' " % (data_type,)

        if tag != None:
            #print "tag"
            c = self.execute("""
                        select s.__id, s.__tv, s.__tr
                            from %t_snapshot s, %t_tag_snapshot t
                            where not __deleted
                                and s.__tv >= %s
                                and s.__tv < %s
                                and (s.__tv_end is null or s.__tv_end > %s)
                                and s.__id = t.__snapshot_id
                                and t.__tag_name = %s """ + type_where +
                            "order by s.__tr", (t1, t2, t1, tag))      
        elif tr != None:      
            #print "tr"
            c = self.execute("""
                        select __id, __tv, __tr
                            from %t_snapshot
                            where not __deleted
                                and __tv >= %s
                                and __tv < %s
                                and (__tv_end is null or __tv_end > %s)
                                and __tr < %s """ + type_where +
                            "order by __tr", (t1, t2, t1, tr))
        else:        
            #print "else"
            c = self.execute("""
                        select __id, __tv, __tr
                            from %t_snapshot
                            where not __deleted
                                and __tv >= %s
                                and __tv < %s
                                and (__tv_end is null or __tv_end > %s) """ + type_where +
                            "order by __tr", (t1, t2, t1))

        lst = c.fetchall()
        #print "lst=", lst
        if s0:
            lst = [(sid, tv, tr) for sid, tv, tr in lst if sid != s0.Id]    # remove s0 if it is there

        lst = [CDSnapshot(self, sid, tv=tv, tr=tr).fetchInfo() for sid, tv, tr in lst]
        if s0:
            lst = [s0] + lst
        #print "other snapshots=",lst
        
        #print "lst1=", lst
        out = self.purgeShadowedSnapshots(lst)
        #print "out=", out
        return out
            
    def getData(self, t, tag=None, tr=None, data_type=None, channel_range=None):
        # returns {channel:(data,)},{channel:tv}
        #print "getData: tr=%s" % (tr,)
        s = self.findSnapshot(t, tag=tag, tr=tr, data_type=data_type)
        if s == None: return {}, {}
        #print 'snapshot for t=%s: %s' % (t,s)
        d0, tv = s.getData(channel_range = channel_range)
        data = d0.copy()
        #print data
        updates, validity = s.getUpdates(t, tr, channel_range = channel_range)
        outv = dict_with_default(tv)
        for channel, tup in updates.items():
            outv[channel] = validity[channel]
            data[channel] = tup
        #print "outv=",outv
        #s.invalidateCache()  
        
        if data_type != None:
            # merge with common data
            common_data, common_tv = self.getData(t, tag, tr, None, channel_range = channel_range)
            #print common_data, common_tv
            if common_data:
                for channel, common_tup in common_data.items():
                    ctv = common_tv[channel]
                    if not data.has_key(channel) or ctv > outv[channel]:
                        data[channel] = common_tup
                        outv[channel] = ctv
        return data, outv

    def getDataInterval(self, t1, t2, tag=None, tr=None, data_type=None, channel_range=None):
        #print "tag=", tag, " data_type=", data_type, "%s %s %s" % (t1, t2, tr)
        snapshots = self.findSnapshots(t1, t2, tag=tag, tr=tr, data_type=data_type)
        #print "got snapshots1:", t1, t2, snapshots
        if data_type != None:
            snapshots += self.findSnapshots(t1, t2, tag=tag, tr=tr, data_type=None)
            snapshots.sort(lambda x,y: cmp(x.Tv, y.Tv))
        #print "got snapshots:", t1, t2, snapshots
        output = {}       # {channel:[(tv, data),...]}
        for i, s in enumerate(snapshots):
            next_tv = None
            if i+1 < len(snapshots):
                next_tv = snapshots[i+1].Tv
            #print "calling getData..."
            
            sdata, stv = s.getData(channel_range = channel_range)
            #print "got snapshot data"
            for channel, tup in sdata.items():
                lst = output.get(channel, None)
                if not lst: 
                    lst = []
                    output[channel] = lst
                lst.append((stv,tup))
            #print "calling getUpdates..."
            updates = s.getUpdatesInRange(t1, t2, tr, channel_range = channel_range)
            #print "updates: ", len(updates)
            for channel, ulst in updates.items():
                lst = output.get(channel, None)
                t_first_update = ulst[0][0]
                if not lst or t_first_update <= t1: 
                    lst = []
                    output[channel] = lst
                output[channel] = lst + ulst
        #print "Output:", len(output)
        return output           
             
    def getDataInterval(self, t1, t2, tag=None, tr=None, data_type=None, channel_range=None):
        #print "tag=", tag, " data_type=", data_type, "%s %s %s" % (t1, t2, tr)
        snapshots = self.findSnapshots(t1, t2, tag=tag, tr=tr, data_type=data_type)
        #print "got snapshots1:", t1, t2, snapshots
        if data_type != None:
            snapshots += self.findSnapshots(t1, t2, tag=tag, tr=tr, data_type=None)
            snapshots.sort(lambda x,y: cmp(x.Tv, y.Tv))
        #print "got snapshots:", t1, t2, snapshots
        output = {}       # {channel:[(tv, data),...]}
        for i, s in enumerate(snapshots):
            next_tv = None
            if i+1 < len(snapshots):
                next_tv = snapshots[i+1].Tv
            #print "calling getData..."
            
            sdata, stv = s.getData(channel_range = channel_range)
            #print "got snapshot data"
            for channel, tup in sdata.items():
                lst = output.get(channel, None)
                if not lst: 
                    lst = []
                    output[channel] = lst
                lst.append((stv,tup))
            #print "calling getUpdates..."
            updates = s.getUpdatesInRange(t1, t2, tr, channel_range = channel_range)
            #print "updates: ", len(updates)
            for channel, ulst in updates.items():
                lst = output.get(channel, None)
                t_first_update = ulst[0][0]
                if not lst or t_first_update <= t1: 
                    lst = []
                    output[channel] = lst
                output[channel] = lst + ulst
        #print "Output:", len(output)
        return output                
                
    def getDataIntervalIterNew(self, t1, t2, tag=None, tr=None, data_type=None, channel_range=None):
        #print "tag=", tag, " data_type=", data_type, "%s %s %s" % (t1, t2, tr)
        snapshots = self.findSnapshots(t1, t2, tag=tag, tr=tr, data_type=data_type)
        #print "got snapshots1:", t1, t2, snapshots
        if data_type != None:
            snapshots += self.findSnapshots(t1, t2, tag=tag, tr=tr, data_type=None)
            snapshots.sort(lambda x,y: cmp(x.Tv, y.Tv))
        #print "got snapshots:", t1, t2, snapshots
        output = {}       # {channel:[(tv, data),...]}
        for i, s in enumerate(snapshots):
            next_tv = None
            if i+1 < len(snapshots):
                next_tv = snapshots[i].Tv
            #print "calling getData..."
            
            tfrom = t1
            tto = min(t2, next_tv)
            
            sdata = s.getDataIntervalIter(tfrom, tto, tr, channel_range)
            for update in sdata:
                yield update
                
    def createSnapshot(self, t, prefill = True, data = {}, data_type=None):
        # data: {channel:(data,...)}
        s = CDSnapshot.create(self, t, data_type)
        if prefill:
            base, v = self.getData(t)
            base.update(data)
            data = base
        if data:
            s.addData(data)
            #print "Added data:", len(data)
        return s

    def addData(self, data, tolerances = None, data_type=None):
        # data: [(channel, tv, (data, ...)),...]
        # tolerances: (tolerance,...)
        # sort by channel then by tv
        tmin = min((x[1] for x in data))
        s0 = self.findSnapshot(tmin, data_type=data_type)
        #print 'found snapshot:', s0
        new_snapshot = (not s0) or (not not s0.tags())
        #print 'new_snapshot=%s' % (new_snapshot,)
        if not new_snapshot:
            # see if existng snapshot overlaps with our data
            #print 'getting updates..'
            updates = s0.getAllUpdates()
            for channel, tv, tup in data:
                old_lst = updates.get(channel,[])
                if old_lst:
                    old_tv, old_data = old_lst[-1]
                    if old_tv > tv:
                        new_snapshot = True
                        break

        #print 'new_snapshot=%s' % (new_snapshot,)
        if not new_snapshot:
            n = s0.getUpdateCount()
            #print "Updates: ", n
            new_snapshot = n > self.MAXUPDATES

        #print 'new_snapshot=%s' % (new_snapshot,)
        if new_snapshot:
            # need new snapshot
            data0, validity = self.getData(tmin, data_type=data_type)
            #print 'data0=', data0
            s0 = CDSnapshot.create(self, tmin, data_type)
            # update all channels if their tv equals the tv of the snapshot
            for chn, tv, values in data:
                if tmin == tv:
                    data0[chn] = values
            s0.addData(data0)
        #print 'addUpdates..', len(data)
        s0.addUpdates(data, tolerances)
        #print 'invalidateCache..'
        s0.invalidateCache()
        #print 'done'
        #s0.printStats()
        return s0
                    
    def tag(self, tag, comment="", override=False):

        if override:
            self.execute("""
                delete from %t_tag_snapshot where __tag_name = %s;
                delete from %t_tag where __name = %s""", (tag, tag))
    
        c = self.execute("""
            insert into %t_tag(__name, __comment)
                values(%s, %s);
            select __id from %t_snapshot
                where not __deleted""", (tag, comment))
        ids = list(c.fetchall())
        for tup in ids:
            sid = tup[0]
            self.execute("""
                insert into %t_tag_snapshot(__tag_name, __snapshot_id)
                    values(%s, %s)""", (tag, sid))
        self.execute("commit", ())
                                       
