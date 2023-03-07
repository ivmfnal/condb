import psycopg2, sys, time, datetime
import io
from psycopg2.errors import UndefinedTable

#from dbdig import DbDig

def cursor_iterator(c):
    tup = c.fetchone()
    while tup:
        yield tup
        tup = c.fetchone()

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

    def createTable(self, name, column_types, owner=None, grants = {}, drop_existing=False):
        t = CDTable.create(self, name, column_types, owner, grants, drop_existing)
        return t

    def execute(self, table, sql, args=()):
        #print "DB.execute(%s, %s, %s)" % (table, sql, args)
        table_no_ns = table.split('.')[-1]
        sql = sql.replace('%t', table)
        sql = sql.replace('%T', table_no_ns)
        c = self.cursor()
        #print "executing: <%s>, %s" % (sql, args)
        #t0 = time.time()
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

class CDTable:

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
            %d
            ,
            primary key (__tv, __tr, __channel, __data_type)
        );

        create index %t_update_tr on %t_update(__tr);
        create index %t_update_data_type on %t_update(__data_type);
    """


    DropTables = """
        drop table %t_tag;
        drop table %t_update;
    """

    StructureColumns = ["__channel", "__tv", "__tr", "__data_type"]
    
    def __init__(self, db, name, columns):
        self.Name = name
        self.Columns = columns      # data columns
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
        columns = [x for x in columns if x not in self.StructureColumns]
        self.Columns = columns
        if not self.validate():
            self.Columns = []
            raise ValueError("Not a conditions DB table (verification failed)")
        
    def columns(self, prefix = None, as_text = False, columns=None):
        columns = columns or self.Columns
        if prefix:
            columns = [f"{prefix}.{c}" for c in columns]
        if as_text:
            return ",".join(columns)
        else:
            return columns

    def all_columns(self, prefix = None, as_text = False):
        return self.columns(prefix=prefix, as_text=as_text, columns=self.AllColumns)

    def execute(self, sql, args=()):
        #print "Table.execute(%s, %s)" % (sql, args)
        return self.DB.execute(self.Name, sql, args)

    def copy_from(self, data, table, columns):
        return self.DB.copy_from(self.Name, data, table, columns)

    def exists(self):
        try:    self.execute("select * from %t_update limit 1")
        except UndefinedTable:
            return False
        else:
            return True
        finally:
            self.execute("rollback")

    @staticmethod
    def create(db, name, column_types, owner, grants = {}, drop_existing=False):
        columns = [c for c,t in column_types]
        t = CDTable(db, name, columns)
        t.createTables(column_types, owner, grants, drop_existing)
        return t

    def tableNames(self):
        return [self.Name + "_" + s for s in ("tag", "update")]

    def dataTableNames(self):
        #return [self.Name + "_" + s for s in ("snapshot_data", "update")]
        return [self.Name + "_" + s for s in ("update",)]

    def validate(self):
        # check if all necessary tables exist and have all the columns
        c = self.DB.cursor()
        for t in self.tableNames():
            try:    
                c.execute("select * from %s limit 1" % (t,))
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
        exists = self.exists()
        c = self.DB.cursor()
        
        if exists and drop_existing:
            try:    self.execute(self.DropTables)
            except UndefinedTable:
                c.execute("rollback")
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
        return struct, data             # (__channel, __tv, __tr, __data_type), (data....)
        
    def shadow_data(self, data_iterator):
        # filter out hidden rows
        # assume rows are sorted by channel, tv, tr desc
        last_tr = 0
        last_channel = None
        for tup in data_iterator:
            (__channel, __tv, __tr, _), _ = self.split_update_tuple(tup)
            if channel != last_channel:
                last_channel = channel
                last_tr = tr
                yield tup
            elif tr >= last_tr:
                last_tr = tr
                yield tup

    def getData(self, tv, tag=None, tr=None, data_type=None, channel_range=None):
        # returns iterator [(channel, tv, data, ...)] unsorted
        #print "getData: tr=%s" % (tr,)

        if data_type is not None:
            # merge with common data
            yield from self.getData(tv, tag, tr, None, channel_range = channel_range)

        data_columns = self.Columns
        all_columns = self.all_columns(prefix="u", as_text=True)

        params = {
            "tv":   tv,
            "tag":  tag,
            "tr":   tr,
            "data_type": data_type,
            "min_channel": channel_range[0] if channel_range else None,
            "max_channel": channel_range[1] if channel_range else None
        }

        if tag is not None:
            c = self.execute(f"""
                select distinct on (u.__channel), {all_columns} from %t_update u, %t_tag t
                    where u.__tv <= %(tv)s
                        and u.__tr < t.__tr
                        and t.__name = %(tag)s
                        and (%(data_type)s is null or u.__data_type = %(data_type)s)
                        and (%(min_channel)s is null or u.__channel >= %(min_channel)s)
                        and (%(max_channel)s is null or u.__channel <= %(max_channel)s)
                    order by u.__channel, u.__tr desc, u.__tv desc
            """, params)
        else:
            c = self.execute(f"""
                select distinct on (u.__channel), {all_columns} from %t_update u
                    where u.__tv <= %(tv)s
                        and (%(tr)s is null or u.__tr < %(tr)s)
                        and (%(data_type)s is null or u.__data_type = %(data_type)s)
                        and (%(min_channel)s is null or u.__channel >= %(min_channel)s)
                        and (%(max_channel)s is null or u.__channel <= %(max_channel)s)
                    order by u.__channel, u.__tr desc, u.__tv desc
            """, params)
    
        yield from cursor_iterator(c)

    def getDataInterval(self, t1, t2, tag=None, tr=None, data_type=None, channel_range=None):

        # initial data
        yield from self.getData(self, t1, tag=tag, tr=tr, data_type=data_type, channel_range=channel_range)
        
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
                select distinct on (u.__channel, u.__tv), {all_columns} from %t_update u, %t_tag t
                    where u.__tv > %(tv1)s
                        and (%(tv2)s is null or u.__tv <= %(tv2)s)
                        and u.__tr < t.__tr
                        and t.__name = %(tag)s
                        and (%(data_type)s is null or u.__data_type = %(data_type)s)
                        and (%(min_channel)s is null or u.__channel >= %(min_channel)s)
                        and (%(max_channel)s is null or u.__channel <= %(max_channel)s)
                    order by u.__channel, u.__tv, u.__tr desc
            """, params)
        else:
            c = self.execute(f"""
                select distinct on (u.__channel, u.__tv), {all_columns} from %t_update u
                    where u.__tv > %(tv1)s
                        and (%(tv2)s is null or u.__tv <= %(tv2)s)
                        and (%(tr)s is null or u.__tr < %(tr)s)
                        and (%(data_type)s is null or u.__data_type = %(data_type)s)
                        and (%(min_channel)s is null or u.__channel >= %(min_channel)s)
                        and (%(max_channel)s is null or u.__channel <= %(max_channel)s)
                    order by u.__channel, u.__tv, u.__tr desc
            """, params)

        yield from self.shadow_data(cursor_iterator(c))

    def addData(self, data, data_type=None, tr=None):
        # data: [(channel, tv, (data, ...)),...]
        csv_rows = []
        data_type = data_type or ""
        if tr is None:  tr = time.time()
        for channel, tv, payload in data:   
            row = ["\\N" if x is None else str(x) for x in (channel, tv, tr, data_type) + tuple(payload)]
            csv_rows.append("\t".join(row))

        csv = io.StringIO('\n'.join(csv_rows))
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
