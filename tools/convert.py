from ConDB import ConDB
from datetime import datetime, timedelta
from timelib import text2datetime
import sys, getopt

Usage = """
python read_data.py [options] <database name> <table_name>
options:
    -h <host>
    -p <port>
    -U <user>
    -w <password>
    -n <namespace>
    
    -R actually do the conversion
"""

dbcon = []
dry_run = True
namespace = None

opts, args = getopt.getopt(sys.argv[1:], 'h:U:w:p:Rn:')

if len(args) < 2 or args[0] == 'help':
    print(Usage)
    sys.exit(0)

for opt, val in opts:
    if opt == '-h':         dbcon.append("host=%s" % (val,))
    elif opt == '-p':       dbcon.append("port=%s" % (int(val),))
    elif opt == '-U':       dbcon.append("user=%s" % (val,))
    elif opt == '-w':       dbcon.append("password=%s" % (val,))
    elif opt == '-R':       dry_run = False
    elif opt == '-n':       namespace = val
    

dbcon.append("dbname=%s" % (args[0],))

dbcon = ' '.join(dbcon)
tname = args[1]
db = ConDB(dbcon)
table = db.table(tname, [])
table.readDataColumnsFromDB()
words = tname.split('.', 1)
if len(words) > 1:
    namespace = words[0]
    tname = words[1]

columns = ','.join(table.Columns)
d_columns = ','.join(["d.%s" % (c,) for c in table.Columns])

set_namespace = ""
if namespace:
    set_namespace = "set search_path to %s;" % (namespace,)

sql = """

    %(ns)s
    
    begin;
    
    alter table %(tname)s_update drop constraint %(tname)s_update_pkey;
    
    create index %(tname)s_update_inx on %(tname)s_update(__snapshot_id, __tv, __channel);
    
    insert into %(tname)s_update (__snapshot_id, __tv, __tr, __channel, %(columns)s)
            (select s.__id as __snapshot_id, s.__tv, s.__tr, d.__channel, %(dcolumns)s
                    from %(tname)s_snapshot s, %(tname)s_snapshot_data d
                    where s.__id=d.__snapshot_id
                );
                
    truncate %(tname)s_snapshot_data;
    
    commit
    """ % {"tname":tname, "columns":columns, "dcolumns":d_columns, "ns":set_namespace}
    
print(sql)

if not dry_run:
    db.execute("", sql, ())    
    


"""
    alter table %s_update drop constraint if exists %s_update_pkey;
    
    insert into %s_update (__snapshot_id, __tv, __tr, __channel, %s)
        (select s.__id, s.__tv, s.__tr, d.__channel, %s
            from %s_snapshot s, %s_snapshot_data d
                where s.__id=d.__snapshot_id);
                
    truncate %s_snapshot_data;
"""
