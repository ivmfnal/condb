from ConDB import ConDB, CDTable
import sys, getopt

Usage = """
python create_table.py [options] <database name> <table_name> <column>:<type> [...]
options:
    -h <host>
    -p <port>
    -U <user>
    -w <password>
    
    -c - force create, drop existing table
    -s - just print SQL needed to create the table without actually creating anything
    -o <table owner>
    -R <user>,... - DB users to grant read permissions to
    -W <user>,... - DB users to grant write permissions to
"""

opts, args = getopt.getopt(sys.argv[1:], 'h:U:w:p:co:R:W:s')

if len(args) < 3 or args[0] == 'help':
    print(Usage)
    sys.exit(0)


opts = dict(opts)
dbcon = []
if "-h" in opts:        dbcon.append("host=%s" % (opts["-h"],))
if "-p" in opts:        dbcon.append("port=%s" % (int(opts["-p"]),))
if "-U" in opts:        dbcon.append("user=%s" % (opts["-U"],))
if "-w" in opts:        dbcon.append("password=%s" % (opts["-w"],))
drop_existing = "-c" in opts
grants_r = []
grants_w = []
if "-R" in opts:    grants_r = opts["-R"].split(',')
if "-W" in opts:    grants_w = opts["-W"].split(',')
owner = opts.get("-o")
sql_only = "-s" in opts

dbcon.append("dbname=%s" % (args[0],))

dbcon = ' '.join(dbcon)

tname = args[1]

ctypes = []
for w in args[2:]:
    n,t = tuple(w.split(':',1))
    ctypes.append((n,t))

if sql_only:
    sql = CDTable.createSQL(tname, owner, ctypes, grants_r, grants_w)
    print(sql)
else:
    db = ConDB(dbcon)
    t = db.createTable(tname, ctypes, owner, 
        {'r':grants_r, 'w':grants_w}, 
        drop_existing)
    print('Table created')

