from ConDB import ConDB
import sys, getopt, psycopg2

Usage = """
python create_table.py [options] <database name> <table_name> <column>:<type> [...]
options:
    -h <host>
    -p <port>
    -U <user>
    -w <password>
    
    -c - force create, drop existing table
    -o <table owner>
    -R <user>,... - DB users to grant read permissions to
    -W <user>,... - DB users to grant write permissions to
"""

host = None
port = None
user = None
password = None
columns = []
grants_r = []
grants_w = []
drop_existing = False
owner = None

dbcon = []

opts, args = getopt.getopt(sys.argv[1:], 'h:U:w:p:co:R:W:')

if len(args) < 3 or args[0] == 'help':
    print(Usage)
    sys.exit(0)

for opt, val in opts:
    if opt == '-h':         dbcon.append("host=%s" % (val,))
    elif opt == '-p':       dbcon.append("port=%s" % (int(val),))
    elif opt == '-U':       dbcon.append("user=%s" % (val,))
    elif opt == '-w':       dbcon.append("password=%s" % (val,))
    elif opt == '-c':       drop_existing = True
    elif opt == '-R':       grants_r = val.split(',')
    elif opt == '-W':       grants_w = val.split(',')
    elif opt == '-o':       owner = val
    

dbcon.append("dbname=%s" % (args[0],))

dbcon = ' '.join(dbcon)
tname = args[1]

ctypes = []
for w in args[2:]:
    n,t = tuple(w.split(':',1))
    ctypes.append((n,t))

db = ConDB(dbcon)
t = db.createTable(tname, ctypes, owner, 
    {'r':grants_r, 'w':grants_w}, 
    drop_existing)
print('Table created')

