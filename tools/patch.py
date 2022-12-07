from ConDB import ConDB
import sys, getopt, psycopg2
from timelib import text2datetime

Usage = """
python patch.py [options] -t <t end> <database name> <table_name> <column>,...
options:
    -h <host>
    -p <port>
    -U <user>
    -w <password>

    -c <csv_file>
    -d <data_type>       default = common
"""

def parseData(input):
    tolerances = None
    data = []
    for l in input.readlines():
        l = l.strip()
        if not l:   continue
        words = l.split(',')
        if words[0] == 'tolerance':
            tolerances = [float(x) for x in words[2:]]
        elif words[0] == 'channel':
            continue
        else:
            cid = int(words[0])
            tv = text2datetime(float(words[1]))
            tup = []
            for x in words[2:]:
                try:    x = int(x)
                except:
                    try:    x = float(x)
                    except:
                        pass
                tup.append(x)
            tup = tuple(tup)
            data.append((cid, tv, tup))
    return data, tolerances

host = None
port = None
user = None
password = None
columns = []
input = sys.stdin
data_type = None
tend = None

dbcon = []

opts, args = getopt.getopt(sys.argv[1:], 'h:U:w:p:c:d:t:T:')

if len(args) < 3 or args[0] == 'help':
    print(Usage)
    sys.exit(0)

for opt, val in opts:
    if opt == '-h':         dbcon.append("host=%s" % (val,))
    elif opt == '-p':       dbcon.append("port=%s" % (int(val),))
    elif opt == '-U':       dbcon.append("user=%s" % (val,))
    elif opt == '-w':       dbcon.append("password=%s" % (val,))
    elif opt == '-c':       input = open(val,'r')
    elif opt == '-t':       tend = text2datetime(val)
    elif opt == '-d':       data_type = val
    

dbcon.append("dbname=%s" % (args[0],))

dbcon = ' '.join(dbcon)
tname = args[1]
columns = args[2].split(',')

db = ConDB(dbcon)
t = db.table(tname, columns)

data, tolerances = parseData(input)
print("input parsed: %s rows, tolerances: %s" % (len(data), tolerances))
t.patch(data, tend, data_type)
