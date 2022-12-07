from ConDB import ConDB
from datetime import datetime
from timelib import text2datetime
import sys, getopt

Usage = """
python read_data.py [options] <database name> <table_name> <t1> <t2> <columns,...>
options:
    -h <host>
    -p <port>
    -U <user>
    -w <password>

    -T <tag>    
    -d <data_type>       default = common
"""

data_type = None
tag = None
dbcon = []
t = datetime.now()

opts, args = getopt.getopt(sys.argv[1:], 'h:U:w:p:t:d:')

if len(args) < 3 or args[0] == 'help':
    print(Usage)
    sys.exit(0)

for opt, val in opts:
    if opt == '-h':         dbcon.append("host=%s" % (val,))
    elif opt == '-p':       dbcon.append("port=%s" % (int(val),))
    elif opt == '-U':       dbcon.append("user=%s" % (val,))
    elif opt == '-w':       dbcon.append("password=%s" % (val,))
    elif opt == '-t':       t = text2datetime(val)
    elif opt == '-T':       tag = val
    elif opt == '-d':       data_type = val
    

dbcon.append("dbname=%s" % (args[0],))
t1 = text2datetime(args[2])
t2 = text2datetime(args[3])

dbcon = ' '.join(dbcon)
tname = args[1]
columns = args[4].split(',')
db = ConDB(dbcon)
table = db.table(tname, columns)

data1, iov1 = table.getData(t1, data_type=data_type, tag=tag)
data2, iov2 = table.getData(t2, data_type=data_type, tag=tag)

channels = list(data2.keys())
channels.sort()
for c in channels:
    if data1[c] != data2[c]:
        print((c, iov1[c], data1[c], iov2[c], data2[c]))
        
            
