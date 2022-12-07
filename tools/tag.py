from ConDB import ConDB
import sys, getopt

Usage = """
python tag.py [options] <database name> <table_name> <tag>
options:
    -h <host>
    -p <port>
    -U <user>
    -w <password>

    -o  - override existing tag
    -c <comment>
    -t <existing_tag> - copy existing tag
"""

tag = None
dbcon = []
override = False
comment = ""
existing_tag = None

opts, args = getopt.getopt(sys.argv[1:], 'h:U:w:p:oc:t:')

if len(args) < 3 or args[0] == 'help':
    print(Usage)
    sys.exit(0)

for opt, val in opts:
    if opt == '-h':         dbcon.append("host=%s" % (val,))
    elif opt == '-p':       dbcon.append("port=%s" % (int(val),))
    elif opt == '-U':       dbcon.append("user=%s" % (val,))
    elif opt == '-w':       dbcon.append("password=%s" % (val,))
    if opt == '-o':         override = True
    if opt == '-c':         comment = val
    if opt == '-t':         existing_tag = val
    

dbcon.append("dbname=%s" % (args[0],))

dbcon = ' '.join(dbcon)
tname = args[1]
tag = args[2]
db = ConDB(dbcon)
table = db.table(tname, [])
if existing_tag:
    table.copyTag(existing_tag, tag, comment=comment, override=override)
else:
    table.tag(tag, comment=comment, override=override)
