import getopt, sys, os
from timelib import text2datetime, epoch

Usage = """
gen_csv.py [options] [<file>]
Options are:
    -n <rows>
    -c [<column>,...]
    -t <tv>
"""

rows = 100
columns = ["value"]
outfile = None
t = 1
value = 1

opts, args = getopt.getopt(sys.argv[1:], "n:c:t:v:")
for opt, val in opts:
    if opt == '-t': t = val
    if opt == '-n': rows = int(val)
    if opt == '-c': columns = val.split(",")
    if opt == '-v': value = int(val)
    
    
if args:    outfile = args[0]
if outfile:
    outfile = open(outfile, 'w')
else:
    outfile = sys.stdout
    
t = epoch(text2datetime(t))

outfile.write("channel,tv,%s\n" % (','.join(columns),))

data_format = ",".join(["%s"]*len(columns))
row_format = "%d,%s,"+data_format+"\n"
for row in range(rows):
    channel = row
    data = tuple([value]*len(columns))
    outfile.write(row_format % ((channel, t) + data))
    
        
