import getopt
import sys
import urllib.request, urllib.error, urllib.parse, hashlib, random, time
from timelib import text2datetime, epoch
from datetime import datetime

Usage = """
python get_csv.py [options] <URL> <table> <column>,...

    -d <data_type>      default = common
    -t <time>           default = now
    -r <record time>    return data as of <tr> default = now
    -T <tag>
    -c <channel>-<channel>      channel range
    -n                  do not retrieve data, just print the URL
"""

opts, args = getopt.getopt(sys.argv[1:], 'd:t:r:nc:T:')
if not args:
    print(Usage)
    sys.exit(1)
data_type = None
t = time.time()
tag = None
tr = None
do_retrieve = True
channel_range = None

for opt, val in opts:
    if opt == '-d':       data_type = val
    if opt == '-t': 
        ttxt = val
        t = epoch(text2datetime(val))
    if opt == '-r': 
        trtxt = val
        tr = epoch(text2datetime(val))
    if opt == '-T': tag = val
    if opt == '-n': do_retrieve = False
    if opt == '-c': channel_range = val

if tag and tr:
    print("Can not specify both tag and record time")
    sys.exit(1)

table = args[1]
url = args[0]
columns = args[2]
args = "table=%s&columns=%s&t=%f" % (table,columns,t)
if tag: args += "&tag=%s" % (tag,)
if data_type:  args += "&type=%s" % (data_type,)
if channel_range:   args += "&cr=%s" % (channel_range,)
if not tag and tr:  args += "&rtime=%f" % (tr,)
url = "%s/get?%s" % (url, args)

if do_retrieve:
    req = urllib.request.Request(url)
    response = urllib.request.urlopen(req)
    print("HTTP status:", response.getcode())
    print(response.read())
else:
    print(url)
