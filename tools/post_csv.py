import getopt
import sys
import urllib.request, urllib.error, urllib.parse, hashlib, random, time
from py3 import to_str, to_bytes
from signature import signature
import requests

Usage = """
python post_csv.py [-d <data type>] <file> <table> <password> <URL>
"""

data_type = None
opts, args = getopt.getopt(sys.argv[1:], 'd:')
for opt, val in opts:
    if opt == '-d':       data_type = val

if not args:
    print(Usage)
    sys.exit(1)

fn = args[0]
table = args[1]
pwd = args[2]
url = args[3]
url_args = "table=%s" % (table,)
if data_type:
    url_args += "&type=%s" % (data_type,)
url += "/put?" + url_args

random.seed(time.time())
salt = '%s' % (random.random(),)
data = open(fn, 'rb').read()
sig = signature(pwd, salt, url_args, data)
headers = {   
        'X-Salt':       salt,
        'X-Signature':  sig
        #'Expect':       "100-continue"
    }

try:    response = requests.post(url, data=data, headers=headers)
except Exception as e:
    print(e)
else:
    print(response.status_code)
    print(response.text)
