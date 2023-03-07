import psycopg2
import sys, random, time, io
from ConDB import ConDB

conn = psycopg2.connect(" ".join(sys.argv[1:]))
db = ConDB(connection=conn)
table = db.createTable("data", 
    list({
    "data1":    "int",
    "data2":    "double precision",
    "data3":    "double precision"}.items())
, drop_existing=True)

t1 = 100.0
NC = 10

t0 = time.time()
time.sleep(0.1)
tr = time.time() - t0

chunk = {}

for _ in range(500):
    tv = int(random.random()*100)
    ch = int(random.random()*NC)
    if random.random() < 0.03 or (ch, tv) in chunk:
        if chunk:
            table.addData(list(chunk.values()), tr=tr)
            tr = time.time() - t0
            chunk = {}
            print(_)  
    data = (ch, tv, tr)
    row = (ch, tv, data)
    chunk[(ch,tv)] = row
    if _ and (_ % 10000) == 0:
        print(_)  

if chunk:
    table.addData(list(chunk.values()), tr=tr)
