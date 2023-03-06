import psycopg2
import sys, random, time, io

conn = psycopg2.connect(" ".join(sys.argv[1:]))
c = conn.cursor()

t1 = 100.0
NC = 100

t0 = time.time()

rows_csv = []
c.execute("select max(tr) from updates")
tr = (c.fetchone()[0] or 0) + 1
    
for _ in range(500000):
    ch = int(random.random()*NC)
    tv = random.random()
    data = (ch, tv, tr)
    row = (ch, tv, tr) + data
    row_csv = "\t".join([str(x) for x in row])
    rows_csv.append(row_csv)
    if random.random() < 0.01:
        tr += 1

for row in chunk.values():
    row_csv = "\t".join([str(x) for x in row])
    rows_csv.append(row_csv)

csv = io.StringIO('\n'.join(rows_csv))
c.copy_from(csv, "updates", 
    columns=['channel', 'tv', "tr", 'data1', 'data2', 'data3'])
c.execute("commit")

c.execute("commit")
