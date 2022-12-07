import time

class TracePoint:
    def __init__(self, name):
        self.Name = name
        self.T0 = None
        self.reset()
        
    def reset(self):
        self.Count = 0
        self.Time = 0

    def begin(self):
        self.T0 = time.time()
        return self
        
    def end(self):
        self.Count += 1
        self.Time += time.time() - self.T0
        return self
        
    def stats(self):
        avg = None
        if self.Count > 0:  avg = self.Time/self.Count
        return self.Count, self.Time, avg

    def __enter__(self):
        self.begin()

    def __exit__(self, et, ev, tb):
        self.end()

class Tracer:

    def __init__(self):
        self.Points = {}
        
    def __getattr__(self, name):
        if name not in self.Points:
            self.Points[name] = TracePoint(name)
        return self.Points[name]
        
    def __getitem__(self, name):
        if name not in self.Points:
            self.Points[name] = TracePoint(name)
        return self.Points[name]
        
    def begin(self, name):
        return self[name].begin()
        
    def end(self, name):
        return self[name].end()
    
    def stats(self):
        return [(n, p.stats()) for n, p in self.Points.items()]
        
    def printStats(self):
        lst = self.stats()
        lst.sort()
        for name, (count, total, average) in lst:
            print("%-40s: %-6d %f %f" % (name, count, total, average))
        
    def reset(self):
        self.Points = {}        
        
if __name__ == '__main__':
    T = Tracer()
    for i in range(100):
        T.op1.begin()
        time.sleep(0.1)
        T.op1.end()
        
        if i % 3:
            T.op2.begin()
            time.sleep(0.2)
            T.op2.end()
        if (i%10) == 0:
            for n, st in T.stats():
                print('%s: %s %s %s' % ((n,)+st))
