from threading import RLock
import time

def synchronized(method):
    def smethod(self, *params, **args):
        self._Lock.acquire()
        try:    
            return method(self, *params, **args)
        finally:
            self._Lock.release()
    return smethod


class LRUCache:

    def __init__(self, maxslots, ttl = None, lowwater = None):
        self._Lock = RLock()
        self.Cache = {}
        self.MaxSlots = maxslots
        self.TTL = ttl
        self.LowWater = lowwater

    @synchronized
    def setTTL(self, ttl):
        self.TTL = ttl

    @synchronized        
    def get(self, k):
        if not self.Cache.has_key(k):   
            #print "key %s not found" % (k,)
            return None
        tc, ta, data = self.Cache[k]
        now = time.time()
        if self.TTL != None and tc < now - self.TTL:
            #print "old entry: ", now - tc
            del self.Cache[k]
            return None
        self.Cache[k] = (tc, now, data)
        return data
        
    __getitem__ = get
    
    @synchronized        
    def purge(self):
        nkeep = self.LowWater
        if nkeep == None:   nkeep = self.MaxSlots
        if len(self.Cache) > nkeep:
            lst = self.Cache.items()
            # sort by access time in reverse order, latest first
            lst.sort(lambda x, y: -cmp(x[1][1], y[1][1]))
            while lst and len(lst) > nkeep:
                k, v = lst.pop()
                del self.Cache[k]
        
    @synchronized        
    def put(self, k, data):
        now = time.time()
        self.Cache[k] = (now, now, data)
        self.purge()
        
    __setitem__ = put
    
    @synchronized        
    def remove(self, k):
        try:    del self.Cache[k]
        except KeyError:    pass
        
    __delitem__ = remove
    
    def keys(self):
        return self.Cache.keys()
        
    @synchronized        
    def clear(self):
        self.Cache = {}
    
            
        
