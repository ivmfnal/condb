import hashlib
from py3 import to_str, to_bytes

def signature(password, salt, args, data):
        m = hashlib.md5()
        m.update(to_bytes(password))
        m.update(to_bytes(salt))
        m.update(to_bytes(args))
        m.update(to_bytes(data))
        return m.hexdigest()

