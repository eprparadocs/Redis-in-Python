# -*- coding: utf-8 -*-

# A decorator that will wrap a function in a lock()/unlock() operation.
def synchronized(lock):
    def syncwrap(f):
        def syncwrapfunc(*args, **kw):
            lock.acquire()
            try:
                return f(*args, **kw)
            finally:
                lock.release()
        return syncwrapfunc
    return syncwrap
