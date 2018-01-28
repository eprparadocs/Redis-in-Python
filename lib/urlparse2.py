# -*- coding: utf-8 -*-

class URL(Exception):
    pass

def urlparse(url):
    """
    Parse a generic URL.
    """
    def swap(a,b,d=False):
        return (a,b) if d == False else (b,a)

    def decode2(a,c,left=True):
        b = a.split(c)
        if len(b) == 2 and len(b[0]) == 0:
            raise URL("Illegal format")
        return swap(b[0],None,left) if len(b) == 1 else (b[0],b[1])

    try:
        scheme,e = decode2(url,"://",left=True)
    except URL:
        schema = None

    up,e = decode2(e,"@")
    if up:
        user,pwd = decode2(up,":")
    else:
        user = pwd = None

    hp,opts = decode2(e,"/", left=False)
    if hp:
        host,port = decode2(hp,":")
    else:
        host = port = None

    return scheme,user,pwd,host,port,opts
