# -*- coding: iso-8859-1 -*-

"""
    Copyright (c) 2009 Charles E. R. Wegrzyn
    All Right Reserved.

    chuck.wegrzyn at gmail.com

    This application is free software and subject to the Version 1.0
    of the Common Public Attribution License. 

"""

import socket

from TSexcept import ConnectionError, InvalidResponse, ResponseError

# Class to handle restricted communication with a slave server.
#
# This class contains only the functions necessary to communicate 
# with a slave. This includes __init__(), ping(), write(), get_response(),
# connect() and disconnect().
#
class SlaveComm:
    def __init__(self, host=None, port=None, timeout=None):
        self.host = host or 'localhost'
        self.port = port or 6400
        if timeout:
            socket.setdefaulttimeout(timeout)
        self._sock = None
        self._fp = None

    def _write(self, s):
        try:
            self._sock.sendall(s)
        except socket.error, e:
            if e.args[0] == 32:
                # broken pipe
                self.disconnect()
            raise ConnectionError("Error %s while writing to socket. %s." % tuple(e.args))

    def _read(self):
        try:
            return self._fp.readline()
        except socket.error, e:
            if e.args and e.args[0] == errno.EAGAIN:
                return
            self.disconnect()
            raise ConnectionError("Error %s while reading from socket. %s." % tuple(e.args))
        if not data:
            self.disconnect()
            raise ConnectionError("Socket connection closed when reading.")
        return data

    def ping(self):
        self.connect()
        self._write('PING\r\n')
        return self.get_response()

    def replaceDB(self,version,pickle):
        self.connect()
        self._write("REPLACEDB %d %d\r\n" % (version,len(pickle)) + pickle)
        return self.get_response()

    def get_response(self):
        data = self._read().strip()
        c = data[0]
        if c == '-':
            raise ResponseError(data[5:] if data[:5] == '-ERR ' else data[1:])
        if c == '+':
            return data[1:]
        if c == '*':
            try:
                num = int(data[1:])
            except (TypeError, ValueError):
                raise InvalidResponse("Cannot convert multi-response header '%s' to integer" % data)
            result = list()
            for i in range(num):
                result.append(self._get_value())
            return result
        return self._get_value(data)

    def _get_value(self, data=None):
        data = data or self._read().strip()
        if data == '$-1':
            return None
        try:
            c, i = data[0], (int(data[1:]) if data.find('.') == -1 else float(data[1:]))
        except ValueError:
            raise InvalidResponse("Cannot convert data '%s' to integer" % data)
        if c == ':':
            return i
        if c != '$':
            raise InvalidResponse("Unkown response prefix for '%s'" % data)
        buf = []
        while True:
            data = self._read()
            i -= len(data)
            buf.append(data)
            if i < 0:
                break
        return ''.join(buf)[:-2]

    def disconnect(self):
        if isinstance(self._sock, socket.socket):
            try:
                self._sock.close()
            except socket.error:
                pass
        self._sock = None
        self._fp = None

    def connect(self):
        if isinstance(self._sock, socket.socket):
            return
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
        except socket.error, e:
            raise ConnectionError("Error %s connecting to %s:%s. %s." % (e.args[0], self.host, self.port, e.args[1]))
        else:
            self._sock = sock
            self._fp = self._sock.makefile('r')
