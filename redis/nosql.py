# -*- coding: iso-8859-1 -*-

"""
    Copyright (c) 2009 Charles E. R. Wegrzyn
    All Right Reserved.

    chuck.wegrzyn at gmail.com

    This application is free software and subject to the Version 1.0
    of the Common Public Attribution License. 

"""

__author__ = "Charles E. R. Wegrzyn"
__copyright__ = "Copyright 2009, C Wegrzyn"
__license__ = "CPAL"
__version__ = "0.3"
__revision__ = "$LastChangedRevision: 01 $"[22:-2]
__date__ = "$LastChangedDate: 2009-07-31 16:16:00 +0500 (July 31 2009) $"[18:-2]

import socket
import threading

# Set defaults for certain things...these can be overriden before making
# the initial call to nosql.
#
#   DEF_HOST - where the server runs, if not specified
#   DEF_PORT - default port for the server

DEF_PORT = 6379
DEF_HOST = "localhost"

# Various exceptions thrown by this code
#
#   NOSQLERROR - parent of all nosql exceptions
#   CONNECTERROR - error establishing a connection or maintaining it
#   RESPONSEERROR - invalid response returned
#
class NOSQLERROR(Exception):
    """
        The parent of all exceptions that can be raised by the nosql
        library.
    """
    pass
class CONNECTERROR(NOSQLERROR):
    """
        The library encountered some issue in communicating with the
        TSnosql server.
    """
    pass
class RESPONSEERROR(NOSQLERROR):
    """
        There was an invalid formatted response from the server.
    """
    pass

class nosql(object):
    """
        This class is the interface to the TSnosql server. The protocol is
        pretty simple, and liberally lifted from other projects including
        memcached and redis.

        Unlike memcache which only supports string variables, TSnosql
        supports string, list and set variables. These variable types
        have the meaning you normally associate with strings, lists
        and variables.

        In addition to the variables, TSnosql has a few other features.
        First, is the ability of the server to back up the contents of
        database to disk and recover them on restart. Secondly, this
        initial release supports subordinate servers, all interlinked. This
        means an update to the main will ripple through to all the
        subordinates. Finally, the "database" isn't just one big database: it
        is composed of many individual, and separately addressible
        databases. So you have the ability to store variables in
        any particular sub-database.

        TSnosql is a Python service written using the Asyncore service.
        As such it can run on any system that support Python 2.5 or
        better (it hasn't been tested on Python 3.0 yet).

        Future releases of TSnosql will include support for multi-
        main mode as well as some extensions to the command set.

        There are four types of commands in the system:

            1. String related variable commands
                - get the contents of one or more string variables
                - set the contents of a variable
                - increment or decrement a variable
                - get a variable value and set it to a new value

            2. List related variable commands
                - push a new value onto a list variable
                - pop off a value from a list
                - return the length of a list variable
                - reduce the elements of a list
                - return a group of elements from a list
                - return a particular indexed element of a list
                - set a indexed element to some value
                - remove a particular indexed element

            3. Set related variable commands
                - add an element to a set
                - remove an element from a set
                - return the size of a set
                - see if an element is a member of a set
                - take the intersection, union or difference of a number of sets
                - see if a member is in a set
                - pop an element from a set
                - move a set variable

            4. General commands
                - connect to the server
                - disconnect from a server
                - ping a server to see if it is alive
                - connect a server (main or subordinate) to another subordinate
                - return the version of the server
                - return information about a server
                - does a variable exist?
                - delete one or more variables from a server
                - get the type of a variable
                - search for variables using regular expressions
                - get a random key from the server
                - sort variables by content
                - set a timeout on a variable
                - return the number of keys in a database
                - clear out a database or an entire server
                - save the server contents
                - move a variable
                - rename a variable
                - return the time the last save occurred

    """

    def __init__(self, host=None, port=None, server = None, timeout=None):
        """
            Initialize a nosql object, which is used to communicate with the TSnosql
            server. You can use (host, port) or server (in form host:port) to address
            the server.

            @type host: string
            @param host: the FQDN or IP address of the TSnosql server.

            @type port: integer
            @param port: the port being used by the TSnosql server.

            @type server: string
            @param server: combined host@port address (used in place of host,port)

            @type timeout: integer
            @param timeout: a timeoout value for all socket level communication

        """
        # Do the address work...if we have server, we will use it.
        if server:
            hp = server.split(":")
            if len(hp) == 1:
                host = hp[0]
            else:
                host = hp[0] if len(hp[0]) else None
                port = int(hp[1]) if len(hp[1]) else None
        self.host = host or DEF_HOST
        self.port = port or DEF_PORT
        if timeout:
            socket.setdefaulttimeout(timeout)
        self._sock = None
        self._fp = None
        self.lock = threading.Lock()

    def _write(self, s):
        try:
            self._sock.sendall(s)
        except socket.error, e:
            if e.args[0] == 32:
                self.disconnect()
            raise CONNECTERROR("Error %s while writing to socket. %s." % tuple(e.args))

    def _read(self):
        try:
            return self._fp.readline()
        except socket.error, e:
            if e.args and e.args[0] == errno.EAGAIN:
                return
            self.disconnect()
            raise CONNECTERROR("Error %s while reading from socket. %s." % tuple(e.args))
        if not data:
            self.disconnect()
            raise CONNECTERROR("Socket connection closed when reading.")
        return data

    def _get_value(self, data=None):
        data = data or self._read().strip()
        if data == '$-1':
            return None
        try:
            c, i = data[0], (int(data[1:]) if data.find('.') == -1 else float(data[1:]))
        except ValueError:
            raise INVALIDRESPONSE("Expected integer got something else '%s'." % data)
        if c == ':':
            return i
        if c != '$':
            raise INVALIDRESPONSE("Unkown prefix (not $-+:*): '%s'" % data)
        buf = []
        while True:
            data = self._read()
            i -= len(data)
            buf.append(data)
            if i < 0:
                break
        return ''.join(buf)[:-2]

    def disconnect(self):
        """
            Disconnect from the TSnosql server.
        """
        if isinstance(self._sock, socket.socket):
            try:
                self._sock.close()
            except socket.error:
                pass
        self._sock = None
        self._fp = None

    def connect(self):
        """
            Connect to the TSnosql server identified in the L(__init__) function.
        """
        if isinstance(self._sock, socket.socket):
            return
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
        except socket.error, e:
            raise CONNECTERROR("Error %s connecting to %s:%s. %s." % (e.args[0], self.host, self.port, e.args[1]))
        else:
            self._sock = sock
            self._fp = self._sock.makefile('r')

    def get_response(self):
        data = self._read().strip()
        c = data[0]
        if c == '-':
            raise RESPONSEERROR(data[5:] if data[:5] == '-ERR ' else data[1:])
        if c == '+':
            return data[1:]
        if c == '*':
            try:
                num = int(data[1:])
            except (TypeError, ValueError):
                raise INVALIDRESPONSEERROR("Cannot convert multi-response header '%s' to integer" % data)
            result = list()
            for i in range(num):
                result.append(self._get_value())
            return result
        return self._get_value(data)

    def ping(self):
        """
            Send a ping to command to the server. If the server is functioning, expect a PONG.\r\n
            Protocol::

                    Client to server:   PING\\r\\n

                    Server to client:   +PONG\\r\\n

            @rtype: string
            @return: If the server is running, PONG should be returned
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('PING\r\n')
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def subordinate(self,host,port):
        """
            Connect a subordinate server to this server.\r\n
            Protocol::

                    Client to server:   SLAVE <host> <port>\\r\\n

                    Server to client:   +OK\\r\\n

                                        -ERR <error message>\\r\\n

            @type host: string
            @param host: FQDN or IP address of subordinate server

            @type port: integer
            @param port: port nunmber of subordinate server

            @rtype: string
            @return: OK if it worked
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write("SLAVE %s:%d\r\n" % (host,port))
            rc = self.get_response()
        finally:
            self.lock.release()

    def set(self, name, value, preserve=False):
        """
            Set a string variable to some value (SET form). Conditionally set the string to
            some value if it doesn't already exist (SETNX form).\r\n
            Protocol::

                    Client to server:   SET <name> <length-of-value>\\r\\n<value>\\r\\n

                                        SETNX <name> <length-of-value>\\r\\n<value>\\r\\n

                    Server to client:   +OK\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of string variable

            @type value: string
            @param value: value assigned to the named variable

            @type preserve: boolean
            @param preserve: if False, then do a SET. Otherwise do a SETNX

            @rtype: string
            @return: OK
        """
        self.connect()
        try:
            self.lock.acquire()
            value = value if isinstance(value, basestring) else str(value)
            self._write('%s %s %s\r\n%s\r\n' % ('SETNX' if preserve else 'SET', name, len(value), value))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def get(self, name):
        """
            Get a string value called name from the server. If the named variable doesn't
            exist return None.\r\n
            Protocol::

                    Client to server:   GET <name>\\r\\n

                    Server to client:   $<length of value>\\r\\n<value>\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of variable to return

            @rtype: string
            @return: value of variable or None.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('GET %s\r\n' % name)
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def getset(self,name,val):
        """
            Return the value of the named variable, or None if it doesn't exist, and set the
            string variable to val.\r\n
            Protocol::

                    Client to server:   GETSET <name> <length of value>\\r\\n<value>\\r\\n

                    Server to client:   $<length of orginal value>\\r\\n<original value>\\r\\n

                                        $-1\\r\\n

                                        -ERR <error message>\\r\\n
            @type name: string
            @param name: name of variable to manipulate

            @type val: string
            @param val: value to assign to variable

            @rtype: string
            @return: Initial value of variable.
        """
        self.connect()
        try:
            self.lock.acquire()
            val = val if isinstance(val, basestring) else str(val)
            self._write('GETSET %s %d\r\n%s\r\n' % (name,len(val),val))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def mget(self, *args):
        """
            Get the values assigned to multiple variables.\r\n
            Protocol::

                     Client to server:  MGET <name1> <name2> ... <nameN>\\r\\n

                     Server to client:  *<No.entries>\\r\\n<entry1>\\r\\n<entry2>\\r\\n ...\\r\\n<entryN>\\r\\n

                                        where <entryN> is either

                                                $<len of value>\\r\\n<value>

                                        or

                                                $-1

                                        The former being used when there is a value and the latter when the
                                        variable doesn't exist.

            @type args: string
            @param args: one or more variable names, separated by commas.

            @rtype: list of strings
            @return: list of zero or more variables.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('MGET %s\r\n' % ' '.join(args))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def version(self):
        """
            Return the version of the server.\r\n
            Protocol::

                    Client to server:   VERSION\\r\\n

                    Server to client:   :<version-as-integer>\\r\\n

            @rtype: integer
            @return: version of server.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write("VERSION\r\n")
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def incr(self, name, amount=1):
        """
            Increment a string variable, which must be an integer in string format, by some value (default is one).
            Return the new value. If the variable doesn't initially exist it is assumed to be 0.\r\n
            Protocol::

                    Client to server:   INCR <name>\\r\\n

                                        INCRBY <name> <amount>\\r\\n

                    Server to client:   :<value>\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of variable to increment

            @type amount: integer
            @param amount: amount to increment

            @rtype: integer
            @return: new, incremented value.
        """
        self.connect()
        try:
            self.lock.acquire()
            if amount == 1:
                self._write('INCR %s\r\n' % name)
            else:
                self._write('INCRBY %s %s\r\n' % (name, amount))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def decr(self, name, amount=1):
        """
            Decrement a string variable, which must be an integer in string format, by some value (default is one).
            Return the new value. If the variable doesn't initially exist it is assumed to be 0.\r\n
            Protocol::

                    Client to server:   DECR <name>\\r\\n

                                        DECRBY <name> <amount>\\r\\n

                    Server to client:   :<value>\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of variable to decrement

            @type amount: integer
            @param amount: amount to decrement

            @rtype: integer
            @return: new, decremented value.
        """
        self.connect()
        try:
            self.lock.acquire()
            if amount == 1:
                self._write('DECR %s\r\n' % name)
            else:
                self._write('DECRBY %s %s\r\n' % (name, amount))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def exists(self, name):
        """
            See if a variable, string, list or set, exists.\r\n
            Protocol::

                    Client to server:   EXISTS <name>\\r\\n

                    Server to client:   :0\\r\\n

                                        :1\\r\\n

                                        -ERR <error message>\\r\\n

            @rtype: integer
            @return: 0, if the variable doesn't exist.  1 otherwise.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('EXISTS %s\r\n' % name)
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def delete(self, *names):
        """
            Delete one or more variables, string, list or set, from the database.\r\n
            Protocol::

                    Client to server:   DEL <name1> <name2> ... <nameN>\\r\\n

                    Server to client:   :<No. of variables deleted>\\r\\n

                                        -ERR <error message>\\r\\n

            @type names: string
            @param names: one or more variable names, comma separated.

            @rtype: integer
            @return: number of variables actually deleted.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('DEL %s\r\n' % ' '.join(names))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def type(self, name):
        """
            Return the type of a given variable,If the variable exists it will return string, list or set.
            If the variable doesn't exist it will return none.\r\n
            Protocol::

                    Client to server:   TYPE <name>\\r\\n

                    Server to client:   +string\\r\\n

                                        +list\\r\\n

                                        +set\\r\\n

                                        +none\\r\\r

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of variable to examine.

            @rtype: string
            @return: string, list or set depending on the type of variable.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('TYPE %s\r\n' % name)
            res = self.get_response()
        finally:
            self.lock.release()
        return None if res == 'none' else res

    def keys(self, pattern):
        """
            Return the names of the variables that match the simple pattern, pattern.

            @type pattern: string
            @param pattern: A string that is matched against all the variables in the selected
            database. The pattern is the Unix-shell wildcards, A pattern can be made up of any
            combination of:

                1. *        matches everything
                2. ?        matches any single character
                3. [seq]    matches any character in seq
                4. [!seq]   matches any character not in seq.\r\n

            Protocol::
                    Client to server:   KEYS <pattern>\\r\\n

                    Server to client:   $<length of value>\\r\\n<value>\\r\\n

                                        where <value> is a string with the format of
                                        <key1> <key2> ... <keyN>

                                        -ERR <error message>\\r\\n

            @rtype: list of strings
            @return: zero or more strings, each of which is the name of a variable that matches
            the pattern.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('KEYS %s\r\n' % pattern)
            rc = self.get_response().split()
        finally:
            self.lock.release()
        return rc

    def re(self, pattern):
        """
            Return the names of the variables that match the regular expression pattern, pattern.\r\n
            Protocol::

                    Client to server:   RE <pattern>\\r\\n

                    Server to client:   $<length of value>\\r\\n<value>\\r\\n

                                        where <value> is a string with the format of
                                        <key1> <key2> ... <keyN>

                                        -ERR <error message>\\r\\n

            @type pattern: string
            @param pattern: A string that is matched against all the variables in the selected
            database. The pattern is a regular expression (see documentation on regular expressions
            to understand the acceptable patterns.)

            @rtype: list of strings
            @return: zero or more strings, each of which is the name of a variable that matches
            the pattern.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('RE %s\r\n' % pattern)
            rc = self.get_response().split()
        finally:
            self.lock.release()
        return rc

    def randomkey(self):
        """
            Pick out a random variable and return it.\r\n
            Protocol::

                    Client to server:   RANDOMKEY\\r\\n

                    Server to client:   $<length of variable name>\\r\\n<variable name>\\r\\n

                                        -ERR <error message>\\r\\n

            @rtype: string
            @return: Name of random key in database
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('RANDOMKEY\r\n')
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def rename(self, src, dst, preserve=False):
        """
            Rename a variable to a new name.The initial and final names can't be the same,
            and the initial variable must exist. When the operation is completed the initial
            variable will no longer exist. Any timer expiration on the initial variable
            will not be carried over to the final variable.

            Protocol::

                    Client to server:   RENAME <src-name> <dst-name>\\r\\n

                    Server to client:   +OK

                                        -ERR <error message>\\r\\n

                    Client to server:   RENAMEX <src-name> <dst-name>\\r\\n

                    Server to client:   :0\\r\\n

                                        :1\\r\\n

                                        -ERR <error message>\\r\\n
            @type src: string
            @param src: initial name of variable

            @type dst: string
            @param dst: final name of variable

            @type preserve: boolean
            @param preserve: If False, a plain rename (RENAME). If True, the src variable
            must exist.

            For preserve = False, the return type is a string and will be OK, if it succeeded.
            For preserve = True, the return type is an integer and will be 1 if the rename succeeded.

        """
        self.connect()
        try:
            self.lock.acquire()
            if preserve:
                self._write('RENAMENX %s %s\r\n' % (src, dst))
            else:
                self._write('RENAME %s %s\r\n' % (src, dst))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def expire(self, name, time):
        """
            Set a time to expire value on a variable. After 'time' seconds the variable will
            be deleted. If the database is copied to a file and restarted later, if the time
            would have expired, the variable will be deleted.\r\n
            Protocol::

                    Client to server:   EXPIRE <name> <time>\\r\\n

                    Server to client:   :<time>\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of variable

            @type time: integer
            @param time: time, in seconds, for the variable to live

            @rtype: integer
            @return: time, in seconds.

        """
        self.connect()
        try:
            time = value if isinstance(time, basestring) else str(time)
            self.lock.acquire()
            self._write('EXPIRE %s %s\r\n' % (name, time))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def dbsize(self):
        """
            Return the number of variables in the selected database.\r\n
            Protocol::

                    Client to server:   DBSIZE\\r\\n

                    Server to client:   :<value>\\r\\n

                                        -ERR <error message>\\r\\n

            @rtype: integer
            @return: number of variables in database
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write("DBSIZE\r\n")
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def flush(self, all=False):
        """
            Clear a particular database or all of them.\r\n
            Protocol::

                    Client to server:   RENAME <src-name> <dst-name>\\r\\n

                    Server to client:   +OK

                                        -ERR <error message>\\r\\n

                    Client to server:   FLUSHDB\\r\\n

                                        FLUSHALL\\r\\n

                    Server to client:   +OK\\r\\n

            @type all: boolean
            @param all: If False, clear a particular database or all of them.

            @rtype: string
            @return: OK
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('%s\r\n' % ('FLUSHALL' if all else 'FLUSHDB'))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def push(self, name, value, tail=False):
        """
            Create or add to a list variable. If the list variable already
            exists, if tail = True the value is pushed on the right. Otherwise
            the value is pushed onto the head.\r\n
            Protocol::

                    Client to server:   RPUSH <name> <value>\\r\\n

                    Server to client:   +OK

                                        -ERR <error message>\\r\\n

                    Client to server:   LPUSH <name> <value>\\r\\n

                    Server to client:   +OK\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of the variable

            @type value: string
            @param value: new value to be added to list

            @type tail: boolean
            @param tail: If False, value is pushed on tail end. If True on head end.

            @rtype: string
            @return: OK if is succeeded.
        """
        self.connect()
        try:
            self.lock.acquire()
            value = value if isinstance(value, basestring) else str(value)
            self._write('%s %s %s\r\n%s\r\n' % (
                'LPUSH' if tail else 'RPUSH', name, len(value), value))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def llen(self, name):
        """
            Return the current number of elements in a list variable.\r\n
            Protocol::

                    Client to server:   LLEN <name>\\r\\n

                    Server to client:   :<number>\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of variable

            @rtype: integer
            @return: number of elements in list
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('LLEN %s\r\n' % name)
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def lrange(self, name, start, end):
        """
            Return the elements of the list variable called name from the start indexed item
            to the end item. The list is 0-indexed. If the indexes are out of range, the list
            returned may be empty.\r\n
            Protocol::

                    Client to server:   LRANGE <name> <start> <end>\\r\\n

                    Server to client:   *<No.entries>\\r\\n<entry1>\\r\\n<entry2>\\r\\n ...\\r\\n<entryN>\\r\\n

                                                where <entryN> is $<len of value>\\r\\n<value>

                                        $-1\\r\\n

            @type name: string
            @param name: name of the list variable

            @type start: integer or string
            @param start: the beginning index, with 0 being the beginning of the list

            @type end: integer or string
            @param end: the ending index of the list to return.

            @rtype: list
            @return: None, or the list of elements extracted from the list variable.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('LRANGE %s %s %s\r\n' % (name, start, end))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def ltrim(self, name, start, end):
        """
            Trim the list variable to include only elements starting from index start
            to index end. The list is assumed 0-indexed.\r\n
            Protocol::

                    Client to server:   LTRIM <name> <start> <end>\\r\\n

                    Server to client:   +OK\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of list variable

            @type start: integer or string
            @param start: the beginning index

            @type end: integer or string
            @param end: the ending index

            @rtype: string
            @return: OK if successful.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('LTRIM %s %s %s\r\n' % (name, start, end))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def lindex(self, name, index):
        """
            Return the index-th item in a list variable. If the indexed item
            doesn't exist, 'None' is returned.\r\n
            Protocol::

                    Client to server:   LINDEX <name> <index>\\r\\n

                    Server to client:   $<len of value>\\r\\n<value>

                                        $-1\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of list variable to examine.

            @type index: integer or string
            @param index: which item in the lsit to return

            @rtype: string
            @return: None, if the indexed item doesn't exist in the lsit, or the indexed
            item in the list.
        """
        self.connect()
        try:
            index = index if isinstance(index, basestring) else str(index)
            self.lock.acquire()
            self._write('LINDEX %s %s\r\n' % (name, index))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def pop(self, name, tail=False):
        """
            Pop off the first element, if tail = False, or the last element, if tail = True, from
            the list variable.\r\n
            Protocol::

                    Client to server:   RPOP <name>\\r\\n

                                        LPOP <name>\\r\\n

                    Server to client:   $<len of value>\\r\\n<value>

                                        $-1\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of list variable

            @type tail: boolean
            @param tail: if False, pop the element off the head. Otherwise it comes from the tail.

            @rtype: string
            @return: None or the popped element.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('%s %s\r\n' % ('RPOP' if tail else 'LPOP', name))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def lset(self, name, index, value):
        """
            Set the indexed item in the list variable to value. The list must be at least
            index items long.\r\n
            Protocol::

                    Client to server:   LSET <name> <index> <length-of-value>\\r\\n<value>\\r\\n

                    Server to client:   +OK\\r\\n

                                        -ERR <error message>\\r\\n

                @type name: string
                @param name: List variable to be manipulated

                @type index: integer or string
                @param index: which item in the list to manipulate

                @type value: string or integer
                @param value: value to assign to the indexed item in the list.

                @rtype: string
                @return: OK if it worked.
        """
        self.connect()
        try:
            self.lock.acquire()
            value = value if isinstance(value, basestring) else str(value)
            self._write('LSET %s %s %s\r\n%s\r\n' % (name, index, len(value), value))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def lrem(self, name, value, num=0):
        """
            Remove the first 'num' occurrences of 'value' from the variable 'name'.\r\n
            Protocol::

                    Client to server:   LREM <name> <num> <length-of-value>\\r\\n<value>\\r\\n

                    Server to client:   +OK\\r\\n

                                        -ERR <error message>\\r\\n

                @type name: string
                @param name: List variable to be manipulated

                @type index: integer or string
                @param index: which item in the list to manipulate

                @type value: string or integer
                @param value: value to assign to the indexed item in the list.

                @rtype: string
                @return: OK if it worked.
        """
        self.connect()
        try:
            self.lock.acquire()
            value = value if isinstance(value, basestring) else str(value)
            self._write('LREM %s %s %s\r\n%s\r\n' % (name, num, len(value), value))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def sort(self, name, by=None, get=None, start=None, num=None, desc=False, alpha=False):
        stmt = ['SORT', name]
        if by:
            stmt.append("BY %s" % by)
        if start and num:
            stmt.append("LIMIT %s %s" % (start, num))
        if get is None:
            pass
        elif isinstance(get, basestring):
            stmt.append("GET %s" % get)
        elif isinstance(get, list) or isinstance(get, tuple):
            for g in get:
                stmt.append("GET %s" % g)
        else:
            raise NOSQLERROR("Invalid parameter 'get' for Redis sort")
        if desc:
            stmt.append("DESC")
        if alpha:
            stmt.append("ALPHA")
        self.connect()
        try:
            self.lock.acquire()
            self._write(' '.join(stmt + ["\r\n"]))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def sadd(self, name, value):
        """
            Add the value to the set variable 'name'.\r\n
            Protocol::

                    Client to server:   SADD <name> <length-of-value>\\r\\n<value>\\r\\n

                    Server to client:   :0\\r\\n

                                        :1\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of set variable

            @type value: string
            @param value: value to put in set.

            @rtype: integer
            @return: 0 if the element was already in the set, 1 if it was added.
        """
        self.connect()
        try:
            self.lock.acquire()
            value = value if isinstance(value, basestring) else str(value)
            self._write('SADD %s %s\r\n%s\r\n' % (name, len(value), value))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def scard(self,name):
        """
            Return the number of elements in the set, called its cardinality.\r\n
            Protocol::

                    Client to server:   SCARD <name>\\r\\n

                    Server to client:   :<nunber>\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of the set variable

            @rtype: integer
            @return: number of elements in the set.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('SCARD %s\r\n'  % (name))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def srem(self, name, value):
        """
            Remove a specific member from a set.\r\n
            Protocol::

                    Client to server:   SREM <name> <length-of-value>\\r\\n<value>\\r\\n

                    Server to client:   :0\\r\\n

                                        :1\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of set variable

            @type value: string
            @param value: member to remove from set

            @rtype: integer
            @return: 0 if the value was in the set. 1 otherwise
        """
        self.connect()
        try:
            self.lock.acquire()
            value = value if isinstance(value, basestring) else str(value)
            self._write('SREM %s %s\r\n%s\r\n' % (name, len(value), value))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def sismember(self, name, value):
        """
            Determine if a value is present in a set.\r\n
            Protocol::

                    Client to server:   SISMEMBER <name> <length-of-value>\\r\\n<value>\\r\\n

                    Server to client:   :0\\r\\n

                                        :1\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of set variable to examine

            @type value: string
            @param value: what to check set for

            @rtype: integer
            @return: 0, if the value isn't in the set. 1 if it is.
        """
        self.connect()
        try:
            self.lock.acquire()
            value = value if isinstance(value, basestring) else str(value)
            self._write('SISMEMBER %s %s\r\n%s\r\n' % (name, len(value), value))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def sinter(self, *args):
        """
            Produce and return the intersection of all the set variables.\r\n
            Protocol::

                    Client to server:   SINTER <name1> <name2> ... <nameN>\\r\\n

                    Server to client:   *<No.entries>\\r\\n<entry1>\\r\\n<entry2>\\r\\n ...\\r\\n<entryN>\\r\\n

                                                where <entryN> is $<len of value>\\r\\n<value>

                                        $-1\\r\\n

            @type args: string
            @param args: one or more names of set variables

            @rtype: set
            @return: intersection of *args sets.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('SINTER %s\r\n' % ' '.join(args))
            rc = set(self.get_response())
        finally:
            self.lock.release()
        return rc

    def sunion(self, *args):
        """
            Produce and return the union of all the set variables.\r\n
            Protocol::

                    Client to server:   SUNION <name1> <name2> ... <nameN>\\r\\n

                    Server to client:   *<No.entries>\\r\\n<entry1>\\r\\n<entry2>\\r\\n ...\\r\\n<entryN>\\r\\n

                                                where <entryN> is $<len of value>\\r\\n<value>

                                        $-1\\r\\n

            @type args: string
            @param args: one or more names of set variables

            @rtype: set
            @return: intersection of *args sets.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('SUNION %s\r\n' % ' '.join(args))
            rc = set(self.get_response())
        finally:
            self.lock.release()
        return rc

    def sdiff(self, *args):
        """
            Produce and return the set difference of all the set variables.\r\n
            Protocol::

                    Client to server:   SDIFFER <name1> <name2> ... <nameN>\\r\\n

                    Server to client:   *<No.entries>\\r\\n<entry1>\\r\\n<entry2>\\r\\n ...\\r\\n<entryN>\\r\\n

                                                where <entryN> is $<len of value>\\r\\n<value>

                                        $-1\\r\\n

            @type args: string
            @param args: one or more names of set variables

            @rtype: set
            @return: intersection of *args sets.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('SDIFFER %s\r\n' % ' '.join(args))
            rc = set(self.get_response())
        finally:
            self.lock.release()
        return rc

    def sinterstore(self, dest, *args):
        """
            Produce the intersection of the named sets and store it in the database.\r\n
            Protocol::

                    Client to server:   SINTERSTORE <dest> <name1> <name2> ... <nameN>\\r\\n

                    Server to client:   +OK\\r\\n

                                        -ERR <error message>\\r\\n

            @type dest: string
            @param dest: name of variable to hold computed intersection

            @type args: string
            @param args: one or more names of set variables

            @rtype: set
            @return: intersection of *args sets.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('SINTERSTORE %s %s\r\n' % (dest, ' '.join(args)))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def sunionstore(self, dest, *args):
        """
            Produce the union of the named sets and store it in the database.\r\n
            Protocol::

                    Client to server:   SUNIONSTORE <dest> <name1> <name2> ... <nameN>\\r\\n

                    Server to client:   +OK\\r\\n

                                        -ERR <error message>\\r\\n

            @type dest: string
            @param dest: name of variable to hold computed intersection

            @type args: string
            @param args: one or more names of set variables

            @rtype: set
            @return: intersection of *args sets.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('SUNIONSTORE %s %s\r\n' % (dest, ' '.join(args)))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def sdiffstore(self, dest, *args):
        """
            Produce the set difference of the named sets and store it in the database.\r\n
            Protocol::

                    Client to server:   SDIFTTORE <dest> <name1> <name2> ... <nameN>\\r\\n

                    Server to client:   +OK\\r\\n

                                        -ERR <error message>\\r\\n

            @type dest: string
            @param dest: name of variable to hold computed intersection

            @type args: string
            @param args: one or more names of set variables

            @rtype: set
            @return: intersection of *args sets.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('SDIFFSTORE %s %s\r\n' % (dest, ' '.join(args)))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def smembers(self, name):
        """
            Return the members of a set.\r\n
            Protocol::

                    Client to server:   SMEMBERS <name>\\r\\n

                    Server to client:   *<No.entries>\\r\\n<entry1>\\r\\n<entry2>\\r\\n ...\\r\\n<entryN>\\r\\n

                                                where <entryN> is $<len of value>\\r\\n<value>

                                        $-1\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of set variable

            @rtype: set
            @return: None or a set containing all the members.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('SMEMBERS %s\r\n' % name)
            rc = set(self.get_response())
        finally:
            self.lock.release()
        return rc

    def spop(self,name):
        """
            Pop a random member off the set. If the set is empty or
            doesn't exist return None.\r\n
            Protocol::

                    Client to server:   SPOP <name>\\r\\n

                    Server to client:   $<len of value>\\r\\n<value>\\r\\r

                                        $-1\\r\\n

                                        -ERR <error message>\\r\\n

            @type name: string
            @param name: name of set variable

            @rtype: string
            @return: None or an element of the set as a string.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('SPOP %s\r\n' % name)
            rc = set(self.get_response())
        finally:
            self.lock.release()
        return rc

    def smove(self,src,member,dest):
        """
            Move a 'member' from a source set to a destination set.  The sourcee set
            must exist and have the member in it for the member to be created in
            the destination set. If the src is a set and has the member, 1 is
            returned. Otherwise 0 is returned.\r\n
            Protocol::

                    Client to server:   SMOVE <src> <dest> <length-of-member>\\r\\n<member>\\r\\n

                    Server to client:   :0\\r\\n

                                        :1\\r\\n

                                        -ERR <error message>\\r\\n

            @type src: string
            @param src: name of set variable

            @type member: string
            @param member: member to move from src to dest

            @type dest: string
            @param dest: name of destination set variables

            @rtype: integer
            @return: 0, if the member wasn't found in the source set or the source set
            didn't exist. 1 if it succeeded.

        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('SMOVE %s %s %d\r\n%s\r\n' % src, dest, len(member), member)
            rc = set(self.get_response())
        finally:
            self.lock.release()
        return rc


    def select(self, db):
        """
            All database operations following this one will use the new database
            identified by 'db'. The default 'db' is '0'.\r\n
            Protocol::

                    Client to server:   SELECT <db>\\r\\n

                    Server to client:   +OK\\r\\n

                                        -ERR <error message>

            @type db: string
            @param db: name of database to use

            @rtype: string
            @return: OK
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('SELECT %s\r\n' % db)
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def move(self, name, db):
        """
            Move a variable from the currently selected database to the
            database 'db'. The variable must exist in the original database
            and must not exist in the target database.\r\n
            Protocol::

                    Client to server:   MOVE <name1> <db>\\r\\n

                    Server to client:   :0\\r\\n

                                        :1\\r\\n

                                        -ERR <error message>

            @type name: string
            @param name: name of variable to move

            @type db: string
            @param db: target database (it can be a new database or
            an existing one)

            @rtype: integer
            @return: 0 if the move failed. 1 otherwise.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('MOVE %s %s\r\n' % (name, db))
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def save(self, background=False):
        """
            Save the current state of the database. If background = True, save the state
            in the background.\r\n
            Protocol::

                    Client to server:   BGSAVE\\r\\n

                                        SAVE\\r\\n

                    Server to client:   +OK\\r\\n

            @type background: boolean
            @param background: if True, save the database in the background.

            @rtype: string
            @return: +OK
        """
        self.connect()
        try:
            self.lock.acquire()
            if background:
                self._write('BGSAVE\r\n')
            else:
                self._write('SAVE\r\n')
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def lastsave(self):
        """
            Get the time, in seconds, of the last save of the database.\\rn
            Protocol::

                    Client to server:   LASTSAVE\\r\\n

                    Server to client:   :<last-save-time>\\r\\n

            @rtype: float
            @return: time of last save, in seconds.
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('LASTSAVE\r\n')
            rc = self.get_response()
        finally:
            self.lock.release()
        return rc

    def info(self):
        """
            Return various information about the operation of the server, as an info string.\r\n
            Protocol::

                    Client to server:   INFO\\r\\n

                    Server to client:   $<len of value>\\r\\n<value>

                                        -ERR <error message>\\r\\n

            @rtype: string
            @return: information about the operation of the server
        """
        self.connect()
        try:
            self.lock.acquire()
            self._write('INFO\r\n')
            info = dict()
            for l in self.get_response().split('\r\n'):
                if not l:
                    continue
                k, v = l.split(':', 1)
                info[k] = int(v) if v.isdigit() else v
        finally:
            self.lock.release()
        return info
