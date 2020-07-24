# -*- coding: iso-8859-1 -*-

"""
    Copyright (c) 2009 Charles E. R. Wegrzyn
    All Right Reserved.

    chuck.wegrzyn at gmail.com

    This application is free software and subject to the Version 1.0
    of the Common Public Attribution License. 

"""

import asyncore
import logging

from TSexcept import ERR

# Actual per-connection server support
class PyNoSql(asyncore.dispatcher):
    """
    Handles actual (subset of) redis commands.
    """
    def __init__(self,sock,who,server,db):
        asyncore.dispatcher.__init__(self, sock=sock)
        self.IP = who
        self.output = None
        self.server = server
        self.closeFlag = False
        self.main = server.main

        # select the 0th database as default.
        self.whichdb = "0"
        self.db = db

        # Authenticated, just in case...
        self.auth = False if "auth" in dir(server) else True

        # Log event, just in case...
        logging.debug("PyNoSql.__init__: New connection from (%s,%d)" % (self.IP[0],self.IP[1]))

    def selectFunc(self,parts):
        """
            Select the default database to use. All sets, gets,etc will occur
            in this database.

            SELECT <db>\r\n

            Always returns +OK\r\n
        """
        # Save the value for our later use.
        self.whichdb = parts[0]

        # Make sure there really is a database with that name.
        self.db.select(parts[0])

        # Return ok!
        return "+OK"

    def getFunc(self,parts):
        """
            Get a variable value

            value = GET <varName>\r\n

            Return either the value of the variable, $<len>\r\n<value>\r\n or None,$-1\r\n
        """
        try:
            rval = self.db.get(self.whichdb,parts[0])
            val = "$%d\r\n%s" % (len(rval),rval)
        except KeyError:
            val = "$-1"
        return val

    def mgetFunc(self,parts):
        """
            Get values for multiple variables at one time.

            [value1,...] = MGET <varName1> <varName2> ....\r\n

            Returns a list of values. Each value can be either None or the real value of the variable.
            Note that all variables must be string; any variable that isn't a string will result in
            None being returned for its value.

            Returns list looks like:

                *<numEntriesinList>\r\n<Entry1>\r\n....

            Where each <EntryN> looks like $-1 if the variable doesn't exist or
            $<len>\r\n<value>, if it does exist.

        """
        ret = "*%d" % len(parts)
        for i in parts:
            ret = ret + "\r\n"
            ret += self.getFunc([i])
        return ret

    def getset(self,parts,wdb=None,q=True):
        """
            Get and set variable

            oldValue = GETSET <varName> <newValueLen>\r\n<newValue>\r\n

            Returns either the old value of the variable as $<len>\r\n<value>\r\n or
            None, $-1\r\n
        """
        wdb = wdb or self.whichdb
        rval = self.db.getset(wdb,parts[0],parts[1])
        if q == True: self.server.addToQueue(wdb,"set",parts[0],parts[1] )
        if rval == None:
            val = "$-1"
        else:
            val = "$%d\r\n%s" % (len(rval),rval)
        return val

    def setFunc(self,parts,wdb=None,q=True):
        """
            Set a variable to some value

            SET <varName> <valueLen>\r\n<value>\r\n

            Always returns +OK\r\n
        """
        s = parts[1]
        wdb = wdb or self.whichdb
        self.db.set(wdb,parts[0],s)
        if q == True: self.server.addToQueue(wdb,"set",parts[0],s )
        return "+OK"

    def setnxFunc(self,parts,wdb=None,q=True):
        """
            Set a variable to some value only if it doesn't already exist

            SETNX <varName> <valueLen>\r\n<value>\r\n

            Returns :1\r\n if the operation worked, otherwise :0\r\n.
        """
        rc = ":0"
        if not self.db.defined(self.whichdb,parts[0]):
            if self.setFunc(parts,wdb,q) in ["+OK"]:
                rc = ":1"
        return rc

    def delFunc(self,parts,wdb=None,q=True):
        """
            Delete one or more variables.

            DELETE <varName1> ....\r\n

            Returns the count of successful deletes, as :<cnt>\r\n
        """
        cnt = 0
        wdb = wdb or self.whichdb
        for i in parts:
            if self.db.defined(wdb,i) :
                self.db.remove(wdb,i)
                if q == True: self.server.addToQueue(wdb,"del",i)
                cnt = cnt +1
        return ":%d" % cnt

    def incrByFunc(self,parts,wdb=None,q=True):
        """
            Increment a numeric string variable by some value

            INCRBY <varName> <incrValue>\r\n

            Returns the new value as :<value>\r\n
        """
        try:
            wdb = wdb or self.whichdb
            i = self.db.add(wdb,parts[0],int(parts[1]))
            if q == True: self.server.addToQueue(wdb,"incrby",parts[0], parts[1])
            return ":%d" % (i)
        except KeyError:
            return "-ERR no such key" % (parts[0])
        except ValueError:
            return "-ERR %s is not an integer value" % (parts[1])

    def incrFunc(self,parts,wdb=None,q=True):
        """
            Increment a numeric string value by one.

            INCR <varName>\r\n

            Returns the new value as :<value>\r\n
        """
        return self.incrByFunc([parts[0],"1"],wdb=wdb,q=q )

    def decrByFunc(self,parts,wdb=None,q=True):
        """
            Decrement a numeric string value by some amount.

            DECRBY <varName> <value>\r\n

            Returns the new value as :<value>\r\n
        """
        try:
            val = str(-int(parts[1]))
            return self.incrByFunc([parts[0],val],wdb=wdb,q=q )
        except ValueError:
            return "-ERR %s is not an integer value" % (parts[1])

    def decrFunc(self,parts,wdb=None,q=True):
        """
            Decrement a numeric string value by 1

            DECR <varName>

            Returns the new value as :<value>\r\n
        """
        return self.incrByFunc([parts[0],"-1"],wdb=wdb,q=q )

    def saveFunc(self,parts,wdb=None,q=True):
        """
            Save the database to local disk.

            SAVE\r\n

            Always returns +OK\r\n
        """
        self.db.saveToDump()
        self.server.mark = self.db.totalChangeOperations
        if q == True: self.server.addToQueueNoVersion(wdb or self.whichdb,"save")
        return "+OK"

    def quitFunc(self,parts):
        """
            Quit the service. This will result in the connection to the client
            being shut down. The server is still running.

            QUIT\r\n

            Always returns +OK\r\n
        """
        self.closeFlag = True
        return "+OK"

    def pingFunc(self,parts):
        """
            Test if the service is still running.

            PING

            ALways returns +PONG\r\n
        """
        return "+PONG"

    def keysFunc(self,parts):
        """
            Returns a list of keys with a certain format.

            KEYS <globName>\r\n

        Returns a list of variable names that match the given argument of the
        format: $<lenOfValue>\r\n<value>. <value> is a space separated lists of
        names.
        """
        s = self.db.keys(self.whichdb,parts[0])
        return "$%d\r\n%s" % (len(s),s)

    def reFunc(self,parts):
        """
            Returns a list of keys with a certain format.

            RE <regExp>\r\n

        Returns a list of variable names that match the given argument of the
        format: $<lenOfValue>\r\n<value>. <value> is a space separated lists of
        names.
        """
        s = self.db.re(self.whichdb,parts[0])
        return "$%d\r\n%s" % (len(s),s)


    def existsFunc(self,parts):
        """
            Returns an indication of a variable existing at a specific instance.

            EXISTS <varName>\r\n

            Returns :1/r/n if the variable exists, :0/r/n otherwise.
        """
        if self.db.defined(self.whichdb,parts[0]):
            return ":1"
        else:
            return ":0"

    def renFunc(self,parts,wdb=None,q=True):
        """
            Rename a variable to a new name. If the new name matches the old name, or the
            original variable doesn't exist, and error is returned.

            RENAME <origName> <newName>\r\n

            Returns +OK\r\n if it worked, otherwise -ERR<errmsg>\r\n if it failed.
        """
        try:
            wdb = wdb or self.whichdb
            self.db.rename(wdb,parts[0],parts[1])
            if q == True: self.server.addToQueue(wdb,"rename",parts[0],parts[1])
            return "+OK"
        except ERR,e:
            return "-ERR " + e.__str__()

    def renxFunc(self,parts,wdb=None,q=True):
        """
            Rename a variable to a new name if it exists.

            RENAMENX <origName> <newName>\r\n

            Returns :1\r\n if it worked. Otherwise :0\r\n
        """
        wdb = wdb or self.whichdb
        if self.db.defined(wdb,parts[1]):
            return ":0"
        else:
            self.renFunc(parts,wdb,q)
            return ":1"

    def moveFunc(self,parts,wdb=None,q=True):
        """
            Move a variable from one database to another. The source variable must exist, and there
            must be no variable of the given name in the target database.

            MOVE <varName> <newDB>\r\n

            Returns :1\r\n if the move worked, otherwise it returns :0\r\n
        """
        try:
            wdb = wdb or self.whichdb
            self.db.move(wdb,parts[0],parts[1])
            if q == True: self.server.addToQueue(wdb,"move",parts[0],parts[1])
            return ":1"
        except ERR,e:
            return ":0"

    def bgFunc(self,parts,wdb=None,q=True):
        """
            Dump the database to disk in the background.

            BGSAVE\r\n

            Always returns +OK\r\n
        """
        self.server.dumpSema.set()
        if q == True: self.server.addToQueueNoVersion(wdb or self.whichdb,"bgsave")
        return "+OK"

    def dbsizeFunc(self,parts):
        """
            Return the number of keys in a database.

            DBSIZE\r\n

            Returns :<NoOfKeys>\r\n
        """
        return ":%d" % self.db.size(self.whichdb)

    def flushall(self,parts,wdb=None,q=True):
        """
            Clear out all variables from all databases.

            FLUSHALL\r\n

            Always returns +OK\r\n
        """
        self.db.flushAll()
        if q == True: self.server.addToQueue(wdb or self.whichdb,"flushall")
        return "+OK"

    def flushdb(self,parts,wdb=None,q=True):
        """
            Remove all the variables from the currently selected database.

            FLUSHDB\r\n

            Always returns +OK\r\n
        """
        wdb = wdb or self.whichdb
        self.db.flush(wdb)
        if q == True: self.server.addToQueue(wdb,"flushdb")
        return "+OK"

    def typeFunc(self,parts):
        """
            Returns the type of a given variable.

            TYPE <varName>\r\n

            Returns one of: +none\r\n, +string\r\n, +list\r\n +set\r\n
        """
        val = self.db.getType(self.whichdb,parts[0])
        return "+" + val

    def expireFunc(self,parts,wdb=None,q=True):
        """
            Sets a time to live value on a variable. After the time expires the variable will
            cease to exist. If the value of a variable is changed before the variable expires
            the time to live is erased.

            EXPIRE <varName> <seconds>\r\n

            Returns -ERR<errmsg> if some error occurred, or :<seconds> if it worked.
        """
        try:
            wdb = wdb or self.whichdb
            secs = int(parts[1])
            val = self.db.expire(wdb,parts[0],secs)
            if q == True: self.server.addToQueue(wdb,"expire",parts[0],parts[1])
        except ValueError:
            return "-ERR expiration time '%s' is not an integer" % parts[1]
        except ERR,e:
            return "-ERR " + e.__str__()
        return ":" + str(val)

    def infoFunc(self,parts):
        tO,tcO,ls,v,NCC,NTC,st = self.server.getStats()
        dt = time.time() - st
        s = """version:%s\r\nconnected_clients:%d\r\nconnected_subordinates:%d\r\nused_memory:%d\r\nchanges_since_last_save:%d\r\nlast_save_time:%d\r\ntotal_connections_received:%d\r\ntotal_commands_processed:%d\r\nuptime_in_seconds:%d\r\nuptime_in_days:%d\r\nbgsave_in_progress:1\r\nrole:main""" % (v,
                NCC,0,0,tcO,ls,NTC,tO,dt,dt/(60*60*24))
        return "$%d\r\n" % len(s) + s

    def shutFunc(self,parts,wdb=None,q=True):
        """
            Shutdown the server, terminating all connections.

            SHUTDOWN\r\n

            Nothing returned; connection will be closed.
        """
        # Queue it up for all the other servers...
        if q == True: self.server.addToQueueNoVersion(wdb,"shutdown",noVersion=True)

        # Mark it as closing down...we do so gradually.
        self.server.serverClose()

        # Make sure the dumper exits...
        self.server.dumpSema.set()

    def lastFunc(self,parts):
        """
            Returns the unix time of the last save operation.

            LASTSAVE\r\n

            Returns :<lastSaveTime>\r\n
        """
        return ":%d" % self.db.timeofLastSave

    def authFunc(self,parts):
        """
            In systems that require authorization to use, this operation will
            invoke the operation.

            AUTH <password>\r\n

            Returns +OK if authorization succeeded, other -ERR<err msg> is returned
            if it failed. If autheorization is required and you aren't authorized you
            will not be allowed any access to the system.
        """
        # See if we have a auth entry in the
        self.auth = self.server.auth == parts[0]
        return "+OK" if self.auth else "-ERR authentication password incorrect"

    def ttlFunc(self,parts):
        """
            See if a variable has a time-to-live (expire) value set.

            TTL <varName>\r\n

            Returns :1\r\n if it does; otherwise :0\r\n
        """
        # Do we have an expiration in progress?
        rc = -1
        if self.db.__isExpire(self.whichdb,parts[0]):
            rc = self.db.expiredb[self.whichdb][parts[0]]

        # Return the result
        return ":%d" % rc

    def subordinateFunc(self,parts):
        """
            This function connects this server to a subordinate. Notice we don't check
            to see if this is a main or a subordinate. A subordinate can connect to other
            subordinates via relaying of changes.

            SLAVE <ip> <port>\r\n

            Returns +OK\r\n
        """
        # See if we are already connected.
        host,port = parts[0].split(":")
        if self.server.isConnected(host,port) == 0:
            # Nope. So connect to it.
            try:
                c = SubordinateComm(host,int(port))
                self.server.remoteConnection(host,port,c)

                # Send current database to subordinate.
                version,contents = self.db.getdb()
                c.replaceDB(version,contents)

                # Save the version number in the server table.
                self.server.version(host,port,version)
            except Exception,e:
                # Couldn't connect.
                logging.exception(e)
                self.server.removeConnection(host,port)
                return "-ERR Couldn't connect to server at ip '%s' port '%s'" % (host,port)

        # Return ok!
        return "+OK"

    def lpush(self,parts,wdb=None,q=True):
        """
            Create a list or for an existing list put the element at the head.

            LPUSH  <varName> <elem>\r\n

            Returns +OK\r\n
        """
        # This sets up a set.
        wdb = wdb or self.whichdb
        t = self.db.getType(wdb,parts[0])
        if t  == "none" :
            self.db.set(wdb,parts[0],[])
        self.db.insert(wdb,parts[0],0,parts[1])
        if q == True: self.server.addToQueue(wdb,"lpush",parts[0], parts[1])
        return "+OK"

    def rpush(self,parts,wdb=None,q=True):
        """
            Create a list or for an existing list put the element at the end.

            RPUSH  <varName> <elem>\r\n

            Returns +OK\r\n
        """
        # This sets up a set.
        wdb = wdb or self.whichdb
        t = self.db.getType(wdb,parts[0])
        if t  == "none" :
            self.db.set(wdb,parts[0],[])
        self.db.insert(wdb,parts[0],-1,parts[1])
        if q == True: self.server.addToQueue(wdb,"rpush",parts[0], parts[1])
        return "+OK"

    def llen(self,parts):
        # Get the length of a list...
        t = self.db.getType(self.whichdb,parts[0])
        return ":%d" % self.db.listlen(self.whichdb,parts[0])

    def lrange(self,parts):
        try:
            i = parts[1]
            start = int(parts[1])
            i = parts[2]
            end = int(parts[2])
            l = self.db.lrange(self.whichdb,parts[0],start,end)
            ret = "*%d" % len(l)
            for i in l:
                ret = ret + "\r\n$%d\r\n%s" % (len(i),i)
        except ValueError:
            ret = "-ERR %s is not an integer value" % i
        return ret

    def ltrim(self,parts,wdb=None,q=True):
        try:
            wdb = wdb or self.whichdb
            i = parts[1]
            start = int(parts[1])
            i = parts[2]
            end = int(parts[2])
            self.db.ltrim(wdb,parts[0],start,end+1)
            if q == True: self.server.addToQueue(wdb,"ltrim",parts[0],parts[1],parts[2] )
        except ValueError:
            return "-ERR %s is not an integer value" % i
        return "+OK"

    def lindex(self,parts):
        try:
            i = int(parts[1])
            r = self.db.lindex(self.whichdb,parts[0],i)
            if r == None:
                rc = "$-1"
            else:
                rc = "$%d\r\n%s" % (len(r),r)
        except ValueError:
            return "-ERR %s is not an integer value" % parts[1]
        return rc

    def lset(self,parts,wdb=None,q=True):
        try:
            wdb = wdb or self.whichdb
            s = parts[1]
            i = int(s)
            r = self.db.lset(wdb,parts[0],i,parts[2])
            if r == 0 and q == True: self.server.addToQueue(wdb,"lset",parts[0],parts[1],parts[2] )
            return "+OK" if r == 0 else "-ERR*range*"
        except ValueError:
            return "-ERR %s is not an integer value" % s

    def lrem(self,parts,wdb=None,q=True):
        try:
            wdb = wdb or self.whichdb
            s = parts[1]
            num = int(s)
            rc = self.db.lrem(wdb,parts[0],num,parts[2])
            if q == True: self.server.addToQueue(wdb,"lrem",parts[0],parts[1],parts[2] )
            return ":%d" % rc
        except ValueError:
            return "-ERR %s is not an integer value" % s

    def lpop(self,parts,wdb=None,q=True):
        wdb = wdb or self.whichdb
        rc = self.db.lpop(wdb,parts[0])
        if rc and q == True: self.server.addToQueue(wdb,"lpop",parts[0] )
        return "$%d\r\n%s" % (len(rc),rc) if rc else "$-1"

    def rpop(self,parts,wdb=None,q=True):
        wdb = wdb or self.whichdb
        rc = self.db.rpop(wdb,parts[0])
        if rc and q == True: self.server.addToQueue(wdb,"rpop",parts[0] )
        return "$%d\r\n%s" % (len(rc),rc) if rc else "$-1"

    def sadd(self,parts,wdb=None,q=True):
        """
            Add an item to a set, optionally creating the set.

            SADD <varName> <item>\r\n

            Returns :0\r\n if the element was already in the set, otherwise return
            :1\r\n
        """
        wdb = wdb or self.whichdb
        rc =  self.db.sadd(wdb,parts[0],parts[1])
        if rc and q == True: self.server.addToQueue(wdb,"sadd",parts[0],parts[1])
        return  ":%d" % (rc)

    def scard(self,parts):
        """
            Return the cardinality of the set.

            SCARD <varName>\r\n

            Returns :<cardinality>\r\n
        """
        return ":%d" % self.db.scard(self.whichdb,parts[0])

    def sismember(self,parts):
        """
            See if member is in the set.

            SISMEMBER <varName> <member>\r\n

            Returns :1\r\n if member is in the set, and :0\r\n otherwise.
        """
        return ":%d" % self.db.sismember(self.whichdb,parts[0],parts[1])

    def smembers(self,parts):
        """
            Return the members of a set.

            SMEMBERS <varName>\r\n

            Returns list looks like:

                *<numEntriesinList>\r\n<Entry1>\r\n....

            Where each <EntryN> looks like  $<len>\r\n<value>, if it does exist.
        """
        rlist = self.db.smembers(self.whichdb,parts[0])
        rc = "*%d" % (len(rlist))
        for i in rlist:
            rc = rc + "\r\n" + "$%d\r\n%s" % (len(i),i)
        return rc

    def srem(self,parts,wdb=None,q=True):
        """
            Remove a member from a set.

            SREM <varName> <memberName>\r\n

            Returns :1\r\n if the member was in the set and was removed.
            Otherwise it returns a :0\r\n if the member wasn't in the set.
        """
        wdb = wdb or self.whichdb
        rc =  self.db.srem(wdb,parts[0],parts[1])
        if rc and q == True: self.server.addToQueue(wdb,"srem",parts[0],parts[1])
        return ":%d" % (rc)

    def sinter(self,parts):
        """
            Produce the intersection of all the set variables.

            SINTER <varName1> <varName2> ...\r\n

            Returns list looks like:

                *<numEntriesinList>\r\n<Entry1>\r\n....

            Where each <EntryN> looks like  $<len>\r\n<value>, if it does exist.
        """
        rlist = self.db.sinter(self.whichdb,parts)
        rc = "*%d" % (len(rlist))
        for i in rlist:
            rc = rc + "\r\n" + "$%d\r\n%s" % (len(i),i)
        return rc

    def sunion(self,parts):
        """
            Produce the union of all the set variables.

            SUNION <varName1> <varName2> ...\r\n

            Returns list looks like:

                *<numEntriesinList>\r\n<Entry1>\r\n....

            Where each <EntryN> looks like  $<len>\r\n<value>, if it does exist.
        """
        rlist = self.db.sunion(self.whichdb,parts)
        rc = "*%d" % (len(rlist))
        for i in rlist:
            rc = rc + "\r\n" + "$%d\r\n%s" % (len(i),i)
        return rc

    def sdiff(self,parts):
        """
            Produce the difference of all the set variables.

            SDIFFER <varName1> <varName2> ...\r\n

            Returns list looks like:

                *<numEntriesinList>\r\n<Entry1>\r\n....

            Where each <EntryN> looks like  $<len>\r\n<value>, if it does exist.
        """
        rlist = self.db.sdiff(self.whichdb,parts)
        rc = "*%d" % (len(rlist))
        for i in rlist:
            rc = rc + "\r\n" + "$%d\r\n%s" % (len(i),i)
        return rc

    def sinterstore(self,parts,wdb=None,q=True):
        """
            Produce the intersection of all the set variables and store the result
            in variable dstName

            SINTERSTORE <dstName> <varName1> <varName2> ...\r\n

            Returns +OK
        """
        wdb = wdb or self.whichdb
        self.db.sinterstore(wdb,parts[0],parts[1:])
        if q == True: self.server.addToQueue(wdb,"sinterstore",parts[0],parts[1:])
        return "+OK"

    def sunionstore(self,parts,wdb=None,q=True):
        """
            Produce the union of all the set variables and store the result
            in variable dstName

            SUNIONSTORE <dstName> <varName1> <varName2> ...\r\n

            Returns +OK
        """
        wdb = wdb or self.whichdb
        self.db.sunionstore(wdb,parts[0],parts[1:])
        if q  == True: self.server.addToQueue(wdb,"sunionstore",parts[0],parts[1])
        return "+OK"

    def sdiffstore(self,parts,wdb=None,q=True):
        """
            Produce the difference of all the set variables and store the result
            in variable dstName

            SDIFFSTORE <dstName> <varName1> <varName2> ...\r\n

            Returns +OK
        """
        wdb = wdb or self.whichdb
        self.db.sdiffstore(wdb,parts[0],parts[1:])
        if q == True: self.server.addToQueue(wdb,"sdiffstore",parts[0],parts[1:])
        return "+OK"

    def spop(self,parts,wdb=None,q=True):
        """
            Pop a random member off the set. If the set is empty or
            doesn't exist return None.

            SPOP <varName>\r\n

            Returns $-1\r\n if there is no member, or $<len>\r\n<value>\r\n
            if there is a member.
        """
        wdb = wdb or self.whichdb
        rc = self.db.spop(wdb,parts[0])
        if rc and q == True: self.server.addToQueue(wdb,"spop",parts[0])
        return "$-1" if rc is None else "$%d\r\n%s" % (len(rc),rc)

    def smove(self,parts,wdb=None,q=True):
        """
            Move a member from a source set to a destination set.  The sourcee set
            must exist and have the member in it for the member to be created in
            the destination set.

            SMOVE <src> <dst> <member>\r\n

            If the src is a set and has the member, :1\r\n is returned. Otherwise
            :0\r\n is returned.
        """
        wdb = wdb or self.whichdb
        rc = self.db.smove(wdb,parts[0],parts[1],parts[2])
        if rc == 1 and q == True: self.server.addToQueue(wdb,"smove",parts[0],parts[1],parts[2])
        return ":%d" % rc if rc != -1 else "-ERR*Works on sets only"

    def randomkey(self,parts):
        rc = self.db.randomkey(self.whichdb)
        return "$%d\r\n%s" % (len(rc),rc) if rc else "$-1"

    def sortFunc(self,parts):
        """
            Sort the elements of a list or set.

            SORT <varName> [BY <pattern>] [LIMIT <start> <end>] [GET <pattern>] [ASC | DESC ALPHA]\r\n

            Returns the sorted list of elements.
        """
        # Figure out what sort of thing we are dealing with, and get the list of elements.
        t = self.db.getType(self.whichdb,parts[0])
        if t is "list":
            rl = self.db.lrange(self.whichdb,parts[0],0,-1)
        elif t is "set":
            rl = self.db.smembers(self.whichdb,parts[0])
        elif t is "none":
            raise ERR("-ERR no such key")
        else:
            raise ERR("-ERR Operation against a key holding the wrong kind of value")

        # Collect the various information from the remaining arguments...
        ascFlag = False
        alphaFlag = False
        byValue = None
        keyFunc = None
        limitValue = [0,-1]
        getValue = None

        # How we can sort things...numeric or alphabetical. The default
        # is numerical.
        def numeric(a,b):
            return cmp(float(a),float(b))

        def alpha(a,b):
            return cmp(a,b)
        cmpFunc = numeric

        # Scan everything else on the command line
        a = parts[1:]
        cnt = len(a)
        i = 0
        while i < cnt:
            # Get the option, converting it to lower case
            opt = a[i].lower().strip()

            # A little defensive programming....a null option is just ignored.
            if len(opt) == 0:
                i += 1
                continue

            # Figure out which option we are seeing.
            if opt == "by":
                # The rule is if the By pattern doesn't have an asterisk in it, we
                # don't use it.
                if (i+1) < cnt:
                    bv = a[i+1].strip()
                    if bv.find('*') != -1: byValue = bv
                    i += 1
                else:
                    raise ERR("-ERR sort has BY keyword and no pattern!")
            elif opt == "get":
                if (i+1) < cnt:
                    getValue = a[i+1].strip()
                    i += 1
                else:
                    raise ERR("-ERR sort has GET keyword and no pattern!")
            elif opt == "asc":
                ascFlag = False
            elif opt == "desc":
                ascFlag = True
            elif opt == "alpha":
                cmpFunc = alpha
            elif opt == "limit":
                if (i+2) < cnt:
                    num = a[i+1].strip()
                    if num.isdigit():
                        limitValue[0] = int(num)
                    else:
                        raise ERR("-ERR start value of limit not integer")
                    num = a[i+2].strip()
                    if num.isdigit():
                        limitValue[1] = int(num)
                    else:
                        raise ERR("-ERR end value of limit not integer")
                    i += 2
                else:
                    raise ERR("-ERR sort has LIMIT keyword and incomplete start and/or end values")
            else:
                raise ERR("-ERR unknown option '%s' on SORT command" % (a[i]))
            i += 1

        # If we do things with a By, we use the key function to get the value
        # associated with it.
        def byKeyFunction(key):
            var = byValue.replace("*",key)
            try:
                aVal = self.db.rawget(self.whichdb,var)
            except Exception,e:
                logging.exception("In byKeyFunction byVal is '%s'  key is '%s'  var is '%s'" % (byValue,key,var))
                raise ERR("-ERR sort aborted because of exception [%s %s %s]" % (byValue,key,var))
            return aVal if ascFlag == True else int(aVal)

        # If the user set the By value, we will use the function...
        if byValue :
            keyFunc = byKeyFunction
        else:
            keyFunc = None

        self.db.lock.acquire()
        sl = sorted(rl,cmp=cmpFunc,key=keyFunc,reverse=ascFlag)
        self.db.lock.release()

        # See if we have a limit...
        if limitValue[0] :
            s = limitValue[0]
            if s < 0: s += len(sl)
            e = limitValue[1]
            if e < 0: e += len(sl)
            e += 1
            t = sl[s:e]
            sl = t

        # Do we have a getValue to deal with? If so, we will process that here....
        if getValue:
            # We do, so we have an indirect step...
            pass
        else:
            # No, just just return the right stuff...
            rc = "*%d" % (len(sl))
            for i in sl:
                rc = rc + "\r\n" + "$%d\r\n%s" % (len(i),i)

        # Return the results.
        return rc

    def replacedb(self,parts):
        # Replace the entire database from a pickled image. This is mostly
        # for main-subordinate communications. A Main will send a copy of its
        # current database to a subordinate.
        vers = int(parts[0])
        self.db.replace(vers,parts[1])
        return "+OK"

    def version(self,parts):
        """
        Get the current version of the database. As each change is made to the database
        the version will change by 1.
        """
        return ":%d" % (self.db.version)

    # Commands supported by DO (inter-server functions)
    cmds = {"set":setFunc,"del":delFunc,"save":saveFunc,"incrby":incrByFunc,
                "decrby":decrByFunc,"rename":renFunc,"incr":incrFunc,
                "decr":decrFunc, "renamenx":renxFunc, "bgsave":bgFunc,
                "flushall":flushall,"flushdb":flushdb,"setnx":setnxFunc,
                "expire":expireFunc,"move":moveFunc,"shutdown":shutFunc,
                "lpush":lpush,"rpush":rpush,"ldel":delFunc, "ltrim":ltrim,
                "lset":lset, "lrem":lrem, "lpop":lpop, "rpop":rpop,
                "sadd":sadd, "srem":srem, "sinterstore":sinterstore, "spop":spop,
                "sunionstore":sunionstore, "sdiffstore":sdiffstore, "smove":smove }

    def doFunc(self,parts):
        """
        Take an operation, in pickled format, from some other server and
        do it!
        """
        rc = "+OK"

        # Do the operation - unpickle the command. The command has the format:
        #
        #   l[0] - version of the database expected
        #   [1] - the command
        #
        # with l[1] being a list with the following elements:
        #
        #       l[1][0] - which db to apply
        #       l[1][1] - the command
        #       l[1][2:] - args applied to command
        #
        s = StringIO(parts[0])
        l = pickle.load(s)
        s.close()

        # Make sure the versions match...
        mver = l[0]
        if mver != -1 and (self.db.version+1) != mver :
            logging.debug("DB version is %d  Command wants %d" % (self.db.version,mver))
            raise ERR("-ERR version mismatch")

        # Execute the command. Notice something about the arguments wdb and q.
        # The cases that happen are:
        #
        # Meaning of wdb:
        #
        #       If None, always execute command against db referenced by self.whichdb (changed by select command)
        #       If not None, this is the db to be used.
        #
        # Meaning of q:
        #
        #       If True, send command to subordinated connected to this server
        #       If False, don't send command to subordinates connected to this server
        #
        # Specific meansings:
        #
        #   in Main :   wdb = None, q = True       cmd executed against self.whichdb; queue cmd to subordinates
        #                 wdb = ...   q = True       cmd executed against wdb; queue cmd to subordinates
        #
        #   in Subordinate  :   wdb = ...   q = False      cmd executed in subordinate against wdb; don't push to subordinates of subordinate
        #                 wdb = ...   q = True       cmd executed in subordinate against wdb; pushed to subordinates of subordinate
        #
        # This implies that a Main can talk to N subordinates, each of which can be connected to other subordinates. Hence
        # a web like model.
        try:
            func = self.cmds[l[1][1]]
            logging.debug("Executing function %s with args = %s wdb = %s q = False" % (l[1][1],l[1][2:],l[1][0]))
            rc = func(self,list(l[1][2:]),wdb=l[1][0],q=self.server.anySubordinate())
        except Exception,e:
            # If there is no command, just ignore the operation.
            logging.exception(e)

        # Return the result
        return rc

    # Commands supported and functions that handle them.
    cmds = {"get":getFunc,"set":setFunc,"del":delFunc,"keys":keysFunc,
                "ping":pingFunc,"save":saveFunc,"incrby":incrByFunc,
                "decrby":decrByFunc,"do":doFunc,"quit":quitFunc,
                "exists":existsFunc,"rename":renFunc,"incr":incrFunc,
                "decr":decrFunc, "renamenx":renxFunc, "bgsave":bgFunc,
                "dbsize":dbsizeFunc, "mget":mgetFunc,"flushall":flushall,
                "flushdb":flushdb,"setnx":setnxFunc, "type":typeFunc,
                "expire":expireFunc, "select":selectFunc, "info":infoFunc,
                "move":moveFunc,"shutdown":shutFunc, "lastsave":lastFunc,
                "auth":authFunc,"getset":getset, "ttl":ttlFunc,
                "subordinate":subordinateFunc,"lpush":lpush,"rpush":rpush, "llen":llen,
                "ldel":delFunc, "lrange":lrange, "ltrim":ltrim, "lindex":lindex,
                "lset":lset, "lrem":lrem, "lpop":lpop, "rpop":rpop,
                "sadd":sadd, "scard":scard, "sismember":sismember,
                "smembers":smembers, "srem":srem, "sinter":sinter,
                "sunion":sunion, "sinterstore":sinterstore, "spop":spop,
                "sunionstore":sunionstore, "sdiff":sdiff, "sdiffstore":sdiffstore,
                "smove":smove, "randomkey":randomkey, "sort":sortFunc, "replacedb":replacedb,
                "version":version, "re":reFunc }

    # No. of arguments each command takes. -1 means undefined. Otherwise
    # the number is the number of parameters passed to the function.
    noArgs = {"get":1,"set":-1,"del":-1,"keys":1,
                "ping":0,"save":0,"incrby":2,
                "decrby":2,"do":-1,"quit":0,
                "exists":1,"rename":2,"incr":1,
                "decr":1, "renamenx":2, "bgsave":0,
                "dbsize":0, "mget":-1,"flushall":0,
                "flushdb":0,"setnx":2, "type":1,
                "expire":2, "select":1, "info":0,
                "move":2, "shutdown":0, "lastsave":0,
                "auth":1, "getset":2, "ttl":1,
                "subordinate":1,"lpush":2, "rpush":2,  "llen":1,
                "ldel":1, "lrange":3, "ltrim":3, "lindex":2,
                "lset":3, "lrem":3, "lpop":1, "rpop":1,
                "sadd":2, "scard":1, "sismember":2, "smembers":1,
                "srem":2, "sinter":-1, "sunion":-1, "sinterstore":-1,
                "sunionstore":-1, "sdiff":-1, "sdiffstore":-1, "spop":1,
                "smove":3, "randomkey":0, "sort":-1, "replacedb":2,
                "version":0, "re":1 }

    # In subordinate mode we allow ReplaceDB and Do along with any command
    # that has value of 1. In main mode we allow them all.
    #
    #   0   -> command modifies database
    #   1   -> command doesn't modify database
    #  -1   -> special command allowed all the time (ReplaceDB,Subordinate,etc)
    #
    noMod = {"get":1,"set":0,"del":0,"keys":1,
                "ping":1,"save":1,"incrby":0,
                "decrby":0,"do":-1,"quit":1,
                "exists":1,"rename":0,"incr":0,
                "decr":0, "renamenx":0, "bgsave":1,
                "dbsize":1, "mget":1,"flushall":0,
                "flushdb":0,"setnx":0, "type":1,
                "expire":0, "select":1, "info":1,
                "move":0, "shutdown":1, "lastsave":1,
                "auth":0, "getset":0, "ttl":1,
                "subordinate":-1, "lpush":0, "rpush":0, "llen":1,
                "ldel":0, "lrange":1, "ltrim":0, "lindex":1,
                "lset":0, "lrem":0, "lpop":0, "rpop":0,
                "sadd":0, "scard":1, "sismember":1,
                "smembers":1, "srem":0, "sinter":1,
                "sunion":1, "sinterstore":0, "sunionstore":0,
                "sdiff":1, "sdiffstore":0, "spop":0,
                "smove":0, "randomkey":1, "sort":1, "replacedb":-1,
                "version":1, "re":1 }

    # Type of primary argument. The result can be:
    #
    #    0 - string or none
    #   10 - string (must exist)
    #    1 - list or none
    #   11 - list (must exist)
    #    2 - set or none
    #   12 - set (must exist)
    #    3 - unknown (function will determine argument type)
    #
    argType = {"get":0,"set":3,"del":3,"keys":3,
                "ping":3,"save":3,"incrby":0,
                "decrby":0,"do":3,"quit":3,
                "exists":3,"rename":3,"incr":0,
                "decr":0, "renamenx":3, "bgsave":3,
                "dbsize":3, "mget":3,"flushall":3,
                "flushdb":3,"setnx":0, "type":3,
                "expire":3, "select":3, "info":3,
                "move":3, "shutdown":3, "lastsave":3,
                "auth":3, "getset":0, "ttl":3,
                "subordinate":3, "lpush":1, "rpush":1, "llen":11,
                "ldel":1, "lrange":1, "ltrim":11, "lindex":11,
                "lset":11, "lrem":11, "lpop":1, "rpop":1,
                "sadd":2, "scard":2, "sismember":2,
                "smembers":2, "srem":12, "sinter":3,
                "sunion":3, "sinterstore":3, "sunionstore":3,
                "sdiff":3, "sdiffstore":3, "spop":2, "smove":3,
                "randomkey":3, "sort":3, "replacedb":3,
                "version":3, "re":3 }

    def parse(self,data):
        try:
            # Split out the command into parts and make the actaul command
            # all lower case. Do it the hard way because we don't want to
            # touch, the possibly binary, data.
            halves = data.split("\r\n",1)
            parts = halves[0].split(" ")

            # See if we have "mass" data for the call. If we do, we need to determine the
            # size from the message. The size must be an integer, and if it is we will
            # truncate the mass data to the exact size (meaning we get rid of the ending
            # \r\n characters). We have to be careful with the mass data because it might
            # have embedded \r\n!
            if len(halves[1]):
                try:
                    i = len(parts)-1
                    parts[i] = halves[1][:int(parts[i])]
                except ValueError,e:
                    raise ERR("-ERR length in message is not an integer [%s]" % (e.__str__()))

            cmd = parts[0].lower()
            if len(parts) > 1 and len(parts[1]) == 0: del parts[1]

            # See what we need to do about authentication
            if self.auth == False and not cmd == "auth":
                raise ERR( "-ERR Must be authenticated first" )

            # Execute the command! First make sure we have a command we know about.
            if cmd not in self.cmds.keys():
                raise ERR( "-ERR '%s' is an unknown command" % cmd )

            # Get the number of arguments we expect and make sure we have them.
            n = self.noArgs[cmd]
            if n > 0 and len(parts)-1 != n:
                raise ERR( "-ERR poorly formed command %s - missing arguments. Expected %d got %d" % (cmd,n,len(parts)-1) )


            # Figure out if we allow the operation to execute. If the operations is reading
            # something, we always allow it. If it is writing, we only allow this to happen
            # on a server running in main mode and not terminating.
            modCmd = self.noMod[cmd]
            if modCmd == 0 and not self.main :
                raise ERR("-ERR server is operating in subordinate only mode; redirect to main")
            if modCmd == 0 and (self.server.halting or self.closeFlag) :
                raise ERR("-ERR server shutting down and command not allowed.")

            # See if we have the right type of the first (primary) argument, but only if we expect one.
            typeOfArg = self.argType[cmd]
            if typeOfArg != 3:
                t = self.db.getType(self.whichdb,parts[1])
                if  (typeOfArg ==  0 and t not in ["string","none"]) or \
                    (typeOfArg == 10 and t not in ["string"] ) or \
                    (typeOfArg ==  1 and t not in ["list","none"]) or \
                    (typeOfArg == 11 and t not in ["list"] ) or \
                    (typeOfArg ==  2 and t not in ["set","none"]) or \
                    (typeOfArg == 12 and t not in ["set"] ) :
                        raise ERR("-ERR Operation against a key holding the wrong kind of value")

            # Everything checked out, so execute the command.
            s = self.cmds[cmd](self,parts[1:])

            # Log the operation and result
            logging.debug( "PyNoSql.parse: CMD: %s  ARGS: %s  RESULTS: %s" % (parts[0],parts[1:],s) )

            # See if we need to write out the changes to the backing store...
            if self.db.totalChangeOperations - self.server.mark >= self.server.updateCount:
                self.saveFunc([])
                self.server.mark = self.db.totalChangeOperations

            # Return the results.
            return s

        except ValueError:
            return "-ERR argument length not an integer"

        except ERR,e:
            # One of the many exceptions!
            return e.__str__()

        except Exception,e:
            # Unhandled error!
            logging.exception(e.__str__())
            return "-ERR Unhandled exception [%s]" % (e.__str__())

    def handle_read(self):
        # Get data. If the connection has been closed we get a string of zero
        # length, so catch it. If we get a zero length string just close the connection
        # on our end too.
        data = self.recv(4024*1024)
        if len(data) == 0:
            self.close()
        else:
            # We have some data, so process it. We take on a CR-LF to the returned
            # data so it is in the right format. We don't worry about binary data
            # being returned since we are merely adding the characters to the end.
            s = self.parse(data) + "\r\n"

            # If the command was quit, now is the time to handle it...just
            # close the connection. Otherwise take the results of the command
            # processing and return it to the client.
            if self.closeFlag is True:
                self.close()
            else:
                self.output = s

    def writeable(self):
        # If there is still output to process, make sure it is sent.
        return bool(self.output)

    def handle_write(self):
        # If we have output, do...
        if self.output :
            # Pickup the data and send it out.
            data = self.output
            self.output = None
            sent = self.send(data)

            # If we still have more to send, make sure it goes out.
            if sent < len(data):
                remaining = data[sent:]
                self.output = remaining

    def handle_close(self):
        # We get here when the socket is closed. So update the current conection count
        # and log a message, just in case.
        logging.debug("PyNoSql.handle_close: Closing connection from (%s,%d)" % (self.IP[0],self.IP[1]))
        self.server.clientClose()

