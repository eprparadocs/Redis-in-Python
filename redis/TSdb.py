# -*- coding: iso-8859-1 -*-

"""
    Copyright (c) 2009 Charles E. R. Wegrzyn
    All Right Reserved.

    chuck.wegrzyn at gmail.com

    This application is free software and subject to the Version 1.0
    of the Common Public Attribution License. 

"""


import threading
import cPickle as pickle
import time
import Queue
import logging
import sched
import re
import fnmatch
import random
from cStringIO import StringIO


from decorators import synchronized
from TSexcept import ERR


# Delete object via timer support
def delay_put(duration, queue, message):
    time.sleep(duration)
    queue.put(message)

def run_scheduler(scheduler):
    scheduler.run()

def deleteVariable(msg):
    # Delete the variable from the specific database dictionary
    which,var,dbObj = msg["data"]
    logging.debug("deleteVariable: Deleting object '%s' from database '%s' because of expiration" % (var,which))
    dbObj.remove(which,var)

class Scheduler(sched.scheduler):
    def __init__(self, queue, handler, timeout):
        self.message_queue = queue
        self.handler = handler
        self.timeout = timeout
        sched.scheduler.__init__(self, time.time, self.delay)

    def delay(self, duration):
        queue = self.message_queue
        if duration > 0:
            # Spawn a process that will sleep, enqueue None, and exit.
            threading.Thread(target=delay_put, args=(duration, queue, None)).start()
        try:
            message = queue.get(True, duration + self.timeout) # Block!
        except Queue.Empty:
            self.timed_out()
        else:
            if message is not None:
               # A message was enqueued during the delay.
                timestamp = message.get('timestamp', self.timefunc())
                priority = message.get('priority', 1)
                self.enterabs(timestamp, priority, self.handler, (message,))

    def timed_out(self):
        pass

    def startup(self):
        pass

    def shutdown(self):
        pass

    def run(self):
        # Schedule the `startup` event to trigger `delayfunc`.
        self.enter(0, 0, self.startup, ())
        sched.scheduler.run(self)
        self.shutdown()

class MessageQueue(object):
    def __init__(self, handler, timeout=10, scheduler_class=Scheduler):
        self.queue = Queue.Queue()
        self.scheduler = scheduler_class(self.queue, handler, timeout)
        self.worker = None

    def enq(self, message):
        self.queue.put(message)
        if not self.working():
            self.start_worker()

    def start_worker(self):
        self.worker = threading.Thread(target=run_scheduler, args=(self.scheduler,))
        self.worker.start()

    def working(self):
        return self.worker is not None and self.worker.isAlive()

# The shared databases class.
class DB(object):

    # The "database"...empty but with the one sub-dictionary we need.
    db = {"0":{}}
    expiredb = {}

    # Lock for synchronization
    lock = threading.Lock()

    # Various flags and counters...
    #
    #   changed = 1 implies db has been added to or removed from
    #   totalOperations is the number of all operations
    #   totalChangeOperations is the number of operations that changed the database since the last save
    #   timeofLastSave is the unix time of the last db save
    changed = 0
    totalOperations = 0
    totalChangeOperations = 0
    timeofLastSave = 0

    # The timer queue is related to expiration support in the system
    timerque = MessageQueue(deleteVariable)

    # generation of this database...this is incremented for each change of the  database
    version = 0

    def __init__(self,dbfile="rdump.db",expfile="expfile.db"):
        self.dbfile = dbfile
        self.expfile = expfile

    @synchronized(lock)
    def changedDB(self):
        """
        Return the existing changed flag and set it to 0.
        """
        r = self.changed
        self.changed = 0
        return r

    def getStats(self):
        """
        Return the statistics: total operation count, total operations that changed the database,
        and time of last save operation.
        """
        return [self.totalOperations, self.totalChangeOperations, self.timeofLastSave]

    def __clearExpire(self,which,var):
        # This function is run with the lock already acquired! It is a convenience function
        # for use only inside this class and any instances.
        if which in self.expiredb.keys():
            if var in self.expiredb[which].keys():
                del self.expiredb[which][var]

    def __setExpire(self,which,var,time):
        # This is an internal convenience function. It sets the entry in the expire
        # dictionary.
        if which not in self.expiredb.keys():
            self.expiredb[which] = {}
        self.expiredb[which][var] = time
        self.version += 1

    def __isExpire(self,which,var):
        # This is a convenience function to return an indication if an expire time 
        # is set for some variable.
        rc = 0
        if which in self.expiredb.keys():
            if var in self.expiredb[which].keys():
                rc = 1
        return rc

    def rawget(self,which,var):
        """
        This is the "essence" of a get() without the overhead.
        It assumes that someone else has locked the database.
        This is mainly used in the sort() operation.
        """
        val = self.db[which][var]
        if type(val) != type(""):
                raise KeyError
        return val

    @synchronized(lock)
    def get(self,which,var):
        """
        Return the value of a variable, throwing an KeyError excception
        if the variable doesn't exist or isn't a string type.
        """

        # Get the variable from the database. If it isn't there
        # Python will throw a KeyError exception. This should be
        # caught by the caller and used to indicate the variable
        # doesn't exist.
        val = self.db[which][var]

        # Make sure it is a "string" thinng. If not we throw
        # a KeyError exception. After all the string variable
        # doesn't exist.
        if type(val) != type(""):
            raise KeyError

        # Increment the count against the database.
        self.totalOperations += 1
        return val

    @synchronized(lock)
    def set(self,which,var,val):
        """
        Set a variable to a value. This will cancel any outstanding time expiration
        on the variable.
        """

        # If there are any expire events on this variable,
        # wipe them out.
        self.__clearExpire(which,var)

        # Set the variable to the value
        self.db[which][var] = val

        # Update the statistics for the database
        self.totalOperations += 1
        self.totalChangeOperations += 1
        self.changed = 1
        self.version += 1

    @synchronized(lock)
    def getset(self,which,var,val):
        """
        Return the existing variable value, or None, at the same time giving
        the variable a new value.
        """

        # If the variable exists, hold on to the orignal value. We will
        # return it to the caller.
        if var not in self.db[which].keys():
            oval = None
        else:
            oval = self.db[which][var]

        # Set the new value and clear out any timed expiry of the value.
        self.db[which][var] = val
        self.__clearExpire(which,var)

        # Indicate the database has changed and update the version counter.
        self.changed = 1
        self.version += 1

        # One more operation done against the database.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Return the original value
        return oval

    @synchronized(lock)
    def select(self,which):
        """
        Make sure the selected database exits.
        """
        if which not in self.db.keys():
            self.db[which] = {}

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

    @synchronized(lock)
    def defined(self,which,var):
        """
        Returns 1 if a variable exists in the database 'which', or 0 otherwise.
        """

        # Does the variable exist? If so, return 1. If not, return 0.
        if var in self.db[which].keys():
            val = 1
        else:
            val = 0

        # Update the total oper. count
        self.totalOperations += 1

        # Return the flag.
        return val

    @synchronized(lock)
    def move(self,fwhich,var,twhich):
        """
        Move a variable from one database, fwhich, to a new one, twhich. The variable
        must exist in the source database and not known in the new one.
        """
        # Make sure the target database exists.
        if twhich not in self.db.keys():
            self.db[twhich] = {}

        # Does the variable exist in the source database? If not
        # raise an error.
        if var not in self.db[fwhich].keys():
            raise ERR("%s not known in %s" % (var,fwhich))

        # Does the variable exist in the target database? If so
        # throw an error because of it.
        if var in self.db[twhich].keys():
            raise ERR("%s already known in %s" % (var,twhich))

        # Everything is fine...so move the variable over.
        self.db[twhich][var] = self.db[fwhich][var]

        # Delete the variable in the source database and get rid of
        # any expiry timer we have floating around.
        del self.db[fwhich][var]
        self.__clearExpire(fwhich,var)

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

    @synchronized(lock)
    def rename(self,which,fvar,tvar):
        """
        Rename a variable,fvar, to a new name, tvar. The source variable must
        exist and the target variable name must not be the source name.
        """
        if fvar == tvar:
            raise ERR("same names")
        if fvar not in self.db[which]:
            raise ERR("%s doesn't exist" % fvar)
        self.db[which][tvar] = self.db[which][fvar]
        del self.db[which][fvar]
        self.__clearExpire(which,fvar)

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

    @synchronized(lock)
    def add(self,which,var,val):


        # Does the variable exist and is it a string and numeric?
        # If not, we will force it to exist and be "0".
        if var not in self.db[which].keys():
            self.db[which][var] = "0"
        else:
            v = self.db[which][var]
            if type(v) != type("") or not v.isdigit():
                self.db[which][var] = "0"

        # Now add the value to it.
        val = int(self.db[which][var]) + val
        self.db[which][var] = str(val)
        self.__clearExpire(which,var)

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

        # Return the results.
        return val

    def __remove(self,which,var):
        # Special helper function - it assumes the table is already locked. This is 
        # used in the loading function when a variable needs to be removed.
        #

        # Do we have the specific sub-dictionary we need? If not, the
        # variable doesn't exist, so no need in gaing any further.
        if which in self.db.keys():

            # Does the variable exist in the dictionary?
            if var in self.db[which].keys():

                # Yes. So delete it from the dictionary.
                del self.db[which][var]

                # If there is an expiry on the variable, get rid of it too.
                self.__clearExpire(which,var)

    @synchronized(lock)
    def remove(self,which,var):
        # Do the actual remove
        self.__remove(which,var)

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

    @synchronized(lock)
    def keys(self,which,match):
        """
        Return all the variable names in the database.
        """
        matched = [x for x in self.db[which].keys() if fnmatch.fnmatch(x,match)]
        s = " ".join(matched)

        # Increment the total op. count.
        self.totalOperations += 1

        # Return results
        return s

    @synchronized(lock)
    def re(self,which,match):
        """
        Return all the variable names in the database that match
        a the regular expression.
        """
        p = re.compile(match)
        matched = [x for x in self.db[which].keys() if p.match(x)]
        s =  " ".join(matched)

        # Increment the total op. count.
        self.totalOperations += 1

        # Return results
        return s

    @synchronized(lock)
    def getType(self,which,var):
        """
        Return the type of a variable, which can be one of:

            none - variable doesn't exist
            string - variable is a string type
            list - variable is a list
            set - variable is a set
        """
        if var in self.db[which].keys():
            val = self.db[which][var]
            if type(val) is type(""):
                val = "string"
            elif type(val) is type([]):
                val = "list"
            else:
                val = "set"
        else:
            val = "none"

        # Imcrement the total op. count.
        self.totalOperations += 1

        # Return the results
        return val

    def __expire(self,which,var,secs):
        # Special helper function; used when loading files.
        #
        dt = time.time() + secs
        self.__setExpire(which,var,dt)

        # Trigger an event to remove it.
        self.timerque.enq({'data': [which,var,self], 'timestamp':dt })

    @synchronized(lock)
    def expire(self,which,var,secs):
        """
        Set an time to live limit on a variable. The variable must exist
        and the time is given in seconds. Tiem to live values live across
        invocations.
        """
        # Make sure we have the variable in the system.
        if var not in self.db[which].keys():
            raise ERR("no such key")

        # See if there is an expiration already on the variable. If so
        # we don't update it. Otherwise we do set it. Notice if we don't
        # take the "then" clause the return code is 0. This means it
        # failed.
        if self.__isExpire(which,var) == 0:
            self.__expire(which,var,secs)

            # Things have changed...
            self.changed = 1
            self.version += 1

        # UPdate the operation count.
        self.totalOperations += 1

        # Return the results
        return 1

    @synchronized(lock)
    def getTTL(self,which,var):
        """
        Get the current time to live value for a variable.
        """
        # Just check to see if the expire exists.
        pass

    @synchronized(lock)
    def flushAll(self):
        """
        Remove all the variables behind all the database tables.
        """
        self.db = {"0":{}}
        self.expiredb = {}

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

    @synchronized(lock)
    def flush(self,which):
        """
        Remove all the variables behind a specific table.
        """
        self.db[which] = {}
        self.expiredb[which] = {}

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

    @synchronized(lock)
    def size(self,which):
        """
        Return the number of keys in a specific table.
        """

        # Compute how many keys are in the subdictionary
        val = len(self.db[which].keys())

        # Update the counters.
        self.totalOperations += 1

        # Return the count.
        return val

    @synchronized(lock)
    def insert(self,which,var,index,val):
        # Insert it...
        if index == -1:
            self.db[which][var].append(val)
        else :
            self.db[which][var].insert(index,val)

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

    @synchronized(lock)
    def listlen(self,which,var):

        # Get the length of the list
        rc = len(self.db[which][var])

        # Update the total op. counter
        self.totalOperations += 1

        # Return the results.
        return rc

    @synchronized(lock)
    def lrange(self,which,var,s,e):

        # Does the variable exist?
        if var in self.db[which].keys():
            # Yes, so just extract the piece we want.
            v = self.db[which][var]
            if s < 0 : s = len(v) + s
            if e < 0 : e = len(v) + e
            e = e + 1
            rc = v[s:e]
        else:
            # Nope, return an empty list.
            rc = []

        # Update the operation counter.
        self.totalOperations += 1

        # Return the results
        return rc

    @synchronized(lock)
    def ltrim(self,which,var,s,e):

        # Update the list variable...we keep the slice [s;e] of the list
        v = self.db[which][var]
        if e > len(v): e = len(v)
        try:
            self.db[which][var] = v[s:e]
        except IndexError:
            self.db[which][var] = []

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

    @synchronized(lock)
    def lindex(self,which,var,i):

        try:
            r = self.db[which][var][i]
        except IndexError:
            r = None
        self.totalOperations += 1
        return r

    @synchronized(lock)
    def lset(self,which,var,i,val):

        # Find the variable and make sure we have a good
        # index.
        r = self.db[which][var]
        if  i >= len(r):
            self.totalOperations += 1
            return -1
        r[i] = val

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

        # Return the results
        return 0

    @synchronized(lock)
    def lrem(self,which,var,num,val):

        # Get the list we are to change...we are going to remove
        # at least 'num' instances of 'val' from the list. If
        # num is 0, we will remove all instances.
        v = self.db[which][var]
        if num >= 0 :
            start = 0
            end = len(v)
            incr = 1
            rev = 0
            if num == 0: num = end
        else:
            start = len(v)-1
            end = -1
            incr = -1
            num = abs(num)
            rev = 1
        l = []
        cnt = 0
        for i in range(start,end,incr):
            if v[i] != val:
                l.append(v[i])
            else:
                if num > 0:
                    cnt = cnt + 1
                    num = num - 1
                else:
                    l.append(v[i])

        # Append always puts things at the end of the list, which is okay
        # if we are scanning the list from 0 to the end ( num > -1). If we
        # are scanning it the other way, we need to reverse the list.
        if rev : l.reverse()
        self.db[which][var] = l

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

        # Return the results.
        return cnt

    @synchronized(lock)
    def lpop(self,which,var):

        try:
            # Does the variable exist? If so, pop it off the
            # front of the list. If not, return None!
            if var in self.db[which].keys():
                rc = self.db[which][var].pop(0)
            else:
                rc = None
        except IndexError:
            rc = None

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

        # Return either None, if there is no list or nothing
        # on it or the thing.
        return rc

    @synchronized(lock)
    def rpop(self,which,var):

        try:
            # See lpop for commentary...
            if var in self.db[which].keys():
                rc = self.db[which][var].pop()
            else:
                rc = None
        except IndexError:
            rc = None

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

        # Return any defined values.
        return rc

    @synchronized(lock)
    def sadd(self,which,var,item):

        if var not in self.db[which].keys():
            self.db[which][var] = set()
        if item in self.db[which][var]:
            rc = 0
        else:
            self.db[which][var].add(item)
            rc = 1

        # Increment the total op. count.
        self.totalOperations += 1
        if rc:
            # The database has changed!
            self.totalChangeOperations += 1
            self.changed = 1
            self.version += 1

        # Return the results
        return rc

    @synchronized(lock)
    def scard(self,which,var):

        if var not in self.db[which].keys():
            self.db[which][var] = set()
            self.totalChangeOperations += 1
            self.changed = 1
            self.version += 1

        self.totalOperations += 1
        return len(self.db[which][var])

    @synchronized(lock)
    def sismember(self,which,var,member):

        rc = 0
        if (var in self.db[which].keys()) and (member in self.db[which][var]):
            rc = 1
        self.totalOperations += 1
        return rc

    @synchronized(lock)
    def smembers(self,which,var):

        if var not in self.db[which].keys():
            self.db[which][var] = set()
            self.totalChangeOperations += 1
            self.changed = 1
            self.version += 1
        self.totalOperations += 1
        return list(self.db[which][var])

    @synchronized(lock)
    def srem(self,which,var,item):

        rc = 1 if item in self.db[which][var] else 0
        if rc:
            self.db[which][var].remove(item)
            self.totalChangeOperations += 1
            self.changed = 1
            self.version += 1
        self.totalOperations += 1
        return rc

    @synchronized(lock)
    def sinter(self,which,listOfVars):

        result = None
        self.totalOperations += 1
        for i in listOfVars:
                # If a variable doesn't exist, it is like
                # a set with no members. Any set that intersects
                # with the empty set is empty. So we just return!
                if i not in self.db[which].keys():
                    return []
                if type(self.db[which][i]) is not type(set()):
                    return []
                if result is None:
                    result = self.db[which][i]
                else:
                    result = result.intersection(self.db[which][i])
        return list(result) if result else []

    @synchronized(lock)
    def sunion(self,which,listOfVars):

        result = None
        for i in listOfVars:
                if i not in self.db[which].keys():
                    continue
                if type(self.db[which][i]) is not type(set()):
                    continue
                if result is None:
                    result = self.db[which][i]
                else:
                    result = result.union(self.db[which][i])
        self.totalOperations += 1
        return list(result) if result else []

    @synchronized(lock)
    def sinterstore(self,which,var,listOfVars):

        result = None
        for i in listOfVars:
                # If a variable doesn't exist, it is like
                # a set with no members. Any set that intersects
                # with the empty set is empty. So we just return!
                if i not in self.db[which].keys():
                    result = set()
                    break
                if type(self.db[which][i]) is not type(set()):
                    result = set()
                    break
                if result is None:
                    result = self.db[which][i]
                else:
                    result = result.intersection(self.db[which][i])
        self.db[which][var] = result

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

    @synchronized(lock)
    def sunionstore(self,which,var,listOfVars):

        result = None
        for i in listOfVars:
                if i not in self.db[which].keys():
                    continue
                if type(self.db[which][i]) is not type(set()):
                    continue
                if result is None:
                    result = self.db[which][i]
                else:
                    result = result.union(self.db[which][i])
        self.db[which][var] = result

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1

    @synchronized(lock)
    def sdiff(self,which,listOfVars):

        result = None
        for i in listOfVars:
                if i not in self.db[which].keys():
                    continue
                if type(self.db[which][i]) is not type(set()):
                    continue
                if result is None:
                    result = self.db[which][i]
                else:
                    result = result.difference(self.db[which][i])
        self.totalOperations += 1
        return list(result) if result else []

    @synchronized(lock)
    def sdiffstore(self,which,var,listOfVars):

        result = None
        for i in listOfVars:
                if i not in self.db[which].keys():
                    continue
                if type(self.db[which][i]) is not type(set()):
                    continue
                if result is None:
                    result = self.db[which][i]
                else:
                    result = result.difference(self.db[which][i])
        self.db[which][var] = result

        # Increment the total op. count.
        self.totalOperations += 1
        self.totalChangeOperations += 1

        # Update the database change flag and the version number.
        self.changed = 1
        self.version += 1


    @synchronized(lock)
    def spop(self,which,var):

        result = None
        try:
            result = self.db[which][var].pop()
            self.totalChangeOperations += 1
            self.changed = 1
            self.version += 1
        except KeyError:
            pass
        except NameError:
            pass
        self.totalOperations += 1
        return result

    @synchronized(lock)
    def smove(self,which,frm,to,member):

        # The from and destination variables, if they exist must be sets.
        self.totalOperations += 1
        if frm in self.db[which].keys() and type(self.db[which][frm]) != type(set()):
            return -1
        if to in self.db[which].keys() and type(self.db[which][to]) != type(set()):
            return -1

        # Ok,so far. Set the default return to it failing.
        result = 0

        # Does the from variable exist and does the member exist in it?
        if frm in self.db[which].keys() and member in self.db[which][frm]:
            self.db[which][frm].remove(member)
            if to not in self.db[which].keys():
                self.db[which][to] = set()
            self.db[which][to].add(member)
            result = 1

            # Increment the total op. count.
            self.totalChangeOperations += 1

            # Update the database change flag and the version number.
            self.changed = 1
            self.version += 1

        return result

    @synchronized(lock)
    def randomkey(self,which):

        try:
            rc = random.choice(self.db[which].keys())
        except IndexError:
            rc = None
        self.totalOperations += 1
        return rc

    @synchronized(lock)
    def loadFromIni(self,which,iniFile="mdb.txt"):

        # Define a worker function...
        def processFile(which,fn):
            # Make sure we have a default dictionary to insert these updates. 
            if which is None:
                which = "0"
            if which not in self.db.keys():
                self.db[which] = {}
                self.expiredb[wnicn] = {}
            cdb = self.db[which]

            # Line counter to 1
            cnt = 1

            # This is the file we will read from
            f = open(fn,"r")

            # For each line in the file do...
            for line in f:
                # Get the line to the absolutely start and end
                sline = line.strip()

                # If the line is empty or is a comment, just ignore it.
                if (len(sline) == 0) or (sline[0] == '#'):
                    continue

                # Is the line one of the command lines we allow? Those are
                #   @include <filename>         read this file in
                #   @select <db>                use this database dictionary for all operations
                if sline[0] == '@':
                    xn = sline.split()
                    if len(xn) != 2:
                        raise ValueError("Poorly formed line (file '%s', line number '%d'): '%s'" % (fn,cnt,sline))
                    if xn[0].startswith("@include"):
                        processFile(which,xn[1])
                    elif xn[0].startswith("@select"):
                        which = xn[1]
                        if which not in self.db.keys():
                            self.db[which] = {}
                            self.expiredb[which] = {}
                        cdb = self.db[which]
                    else:
                        raise ValueError("Poorly formed line (file '%s', line number '%d'): '%s'" % (fn,cnt,line.strip()))
                else:
                    # Split apart the line into two pieces - the variable name (which can't begin with @) and the value.
                    key, value = re.split("[ \t]+",sline,1)

                    # Store it in the dictionary. Note this assumes the exiry value isn't set.
                    cdb[key] = value

                    # Update the total change ops counter
                    self.totalChangeOperations += 1

                # Update the line counter,
                cnt += 1

        # Read the ini file in, filling in the table
        processFile(which,iniFile)
        self.changed = 1
        self.version += 1

    def _loadFromDump(self,dmp):
        """
        Internal function to load the database and expire table from file-like objects.
        Note the file-like objects are closed when the contents are done being read from.
        """

        # Restore the variables part of the system. Note that we would never set the changed
        # variable for this - we have it in a file already!
        self.db = pickle.load(dmp)

        # Do the same for the epxiry dictionary
        c = 0
        temp = pickle.load(dmp)

        # Now fixup any outstanding expired items or will be expiring. If there are expiry items, this will result in the
        # databae being changed.
        ct = time.time()
        for i in temp.keys():
            for j in temp[i].keys():
                # We have something here...see if the time has expired.
                # if so, delete the object. If not set the timer.
                dt = temp[i][j]
                if ct >= dt:
                    # The time has expired, so delete the object.
                    logging.debug("LoadFromDump: removing variable '%s from database '%s' - already expired" % (j,i))
                    self.__remove(i,j)
                else:
                    # It is still valid, so set it.
                    logging.debug("LoadFromDump: setting expiration on variable '%s' from database '%s' to %d seconds from now" % (j,i,dt-ct));
                    self.__expire(i,j,dt-ct )
                c = 1

        self.totalOperations += 1
        self.changed = c
        if c: self.version += 1

    @synchronized(lock)
    def loadFromDump(self,dump=None):
        """
        Load the database and expire table from files.
        """

        # Restore the variables part of the system. Note that we would never set the changed
        # variable for this - we have it in a file already!
        df = dump or self.dbfile
        rdump = open(df,"r")

        # Call the loader, to get it into memory.
        self._loadFromDump(rdump)
        rdump.close()

    @synchronized(lock)
    def replace(self,vers,pdata):
        """
        Replace the database and expre tables from a string.
        The string is the pickled database followed by the
        pickled expire table.
        """
        pstr = StringIO(pdata)
        self._loadFromDump(pstr)
        pstr.close()

        # Update the version, changed and total ops. values
        self.version = vers
        self.changed = 1
        self.totalOperations += 1

    def _saveToDump(self,dmp):
        """
        Internal routine to save the data and expire information
        to file-like objects. The data is stored in pickled
        format.
        """
        # Save the data information
        pickle.dump(self.db,dmp)

        # Do the same for the expiry dictionary
        pickle.dump(self.expiredb,dmp)

        # Update various stats and flags.
        self.timeofLastSave = time.time()
        self.changed = 0
        self.totalOperations += 1

    @synchronized(lock)
    def saveToDump(self,dump=None):
        """
        Visible funtion to save the database and expire tables
        to files.
        """
        # Dump the variables part of the database
        df = dump or self.dbfile
        dfile = open(df,"w")

        # Do it!
        self._saveToDump(dfile)
        dfile.close()

        # Log that we saved it!
        logging.debug("Dumped database to backing store.")


    @synchronized(lock)
    def getdb(self):
        w = StringIO()
        self._saveToDump(w)
        contents = w.getvalue()
        w.close()
        return self.version,contents
