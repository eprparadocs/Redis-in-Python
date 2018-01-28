# -*- coding: iso-8859-1 -*-

"""
    Copyright (c) 2009 Charles E. R. Wegrzyn
    All Right Reserved.

    chuck.wegrzyn at gmail.com

    This application is free software and subject to the Version 1.0
    of the Common Public Attribution License. 

"""

import asyncore
import threading
import time
import Queue
from os.path import exists,isfile
import logging
import socket
from cStringIO import StringIO


from TSdb import DB
from TSconn import PyNoSql

def Dumper(server):
    """
    This thread is responsible for periodically dumping the database to backing store. It might be
    over kill but for now we make sure the database is written after each and every change.
    """
    logging.debug("Dumper: thread starting.")

    while not server.halting:
        # Wait until the database has been chanced...
        server.dumpSema.wait()
        server.dumpSema.clear()

        # Possibly write the database out to disk.
        server.db.saveToDump()

    # Finished.
    logging.debug("Dumper: stopped.")

def slaveDriver(server):
    """
    This thread is responsible for writing all changes to the database to other servers
    in the system.
    """
    logging.debug("slaveDriver: thread starting.")

    # Don't halt until the queue of updates is pushed out to the other servers...
    while not (server.halting and server.queue.empty()):
        try:
            # Get something from the queue, and if nothing after 10 seconds
            # we get an exception.
            msg = server.queue.get(True,10)
            server.writeToAll(msg)
            server.queue.task_done()
        except Queue.Empty:
            # Nothing, so just ignore the exception and try again.
            pass

    # All done!
    logging.debug("slaveDriver: stopped.")

def TimerTask(server):
    """
    This thread is "fired" when the system is shutting down. It will close all the connections
    of servers that didn't close down by their own volition.
    """
    logging.debug("Timer: thread starting.")

    # Wait for the "event" to trigger us.
    server.timerSema.acquire()

    # Sleep for some seconds. When we wake back up, close down everything!
    time.sleep(15)
    logging.debug("Timer: 15 seconds until server shuts down.")
    server.close()

    # Finished.
    logging.debug("Timer: stopped.")


class server(asyncore.dispatcher):
    """
    This is the main server - it only handles connections. The rest is handled by the PyNoSql class.
    """
    # Things related to statistics used in the Info comoand
    version = "0.1"
    NoCurrentClients = 0
    NoTotalClients = 0

    # Time we started...
    startTime = time.time()

    # The dumpSema semaphore is used to trigger the Dumper task into running and saving
    # the state of the database.
    dumpSema = threading.Event()

    # Queue of commands that have changed the database in some way
    queue = Queue.Queue(0)

    # Flag indicating if server is shutting down. Affected by the shutdown command
    halting = False

    # Dictionary of clients connected to us. The key to each entry is the (ip,port)
    # of the client. Each entry holds the reference to the PyNoSql server handling
    # the client
    clientList = {}

    # Dictionary of remote servers we are connected to.
    remoteList = {}
    remoteVersion = {}

    # The shutdown semaphore - when this is released() it triggers the timer task. Once
    # the sleep expires the timer task will force all the clients to close and will kill
    # the server.
    timerSema = threading.Semaphore(0)

    # Mark is the changed operation count for the database. This is used to determine when
    # to dump the database to backing store.
    mark = 0

    def __init__(self, ip="localhost:6379",initfile="MDB.TXT",dbfile="TSnosql-rdump.db",master=True):

        # Initialize the class that makes this object work.
        asyncore.dispatcher.__init__(self)

        # Setup the data base - it is shared by all instances.
        self.db = DB(dbfile)
        self.mark = self.db.totalChangeOperations

        # Remember if we assume master or slave operation
        self.master = master

        # See if the dump database file works. If so, we will use it. If not, we will
        # create a clean database and initialize it, if that file exists.
        if exists(dbfile) and isfile(dbfile):
            # We have it, so make use of it.
            logging.debug("server.__init__: Loading pre-existing cache database from dump file %s" % dbfile)
            self.db.loadFromDump()
        elif exists(initfile) and isfile(initfile):
            # The dump file isn't around, so see if we have an init file.
            # If so, we will use it to build the initial db.
            logging.debug("server.__init__: Loading cache database from ini file %s" % initfile)
            self.db.loadFromIni("0",iniFile=initfile)
            self.db.saveToDump()
        else:
            # Start from scratch!
            logging.debug("server.__init__: Created empty cache database")
            self.db.saveToDump()

        # Start the background slave queueing support...
        queThread = threading.Thread(target=slaveDriver,name="slaveDriver",args=(self,))
        queThread.daemon = True
        queThread.start()

        # Start the background db dumper..
        saveThread = threading.Thread(target=Dumper,name="Dumper",args=(self,))
        saveThread.daemon = True
        saveThread.start()

        # Start the timer task
        timerThread = threading.Thread(target=TimerTask,name="Timer",args=(self,))
        timerThread.daemon = True
        timerThread.start()

        # Create the socket and bind to the address.
        host,port = ip.split(":")
        if len(host) == 0 : host = "localhost"
        if len(port) == 0 : port = "6379"
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind( (host, int(port)) )
        self.listen(5)

    def handle_accept(self):
        """
        Someone is trying to connect to us, so accept it.
        """
        # If we aren't halting the server...
        if not self.halting:
            # Accept the client connection.
            client = self.accept()

            # We use the clientList to point to all the "edis" servers that we
            # are using.
            self.NoTotalClients += 1
            self.NoCurrentClients += 1
            self.clientList[client[1]] = PyNoSql(client[0],client[1],self,self.db)

    def handle_close(self):
        pass

    def serverClose(self):
        # For all the servers, mark them as closing. In this way any new commands will result
        # in the server being shut down.
        for i in self.clientList.keys() :
            # Close it down....
            clientList[i].closeFlag = True

        # Trigger timer event. We give everyone plenty of time and then force all the rest...
        self.timerSema.release()

    def clientClose(self):
        # Reduce the current client count.
        self.NoCurrentClients -= 1

        # If all the clients are closed and we are shutting down the server, close
        # out socket (which fill force the entire process to start closing)
        if self.halting and self.NocurrentClients == 0:
            self.close()

    def start(self):
        # Start everything running...
        asyncore.loop()

    def getStats(self):
        # Return the following as a list:
        #
        #   totalOperations, totalChangeOperations, timeofLastSave, version, NoCurrentClients, NoTotalClients]
        #
        s = self.db.getStats()
        s.append(self.version)
        s.append(self.NoCurrentClients)
        s.append(self.NoTotalClients)
        s.append(self.startTime)
        return s

    def addToQueue(self,*args):
        # Write the parts to the queue...since the queue is infinite size I don't worry
        # about blocking or timeouts. The Dumper will take things off the queue and send
        # them to the end points.

        if len(self.remoteList) :
            # Message in the queue consists of the new version number (which must be 1 greater than
            # the current version) and the command. The command has the form:
            #   <whichdb> <op> <arg1>...
            #
            self.queue.put([self.db.version,args])

    def addToQueueNoVersion(self,*args):
        if len(self.remoteList):
            self.queue.put([-1,args])

    def writeToAll(self,cmd):
        """
        This function is responsible for writing any command that changes the database
        to all the other servers in the cluster of memcache-like servers. The names of the
        other servers come directly from the database.
        """

        # Collect the server list from the database. In this way other parts of the
        # cluster can change the server list and we will take it from there!

        # We have the current list, so now we need to see what servers are already
        # connected to us and which we need to establish. That will be the list
        # we work with...
        for i in self.remoteList.keys():
            try:
                # Send out the command to the other side...the 'cmd' is
                # a list of the format: [whichdb,cmd,arg1,arg2,...]
                #
                # Get the connection object to the slave
                c = self.remoteList[i]

                # Issue the command to the "other side"...we pickle it and
                # send it over. This is the easiest way.
                spickle = StringIO()
                pickle.dump(cmd,spickle)
                contents = spickle.getvalue()
                spickle.close()
                logging.debug("Sending operation '%s' to %s:%d" % (cmd,c.host,c.port))
                c._write("DO %d\r\n" % (len(contents)) + contents + "\r\n")
                rc = c.get_response()
                logging.debug("Received response from remote: %s" % (rc))

                # Increment the version of the database that lives on the remote
                # system.
                self.remoteVersion[i] += 1

            except Exception,e:
                #TODO: We can get a few types of exceptions here. One is related to the
                #Comm. channel breaking down. Another is lose of synchronization between
                #us and the slave.

                # Something happened with the connection or the server. So inform
                # the operator letting them know.
                logging.exception(e)
                logging.log(logging.CRITICAL,"Failure in TSnosql slave - IP %s" % (i[0]))
                c.disconnect()
                del c
                del self.remoteList[i]

    def isConnected(self,host,port):
        """
        See if the (host,port) is connecte to a remote server and the remote end is
        still active.
        """
        rc = 0
        try:
            if (host,port) in self.remoteList.keys():
                if self.remoteList[host,port].ping() == "pong":
                    rc = 1
        except :
            pass
        return rc

    def anySlave(self):
        """
        See if there is any connection to any slave.
        """
        return True if len(self.remoteList) > 0 else False

    def remoteConnection(self,host,port,commObj):
        """
        Add this connection to the remote services table. Returns handle to the data class.
        """
        self.remoteList[host,port] = commObj
        self.remoteVersion[host,port] = 0

    def version(self,host,port,version):
        """
        Save the version of the database in the slave.
        """
        self.remoteVersion[host,port] = version

    def removeConnection(self,host,port):
        """
        Renove a connection from the list.
        """
        if (host,port) in self.remoteList.keys():
          del self.remoteList[host,port]
          del self.remoteVersion[host,port]
