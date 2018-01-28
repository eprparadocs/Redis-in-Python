#!/usr/bin/env python
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

# Import some standard Python modules
import socket
import logging
from os.path import exists,isfile
from os import environ
from ConfigParser import SafeConfigParser

# Our libraries
from lib import log
from nosql.TSserver import server


# If we are running from the command line...
if __name__ == '__main__':

    # Collect the options from the command line. If the scan fails we will just output the
    # error and terminate.
    environ['COLUMNS'] = "100"
    from optparse import OptionParser

    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage,version="%prog 1.0")
    parser.add_option("-d","--debug",dest="debug",action="store_true",
                        help="turn on debug logging [default: %default]")
    parser.add_option("-H", "--home", dest="homedir", metavar="dir",
                        help="home directory for the Twisted services [default: %default]")
    parser.add_option("-n","--nosql",dest="redis",metavar="ip",
                        help="IP address and port of service [default: %default]")
    parser.add_option("-m","--master", dest="master",action="store_true",
                        help="indicates running as Master instead of Slave [default: %default]")
    parser.add_option("-i","--initfile",dest="initfile",metavar="file",
                        help="init file [default: %default]")
    parser.add_option("-r","--rdump",dest="dbfile",metavar="file",
                        help="binary dump file [default: %default]")
    parser.set_defaults(dbfile="TSnosql-rdump.db")
    parser.set_defaults(initfile="MDB.TXT")
    parser.set_defaults(redis="localhost:6379")
    parser.set_defaults(debug=False)
    parser.set_defaults(homedir=".")
    parser.set_defaults(master=False)
    (options, args) = parser.parse_args()

    # Logging, if necessary and make it look nice.
    logging.basicConfig(filename="TSnosql.log",level=logging.DEBUG if options.debug else logging.INFO,
                                format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Get our IP address for later use.
    s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    s.connect(("localhost",0))
    ourIP = s.getsockname()[0]
    s.close()

    # See if we will expect the authentication before proceeding.
    server.updateCount = 10
    if exists(".redis.cfg") and isfile(".redis.cfg"):
        # We have the hidden config file...we have the password in it..
        config = ConfigParser.ConfigParser()
        config.readfp(open('.redis.cfg'))
        passwd = config.get("security","auth")
        if passwd :
            server.auth = auth
        uc = config.get("general","updateCount")
        if uc:
            server.updateCount = uc

    # Create the caching server
    mserver = server(ip=options.redis,master=options.master,initfile=options.initfile,dbfile=options.dbfile)
    mserver.ourIP = ourIP

    # Setup stuff for invoking log setup. We do this because if the TSnosql server fails we want
    # error messages going to the right place.
    class NoNo:
        def get(self,x,default=None):
            a = mserver.db.get("0",x)
            return default if a == None else a

    dc = NoNo()
    dc.debugFlag = options.debug
    dc.appName = "TSnosql"
    if len(args) :
        dc.HostName = args[0]
    else:
        dc.HostName = socket.gethostname()
    #log.setupLogging(dc)

    logging.info("Twisted Storage nosql server started as %s on node %s." % ("master" if options.master else "slave", dc.HostName))
    mserver.start()

    # When we get here things are shutting down. We need to make sure the slaveDriver is all done before
    # we exit. We don't worry about the timer task since we will get here if the server closed by itself
    # or because of the timer going off. 
    logging.info("Twisted Storage nosql server terminating.")
