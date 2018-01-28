# -*- coding: utf-8 -*-
# Standard Python libraries
import exceptions
import threading
import socket

# 3rd party libraries
from nosql import nosql

class notInitialized(exceptions.Exception):
    pass

class noData(exceptions.Exception):
    pass

class ConnectionError(exceptions.Exception):
    pass


class Connection(object):
    """
    This is a generatlized connection to the caching system.
    """
    def __init__(self, appName=None, cluster=None, node=None, nosql=None):
        """
        Initialize the redis connection structure with the cluster and node name.
        """
        # Set the cluster, node and app names - make sure they are in str() format!
        self.cluster = cluster.encode()
        self.node = node.encode()
        self.appName = appName.encode()
        self.nosql = nosql

    def Cluster(self,cluster):
        """
        Set in the cluster name...
        """
        self.cluster = cluster

    def Node(self,node):
        """
        Set in the node name
        """
        self.node = node

    def AppName(self,name):
        """
        Set the app name.
        """
        self.appName = name

    def setCluster(self,keyname,value):
        """
        Set the name at the cluster level
        """
        self.nosql.set(("%s.%s" % (self.cluster,keyname)),value)

    def setNode(self,keyname,value):
        """
        Set the name at the node level.
        """
        self.nosql.set(("%s.%s.%s" % (self.cluster,self.node,keyname)),value)

    def setApp(self,keyname,value):
        """
        Set the name at the app level
        """
        self.nosql.set(("%s.%s.%s.%s" % (self.cluster,self.node,self.appName,keyname)),value)

    def get(self,keyname, default=None, excp=False):
        """
        Get the value from the given key
        """
        if self.cluster is None or self.node is None or self.appName is None:
            raise(notInitialized)

        # Get the value, for which we follow some specific rules.
        # Try to find the variable in the following way:
        #
        #   1)  <cluster>.<node>.<appname>.keyname
        #   2)  <cluster>.<node>.keyname
        #   3)  <cluster>.keyname
        #   4)  keyname
        #   5)  default, if supplied.
        #
        # The idea is that we go from the specific to the general until we
        # get a value.
        datum = self.nosql.get("%s.%s.%s.%s" % (self.cluster,self.node,self.appName,keyname))
        if not datum:
           datum = self.nosql.get("%s.%s.%s" % (self.cluster,self.node,keyname))
        if not datum:
           datum = self.nosql.get("%s.%s" % (self.cluster,keyname))
        if not datum:
           datum = self.nosql.get(keyname)

        # If we don't have a value in the cache, see if we a default. If we don't
        # have a default we will throw an exception.
        if not datum:
            if excp == True :
                raise(noData,("Variable %s is not present" % (keyname)))
            else:
                datum = default
        return datum

    def getOne(self,key, default=None, excp=False):
        """
        Get a specific entry.
        """
        datum = self.nosql.get(key)
        if not datum:
            if excp == True:
                raise noData("Variable %s is not present" % (keyname))
            else:
                datum = default
        return datum

    def getSpecificList(self, keyname):
        """
        Return a list based on 'keyname'. We have keyname.0, keyname.1, etc. Unlike
        get(), we don't search for any specific entry.
        """
        # Get them...
        answer = self.nosql.re(keyname + ".\d")
        if answer: answer.sort()

        # Return the answer.
        return answer

    def getList(self,keyname, default=None, excp=False):
        """
        Get a list from the keyname using the standard search rules.
        """
        if self.cluster is None or self.node is None or self.appName is None:
            raise(notInitialized)

        # Get the value, for which we follow some specific rules.
        # Try to find the variable in the following way:
        #
        #   1)  <cluster>.<node>.<appname>.keyname
        #           the values are specific from this cluster/node and app
        #   2)  <cluster>.<node>.keyname
        #           the values are specific to the cluster/node
        #   3)  <cluster>.keyname
        #           the values are specific to the cluster
        #   1)  keyname
        #           the value are the most general (think of them
        #           as defaults)
        #   0)  default, if supplied.
        #
        # The idea is that we go from the specific to the general until we
        # get a value.
        ans = self.getSpecificList("%s.%s.%s.%s" % (self.cluster,self.node,self.appName,keyname))
        if not ans:
            ans = self.getSpecificList("%s.%s.%s" % (self.cluster,self.node,keyname))
        if not ans:
            ans = self.getSpecificList("%s.%s" % (self.cluster,keyname))
        if not ans:
            ans = self.getSpecificList(keyname)

        # If we don't have a value in the cache, throw an exception.
        if not ans:
            if excp == True:
                raise noData("Variable %s is not present" % (keyname))
            else:
                ans = default
        return ans



