"""
Logging support for the TS platform. We support four different types of logging
in the system off of the root logger:

	1) Object logging - anything affecting an object (uses object-IP/object-PORT)
	2) Critical logging - certain actions in the system require immediate admin
							(uses critical-IP/critical-PORT)
							examination
	3) Demo logging - to support the demo system we log certain things
	4) All else - normal things.

"""

# Standard python libraries
import logging
import logging.handlers


# Our constants
OBJECT = 100
DEMO = 101

# Setup the levels we need...
logging.addLevelName("OBJECT",OBJECT)
logging.addLevelName("DEMO",DEMO)

class MyLevelFilter(logging.Filter):
	def __init__(self,name="",lvl=[]):
		"""Initialize the filter"""
		logging.Filter.__init__(self, name)
		self.lvl = lvl

	def filter(self,rec):
		"""If the level in the record is in our lvl, only allow these through"""
		return (rec.levelno in self.lvl)

	def addLevel(self,lvl):
		self.lvl.append(lvl)


def setupLogging(dc):
	"""
	Setup the logging system per the information in the global database. We use the root logger
    since there is nothing special about it. The following handlers are added to it:

        Level           Handler             Action
        CRITICAL        datagram hander     Sends critical messages to (critical.IP,critical.PORT)
        DEMO            datagram handler    Sends demo messages to (demo.IP,demo.PORT)
        OBJECT          datagram handler    Sends object related messages to (object.IP,object.PORT)

    If debugging is enabled, the level of the logger is set to DEBUG otherwise it is WARNING and all
    messages besides those above are sent to <appName>.log
	"""

	# Configure the basic logger; if we don't have the value in the cache set it to False.
	if dc.debugFlag:
		l = logging.DEBUG
	else:
		l = logging.WARNING
	try:
		fn = dc.appName + '-' + dc.HostName + '-' + dc.Index + '.log'
	except:
		fn = dc.appName + '-' + dc.HostName + '.log'
	logging.basicConfig(level=l, format='%(asctime)s %(levelname)s %(message)s',filename = fn,filemode='w')

	# Get the handle to the root logger...
	rootlogger = logging.getLogger()

	# Create the CRITICAL handler. All critical messages are sent via datagram to the
	# pre-defined (ip adress,port)
	critical = logging.handlers.DatagramHandler(dc.get("critical.IP"),int(dc.get("critical.PORT")))
	critical.setLevel(logging.CRITICAL)
	cfilter = MyLevelFilter()
	cfilter.addLevel(logging.CRITICAL)
	critical.addFilter(cfilter)
	rootlogger.addHandler(critical)

	# Create the demo logger. If we aren't using it, we will not allow anything though the filter.
	demo = logging.handlers.DatagramHandler(dc.get("demo.IP"),int(dc.get("demo.PORT")))
	demo.setLevel(DEMO)
	dfilter = MyLevelFilter()
	dfilter.addLevel(DEMO)
	demo.addFilter(dfilter)
	rootlogger.addHandler(demo)

	# Set up the OBJECT handler.
	obj = logging.handlers.DatagramHandler(dc.get("object.IP"),int(dc.get("object.PORT")))
	obj.setLevel(OBJECT)
	ofilter = MyLevelFilter()
	if dc.get("objectFlag",default=False):
		ofilter.addLevel(OBJECT)
	obj.addFilter(ofilter)
	rootlogger.addHandler(obj)


