# -*- coding: iso-8859-1 -*-
# Exceptions generated in TScomm
#
#   ConnectionError - Error in the connection
#   ResponseError - Error in the response format
#   InvalidResponse - Invalid response
#
class ConnectionError(Exception):
    pass

class ResponseError(Exception):
    pass

class InvalidResponse(Exception):
    pass

# Exceptions generated in TSnosql, TSdb and TSconn
#
class ERR(Exception):
    pass
