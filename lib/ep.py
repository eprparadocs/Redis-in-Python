def checkEP(ep):
    """
    Check that an end point address is correct. This must be of the form:


        [HTTP://]foo.com:port

        or

        [HTTPS://]foo.com:port

    What we return is a completed address. If we return None, the end point
    address was incorrect.
    """
    # Split apart the string. We can get one, two or three elements from it.
    # One element is definitely an error. Two will be an error if the second
    # isn't an integer
    parts = ep.split(':')
    if len(parts) == 2:
        if parts[0].upper() in ("HTTP","HTTPS"): return None
        if parts[1].isdigit() == False: return None
        return "HTTP://" + ep
    elif len(parts) == 3:
        if parts[0].upper() not in ("HTTP","HTTPS"): return None
        if parts[2].isdigit() == False: return None
        return ep
    else: return None