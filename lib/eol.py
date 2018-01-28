import re
import datetime


isodate = re.compile("([\d]{4})[-]([\d]{1,2})[-]([\d]{1,2})")
incdate = re.compile("[+]([\d]{1,3})([dmy])",re.IGNORECASE)

def checkISODATE(eol) :
    """
    Check to insure the eol is an iso-date format only.
    
    Throws ValueError if date isn't valid. Otherwise returns 1
    if date is in the future or 0 if not.
    """
    rc = 0
    if isodate.match(eol) :
        # Yes, so far. Make sure it is a future date.
        r = isodate.match(eol)
        t = datetime.date.today()
        s = datetime.date(int(r.group(1)),int(r.group(2)),int(r.group(3)))
        if s > t :
            rc = 1

    # Return the outcome
    return rc

def checkEOL(eol) :
    """
    This routine will validate the format of an 'eol' item, and optionally
    convert it into an ISO-8601 format.

    The formats accepted as valid eol is:

    YYYY-mm-dd   where YYYY is the year, mm is the month and dd is the day;
                    it is acceptable to leave off mm or mm and dd.
    +NNNa  where NNN is some number from 000 to 999 and a is the increment
                value (d for days, m for months and y for years).
    """

    # Check out which format it is....
    if isodate.match(eol) :
        # This is an iso-8601 date, which is an absolute date. We just leave it alone.
        # We have group(0) is the year, group(1) is the month and group(2) is the day.
        # We check to see if the date is in the future!
        r = isodate.match(eol)
        t = datetime.date.today()
        s = datetime.date(int(r.group(1)),int(r.group(2)),int(r.group(3)))
        if s <= t :
            raise ValueError("Supplied end-of-life must be in the future")
        rc = eol
    elif incdate.match(eol) :
        # It is an incremental date.
        r = incdate.match(eol)

        # r.group(1) is the incremental value and r.group(2) is d,m or y.
        # Convert the incremental time into days, since it is what we can use.
        t = datetime.date.today()
        d = int(r.group(1))
        if r.group(2) == 'y' :
            t = t.replace(t.year + d)
        elif r.group(2) == 'm' :
            # Convert the month value to years and months and adjust as needed
            d += t.month
            if d > 12 :
                t = t.replace(t.year+d/12,d%12)
        else :
            # This is days...
            t = t.toordinal() + d
            t = datetime.date.fromordinal(t)

        # Finally convert the whole thing to an iso-date.
        rc = t.isoformat()
    else :
        raise ValueError("Poorly formed end-of-life value '%s'" % eol)

    # Return the date to the caller!
    return rc
