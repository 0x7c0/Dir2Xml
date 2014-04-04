'''
Common simple functions
Created on 27/mar/2014

@package Dir2Xml
@subpackage database
@version 0.5
@author 0x7c0 <0x7c0@teboss.tk>
@copyright Copyright (c) 2013, 0x7c0
@license http://www.gnu.org/licenses/gpl.html GPL v3 License
'''


try:
    from time import time, gmtime, strftime
    # personal
except ImportError as error:
    print('In %s cannot load required libraries: %s!' \
        % (__name__, error))
    raise Exception


#==============================================================================
# simple functions
#==============================================================================
def s_ut_crono(start, pprint=True):    # Crono old
    '''
    given the initial unix time
    return time spent

    @param time start:    stating time
    @param boolean pprint:    if print to output
    @return: string
    '''

    if(pprint):
        end = time() - start
        microsecond = int((end - int(end)) * 1000)
        if (end < 60):    # sec
            return '%s sec and %s ms' % (strftime('%S', \
                                             gmtime(end)), microsecond)
        elif (end < 3600):    # min
            return '%s min and %s ms' % (strftime('%M,%S', \
                                             gmtime(end)), microsecond)
        else:    # hr
            return '%s hr and %s ms' % (strftime('%H.%M,%S', \
                                             gmtime(end)), microsecond)
    else:
        return int(strftime('%S', gmtime(start)))


def s_temporary_db():    # Set_Tmp old
    '''
    build database in memory

    @return: string
    '''

    return ':memory:'


#==============================================================================
# string functions
#==============================================================================
def s_str_reverse(normal):    # Reverse old
    '''
    reverse string or list

    @param string normal:    string
    @return: string
    '''

    return normal[::-1]
