'''
Iterator for directories
Created on 27/mar/2014

@package Dir2Xml
@subpackage database
@version 0.5
@author 0x7c0 <0x7c0@teboss.tk>
@copyright Copyright (c) 2013, 0x7c0
@license http://www.gnu.org/licenses/gpl.html GPL v3 License
'''


try:
    # personal
    from module.utility.utility import u_dir_join, u_dir_info
except ImportError as error:
    print('In %s cannot load required libraries: %s!' \
        % (__name__, error))
    raise Exception


#==============================================================================
# class
#==============================================================================
class IterDir(object):
    '''
    iterator

    # inside sett
    time time:    starting time
    string absolute:    absolute path of upper directory

    @param tuple sett:    setting list
    @param integer parent:    id of upper directory
    @param list inp:    list of directories
    @param string typ:    type of output
    @return: iterator
    '''

    count = 0
    stop = 0

    def __init__(self, sett, parent, inp, typ='add'):
        self.now = sett[0]
        self.abs = sett[1]
        self.idd = parent
        self.list = inp
        self.reset()
        if(typ == 'add'):
            self.std = self.__r_add
        elif(typ == 'id'):
            self.std = self.__r_id
        elif(typ == 'mod'):
            self.std = self.__r_mod

    def __iter__(self):
        '''
        build iterator

        @return: iterator
        '''

        return self

    def __next__(self):
        '''
        build tuple
        return name, parent's id, atime, gtime    #add
        return time, name, parent's id    #id

        @return: tuple
        '''

        self.count += 1
        if(self.count > self.stop):
            raise StopIteration
        ext = self.list[self.count - 1]
        root = u_dir_join(self.abs, ext)
        return self.std(ext, root)

    def reset(self):
        '''
        reset iterator

        @return: void
        '''

        self.count = 0
        self.stop = len(self.list)
        return

    #==========================================================================
    # remapped
    #==========================================================================
    def __r_add(self, ext, root=None):
        '''
        standard function
        return info to be added into database
        REMAP

        @param string ext:    name
        @param string root:    pathname
        @return: tuple
        '''

        root = u_dir_info(root)
        return ext, self.idd, root[0], root[1]

    def __r_id(self, ext, root=None):
        '''
        standard function
        return time, name and id of parent
        REMAP

        @param string ext:    name
        @param string root:    pathname
        @return: tuple
        '''

        del root
        return self.now, ext, self.idd

    @staticmethod
    def __r_mod(ext, root=None):
        '''
        standard function
        return name and mtime
        REMAP

        @param string ext:    name
        @param string root:    pathname
        @return: tuple
        '''

        del ext
        return u_dir_info(root, True)
