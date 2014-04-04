'''
xml building and parsing
Created on 27/mar/2014

@package Dir2Xml
@subpackage database
@version 0.5
@author 0x7c0 <0x7c0@teboss.tk>
@copyright Copyright (c) 2013, 0x7c0
@license http://www.gnu.org/licenses/gpl.html GPL v3 License
'''


try:
    from abc import ABC, abstractmethod
    from xml.etree.ElementTree import Element, ElementTree, SubElement
    # personal
except ImportError as error:
    print('In %s cannot load required libraries: %s!' \
        % (__name__, error))
    raise Exception


#==============================================================================
# class
#==============================================================================
class Xml(ABC):
    '''
    class for extend method to under class
    like database.database

    @return: object
    '''

    Element = Element
    ElementTree = ElementTree
    SubElement = SubElement

    def __init__(self):
        # override
        self.time = None
        self.log = None
        self.base = None

#==============================================================================
# override
#==============================================================================
    @abstractmethod
    def _init_release(self):
        '''
        see database.database
        '''

        raise NotImplementedError

    @abstractmethod
    def sta_count(self, abc=None):
        '''
        see database.database
        '''

        raise NotImplementedError

    @abstractmethod
    def sta_count_link(self, abc=None):
        '''
        see database.database
        '''
        raise NotImplementedError

    @abstractmethod
    def sta_count_del(self, abc=None):
        '''
        see database.database
        '''
        raise NotImplementedError

    @abstractmethod
    def sta_count_obs(self, abc=None):
        '''
        see database.database
        '''

        raise NotImplementedError

#==============================================================================
# database added
#==============================================================================
    @staticmethod
    def _xml_build_dir(branch, info):
        '''
        build branch of directory

        @param xml branch:    upper branch
        @param list info:    info returned from database
        @return: void
        '''

        leaf = SubElement(branch, 'name')
        leaf.text = info[1]
#         leaf = ET.SubElement( branch, 'atime' )
#         leaf.text = str( info[2] )
        leaf = SubElement(branch, 'mtime')
        leaf.text = str(info[2])
        return

    @staticmethod
    def _xml_build_file(branch, info):
        '''
        build branch of file

        @param xml branch:    upper branch
        @param list info:    info returned from database
        @return: void
        '''

        leaf = SubElement(branch, 'name')
        leaf.text = info[1]
        leaf = SubElement(branch, 'mtime')
        leaf.text = str(info[3])
        leaf = SubElement(branch, 'atime')
        leaf.text = str(info[2])
#         leaf = ET.SubElement( branch, 'size' )
#         leaf.text = str( info[2] )
        leaf = SubElement(branch, 'hash')
        leaf.text = info[4]
        return

    def _xml_build_info(self, head, lst):
        '''
        build branch of file
        other functions are extended by inheritance

        @param xml head:        upper branch
        @param list lst:            info returned from database
        @return: void
        '''

        info = SubElement(head, 'info',)    # build branch

        branch = SubElement(info, 'time',)
        try:
            branch.text = str(self.time)    # override
        except AttributeError:
            branch.text = '0'

        branch = SubElement(info, 'xml',)    # build under branch
        leaf = SubElement(branch, 'dir')
        leaf.text = str(lst[0])
        leaf = SubElement(branch, 'file')
        leaf.text = str(lst[1])
        leaf = SubElement(branch, 'new')
        leaf.text = str(lst[2])
        leaf = SubElement(branch, 'del')
        leaf.text = str(lst[3])

        branch = SubElement(info, 'sql',)    # build under branch
        leaf = SubElement(branch, 'dir')
        leaf.text = str(self.sta_count(False) - 1)    # -1 home
        leaf = SubElement(branch, 'file')
        leaf.text = str(self.sta_count())
        leaf = SubElement(branch, 'link')
        lst = self.sta_count_link()
        lst += self.sta_count_link(False)
        leaf.text = str(lst)
        leaf = SubElement(branch, 'del')
        lst = self.sta_count_del()
        lst += self.sta_count_del(False)
        leaf.text = str(lst)
        leaf = SubElement(branch, 'obs')
        leaf.text = str(self.sta_count_obs())
        return
