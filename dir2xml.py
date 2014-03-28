#!/usr/bin/python3.4
# -*- coding: utf-8 -*-
'''
Main File
Created on 27/mar/2014

@package Dir2Xml
@subpackage main
@version 0.5
@author 0x7c0 <0x7c0@teboss.tk>
@copyright Copyright (c) 2013, 0x7c0
@license http://www.gnu.org/licenses/gpl.html GPL v3 License
'''


# check version
from sys import version_info
if(version_info[0] < 3):
    print('must use Python 3 or greater')
    quit()
del version_info


try:
    from time import time
    from argparse import ArgumentParser, ArgumentTypeError
    # personal
    from database.database import DB
    from module.utility.utility import u_dir_abs, u_dir_exists, \
                                        u_file_exists, u_user_input
    from module.utility.simple import s_ut_crono, s_temporary_db
except ImportError as error:
    print('In %s cannot load required libraries: %s!' \
        % (__name__, error))
    quit()


VERSION = 0.5
NAME = 'Dir2Xml'


if __name__ == '__main__':

    def m_dir(root_dir):
        '''
        type for argparse

        @param string root_dr:    path of file
        @return string
        '''

        roo = u_dir_abs(root_dir)
        if(not u_dir_exists(roo)):
            raise ArgumentTypeError('Directory not found!')
        else:
            return roo

    def m_quit(bye=True):
        '''
        close program

        @param bool bye:    if print bye
        @return quit
        '''

        if(bye):
            print('Bye!')
        quit()

    PARSER = ArgumentParser(description='Run %s' % (NAME,))
    PARSER.add_argument('-v', '--version', action='version', \
                        version='%s version %s' % (NAME, VERSION,))
    PARSER.add_argument('-d', metavar='Dir', nargs=1, type=m_dir, \
                        help='insert path of your directory', required=True)
    GROUP_1 = PARSER.add_argument_group(title='optional parameters')
    GROUP_1.add_argument('-n', metavar='Name', nargs=1, type=str, \
                         help='set name of your xml', default=['dir2.xml'])
    ARGS = PARSER.parse_args()
    NAME = ARGS.n[0]
    DIR = ARGS.d[0]

    try:
        print('crunching "%s" directory...' % DIR)
        if(u_file_exists(NAME)):
            if(not u_user_input(\
                r'new xml file already exist, do you wanna proceed? [Y/N] ')):
                m_quit()
        START = time()

        DBS = DB(DIR, s_temporary_db())
        DBS.run()
        XML = DBS.xml_build()
        XML.write(NAME)
        del XML

        if(u_file_exists(NAME)):
            print('xml generated in %s' % s_ut_crono(START))
        else:
            print('xml not generated')
        m_quit(False)

    except KeyboardInterrupt:    # Ctrl + C
        pass
    except Exception:    # remove for develop
        pass
    m_quit()
