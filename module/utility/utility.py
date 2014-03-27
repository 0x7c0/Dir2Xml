'''
Database
Common functions

@package Dir2Xml
@subpackage database
@version 0.5
@author 0x7c0 <0x7c0@teboss.tk>
@copyright Copyright (cursor) 2013, 0x7c0
@license http://www.gnu.org/licenses/gpl.html GPL v3 License
'''


try:
    from hashlib import md5
    from os import path, utime
    from shutil import copyfile
    # personal
    from module.utility.simple import s_str_reverse
except ImportError as error:
    print('In %s cannot load required libraries: %s!' \
        % (__name__, error))
    raise Exception


#==============================================================================
# dir functions
#==============================================================================
def u_dir_abs(root):
    '''
    check if path is absolute
    otherwise return abs path

    @param string root:    pathname
    @return: string
    '''

    if(path.isabs(root)):
        return root
    else:
        return path.abspath(root)


def u_dir_exists(root):    # Exists old
    '''
    check directory

    @param string root:    pathname
    @return: boolean
    '''

    return path.exists(root) and path.isdir(root)


def u_dir_join(rtwo, dtwo, absolute=False):
    '''
    join r2 path with d2 path (R2-D2)

    @param string rtwo:    pathname
    @param string dtwo:    pathaname
    @param boolean absolute:    if boolean build r2 absolute
    @return: string
    '''

    if(absolute):
        rtwo = u_dir_abs(rtwo)
    return path.join(rtwo, dtwo)


def u_dir_info(root, mod=False):
    '''
    return info from selected directory

    @param string root:    absolute pathname
    @param boolean mod:    if return only mtime
    @return: tuple
    '''

#    if( path.exists( root ) and path.isdir( root ) ):
    try:
        if(mod):
            return int(path.getmtime(root))
        else:
            return int(path.getatime(root)), int(path.getmtime(root))
    except FileNotFoundError:
        if(mod):
            return 0
        else:
            return 0, 0


def u_dir_parent(root):
    '''
    return upper dir from string
    root = r'D:\\Documenti\\Musica_flac\\Jon Hopkins\\Immunity.mp3'
    return r'D:\\Documenti\\Musica_flac\\Jon Hopkins'

    @param string root:    pathname
    @return: string
    '''

    return path.dirname(root)


def u_dir_child(root):
    '''
    return last dir from string
    root = r'D:\\Documenti\\Musica_flac\\Jon Hopkins\\Immunity.mp3'
    return r'Immunity.mp3'

    @param string root:    pathname
    @return: string
    '''

    return path.basename(root)


#==============================================================================
# file functions
#==============================================================================
def u_file_get_info(root, mod=False):
    '''
    return info from selected file

    @param string root:    absolute pathname
    @param boolean mod:    if return only mtime
    @return: tuple
    '''

#     if( path.exists( root ) and path.isfile( root ) ):
    try:
        if(mod):
            return int(path.getmtime(root))
        else:
            return int(path.getsize(root)), \
                   int(path.getatime(root)), \
                   int(path.getmtime(root))
    except FileNotFoundError:
        if(mod):
            return 0
        else:
            return 0, 0, 0


def u_file_set_info(root, atime, mtime):
    '''
    sett the access and modified times of the file specified by root

    @param string root:    file pathname
    @param integer atime:    access time
    @param integer mtime:    modified time
    @return: void
    '''

    if (u_file_exists(root)):
        utime(root, times=(atime, mtime))
        return
    else:
        raise OSError


def u_file_hash(root):
    '''
    return hash from selected file

    @param string root:    absolute pathname
    @return: string
    '''

    try:
        md_five = md5()
        with open(root, 'rb') as dig:
            blocksize = 128    # md5 128byte chunks
            hasher = md_five.update
            rread = dig.read
            buf = rread(blocksize)

            while(buf):
                hasher(buf)
                buf = rread(blocksize)
        return md_five.hexdigest()
    except PermissionError:
        print('file "%s" is blocked' % root)
        raise PermissionError


def u_file_exists(root):    # Exists old
    '''
    check file

    @param string root:    pathname
    @return: boolean
    '''

    return path.exists(root) and path.isfile(root)


def u_file_copy(src, dst):
    '''
    simple function to copy the contents of the src file
    to a file named dst

    @param string src:    pathname of source file
    @param string dst:    pathname of destination file
    @return: string
    '''

    try:
        if (u_file_exists(src)):
            return copyfile(src, dst, follow_symlinks=False)
        else:
            pass
    except PermissionError:
        pass
    raise OSError


def u_user_input(question):
    '''
    question about action
    yes or no question

    @param string question:        output question
    @return: bool
    '''

    while True:
        action = input(question)
        if(action.upper() == 'Y'):
            return True
        elif(action.upper() == 'N'):
            return False


#==============================================================================
# string functions
#==============================================================================
def u_str_split(root):
    '''
    slipt a pathname into a list of dir

    @param string root:        file pathname
    @return: list
    '''

    scraper = []
    scraper_a = scraper.append
    one = u_dir_child(root)
    while one != '':
        scraper_a(one)
        root = u_dir_parent(root)
        one = u_dir_child(root)
    del(scraper[0])    # remove last element because is a file
    return s_str_reverse(scraper)    # reverse to respect hierarchy
