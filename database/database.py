'''
Database
Created on 01/dic/2013

@package DropLan
@subpackage dir2xml
@version 0.5
@author 0x7c0 <0x7c0@teboss.tk>
@copyright Copyright (c) 2013, 0x7c0
@license http://www.gnu.org/licenses/gpl.html GPL v3 License
'''


try:
    from os import walk
    from time import time
    from sqlite3 import connect, DatabaseError, IntegrityError
    # personal
    from database.xml import Xml
    from database.iterdir import IterDir
    from database.iterfile import IterFile
    from module.utility.utility import u_str_split, \
        u_file_copy, u_file_set_info, \
        u_dir_info, u_dir_join, u_dir_parent, u_dir_child, u_dir_abs
except ImportError as error:
    print('In %s cannot load required libraries: %s!' \
        % (__name__, error))
    raise Exception


#==============================================================================
# constant
#==============================================================================
XML_DI = 'dir'
XML_FI = 'file'


#==============================================================================
# class
#==============================================================================
class DB(Xml):
    '''
    database class
    N.B. for single value to execute, use always list (now,)
    if Not normal, build database in RAM (':memory:)

    @param string dirs:    home pathname
    @param string file:    where database is located
    @param time time:    last time the database was updated
    @return: object
    '''

    def __init__(self, dirs, file=None, tim=time()):
        # initialize inherit class
        Xml.__init__(self)

        self._dir = dirs
        self._file = file

        self.time = int(tim)
        self.base = u_dir_child(dirs)

        # isolation_level for commit
        # check_same_thread for multithreading
        # cached_statements for cache
        self.__conn = connect(file, timeout=2, isolation_level='IMMEDIATE', \
                              check_same_thread=False, cached_statements=255)
        self.cursor = self.__conn.cursor()
        # avoiding dots optimization
        self.__exec = self.cursor.execute
        self.__exem = self.cursor.executemany
        self.__one = self.cursor.fetchone
        self.__all = self.cursor.fetchall

    def __del__(self):
        '''
        destructor  for class
        shutdown DB

        @return: void
        '''

        try:
            self.cursor.close()
        except AttributeError:
            pass
        return

    def run(self):
        '''
        run code

        @return: void
        '''

        if(not self.init_controll()):
            self.__init_install()

    #==========================================================================
    # init
    #==========================================================================
    def init_quit(self):
        '''
        standard shutdown database

        @return: quit()
        '''

        self._init_release()
        return

    def init_controll(self):
        '''
        check data integrity
        self.dir must be equal to head in DB

        @return: boolean
        '''

        try:
            # default already install
            self.__exec('SELECT Id FROM DIR WHERE Id=1 \
                                    AND Parent=0 AND Name=?;', (self.base,))
            if (int(self.__one()[0]) == 1):
                return True
            else:
                return False
        except DatabaseError:
            return False

    def __init_install(self):
        '''
        build database structure

        @return: void
        '''

        self.__exec('''
                        CREATE TABLE DIR(
                            Id INTEGER PRIMARY KEY,
                            Name TEXT NOT NULL,
                            Parent INTEGER REFERENCES DIR(Id)
                                ON DELETE NO ACTION
                                ON UPDATE CASCADE,
                            Atime INTEGER(10) NOT NULL,
                            Mtime INTEGER(10) NOT NULL,
                            Lin INTEGER(1) NOT NULL DEFAULT 0,
                            Ltime INTEGER(10) DEFAULT 0,
                            Del INTEGER(1) NOT NULL DEFAULT 0,
                            Dtime INTEGER(10) DEFAULT 0,
                            Obsolete INTEGER(1) NOT NULL DEFAULT 0,
                        UNIQUE(Parent, Name)
                        );
                        ''')
        self.__exec('''
                        CREATE TABLE FILE(
                            Id INTEGER PRIMARY KEY,
                            Name TEXT NOT NULL,
                            Parent INTEGER REFERENCES DIR(Id)
                                ON DELETE NO ACTION
                                ON UPDATE CASCADE,
                            Size INTEGER NOT NULL,
                            Atime INTEGER(10) NOT NULL,
                            Mtime INTEGER(10) NOT NULL,
                            Hash TEXT(32) DEFAULT NULL,
                            Lin INTEGER(1) NOT NULL DEFAULT 0,
                            Ltime INTEGER(10) DEFAULT 0,
                            Del INTEGER(1) NOT NULL DEFAULT 0,
                            Dtime INTEGER(10) DEFAULT 0,
                            Obsolete INTEGER(1) NOT NULL DEFAULT 0,
                            UNIQUE(Parent, Name)
                        );
                        ''')
#         self.__exec( 'CREATE VIEW V_LIN_DIR  AS SELECT Id,Name,Parent,\
# Atime,Mtime FROM DIR WHERE Lin=0 AND Obsolete=0;' )    # view sincro dir
#         self.__exec( 'CREATE VIEW V_LIN_FILE AS SELECT Id,Name,Parent,\
# Size,Atime,Mtime,Hash FROM FILE WHERE Lin=0 AND Obsolete=0;' )    # view file
#         self.__exec( 'CREATE VIEW V_DEL_DIR  AS SELECT Id,Name,Parent\
#  FROM DIR WHERE Del=1 AND Obsolete=0;' )    # view delete dir
#         self.__exec( 'CREATE VIEW V_DEL_FILE AS SELECT Id,Name,Parent\
#  FROM FILE WHERE Del=1 AND Obsolete=0;' )    # view delete file

        self._add_single((self.base, 0), u_dir_info(self._dir), False)

        self.__loop_insert()
        return

    def _init_release(self):
        '''
        commit after every transaction

        @return: void
        '''

        self.__conn.commit()    # release transaction
        self.time = int(time())
        return

    #==========================================================================
    # add
    #==========================================================================
    def _add_single(self, who, info, file=True, ash=None):
        '''
        add single file/directory to database
        VALUES ("%s", "%i", "%i", "%i"); #dir
        VALUES ("%s","%i","%i","%i","%i","%s"); #file

        # inside who
        string name:    name of directory/file
        integer parent:    id of upper directory

        @param tuple who:    name and id of dirs
        @param tuple info:    (size) + access/modification time
        @param boolean file:    if database file or not
        @param string ash:    hash of file
        @return: boolean
        '''

        name = str(who[0])
        parent = int(who[1])
        ash = str(ash)
        try:
            if(file):
                self.__exec('INSERT INTO FILE\
                (Name,Parent,Size,Atime,Mtime,Hash) \
                VALUES (?,?,?,?,?,?);', \
                (name, parent, int(info[0]), int(info[1]), int(info[2]), ash))
            else:
                self.__exec('INSERT INTO DIR\
                (Name,Parent,Atime,Mtime) \
                VALUES (?,?,?,?);', \
                (name, parent, int(info[0]), int(info[1])))
            return True
        except IntegrityError:
            if(file):
                self.__exec('SELECT Id FROM FILE WHERE Name=? AND Parent=?;',
                            (name, parent))
                upd = int(self.__one()[0])
                self._upd_restore(upd)
                self._upd_update(upd, info, ash=ash)
            else:
                self.__exec('SELECT Id FROM DIR WHERE Name=? AND Parent=?;', \
                            (name, parent))
                upd = int(self.__one()[0])
                self._upd_restore(upd, False)
                self._upd_update(upd, info, False)
            return True
        except PermissionError:
            pass
        except DatabaseError:
            return False

    def _add_multiple(self, iterator, file=True):
        '''
        add files/directories to database
        VALUES ("%s", "%i", "%i", "%i");    #dir
        VALUES ("%s","%i","%i","%i","%i","%s");    #file
        RECURSIVE

        @param iterator iterator:    builded iterator
        @param boolean file:    if database file or not
        @return: boolean
        '''

        try:
            if(file):
                self.__exem('INSERT INTO FILE\
                (Name,Parent,Size,Atime,Mtime,Hash) \
                VALUES (?,?,?,?,?,?);', iterator)
            else:
                self.__exem('INSERT INTO DIR\
                (Name,Parent,Atime,Mtime) VALUES \
                (?,?,?,?);', iterator)
            return True
        except IntegrityError:
            iterator.count -= 1    # rewind
            tmp = next(iterator)
            if(file):
                self.__exec('SELECT Id FROM FILE WHERE Name=? AND Parent=?;', \
                            (str(tmp[0]), int(tmp[1])))
                upd = int(self.__one()[0])
                self._upd_update(upd, (tmp[2], tmp[3], tmp[4]), ash=tmp[5])
            else:
                self.__exec('SELECT Id FROM DIR WHERE Name=? AND Parent=?;', \
                            (str(tmp[0]), int(tmp[1])))
                upd = int(self.__one()[0])
                self._upd_update(upd, (tmp[2], tmp[3]), False)
            self._upd_restore(upd, file)
            self._add_multiple(iterator, file)
        except PermissionError:
            pass
        except DatabaseError:
            return False

    def _add_path(self, idd, file=False, asb=True):
        '''
        return a path of a directory/file
        RECURSIVE

        @param integer idd:    database id
        @param boolean file:    if database file or not
        @param boolean asb:    if build with absolute root
        @return: string
        '''

        if(file):
            self.__exec('SELECT Name,Parent FROM FILE WHERE Id=?;', (idd,))
            file = not file
        else:
            self.__exec('SELECT Name,Parent FROM DIR WHERE Id=?;', (idd,))
        try:
            f_tuple = self.__one()
            f_parent = int(f_tuple[1])
            if(f_parent == 0):    # mom I'm home
                if(asb):
                    return self._dir
                else:
                    return ''
            else:
                return u_dir_join(self._add_path(f_parent, file, asb), \
                                  f_tuple[0])    # loop
        except TypeError:
            raise IOError

    def _add_parent(self, lst, parent=1):
        '''
        return a path of a directory/file
        RECURSIVE

        @param list lst:    hierarchy list of dir
        @param integer parent:    id of parent dir
        @return: string
        '''

        try:
            self.__exec('SELECT Id FROM DIR WHERE Name=? AND Parent=?;', \
                        (lst[0], parent))
            f_id = int(self.__one()[0])
            del(lst[0])
            if(len(lst) > 0):
                return self._add_parent(lst, f_id)    # __loop
            else:
                return f_id
        except TypeError:
            return parent
        except IndexError:    # if return None because no dir inside pathname
            return parent

    #==========================================================================
    # update
    #==========================================================================
    def _upd_update(self, idd, info, file=True, ash=None):
        '''
        add single file/directory to database
        VALUES ("%i", "%i", "%i");    #dir
        VALUES ("%i","%i","%i","%i","%s");    #file

        @param integer idd:    database id
        @param tuple info:    access/modification time
        @param boolean file:    if database file or not
        @param string ash:    hash of file
        @return: boolean
        '''

        idd = int(idd)
        try:
            if(file):
                self.__exec('UPDATE FILE SET \
                Size=?,Atime=?,Mtime=?,Hash=? \
                WHERE Id=?;', \
                (int(info[0]), int(info[1]), int(info[2]), str(ash), idd))
            else:
                self.__exec('UPDATE DIR SET \
                Atime=?,Mtime=? \
                WHERE Id=?;', \
                (int(info[0]), int(info[1]), idd))
            return True
        except PermissionError:
            pass
        except DatabaseError:
            return False

    def _upd_restore(self, idd, file=True):
        '''
        restore flag of file/directory

        @param integer idd:    database id
        @param boolean file:    if database file or not
        @return: boolean
        '''

        idd = int(idd)
        try:
            if(file):
                self.__exec('UPDATE FILE SET Del=0,Obsolete=0 WHERE Id=?;', \
                            (idd,))
            else:
                self.__exec('UPDATE DIR SET Del=0,Obsolete=0 WHERE Id=?;', \
                            (idd,))
            return True
        except DatabaseError:
            return False

    def _upd_obsolete(self, idd, file=True):
        '''
        set obsolete flag of file/directory

        @param integer idd:    database id
        @param boolean file:    if database file or not
        @return: boolean
        '''

        idd = int(idd)
        try:
            if(file):
                self.__exec('UPDATE FILE SET Obsolete=1 WHERE Id=?;', (idd,))
            else:
                self.__exec('UPDATE DIR SET Obsolete=1 WHERE Id=?;', (idd,))
            return True
        except DatabaseError:
            return False

    def _upd_multiple_obsolete(self, iterator, file=True):
        '''
        set obsolete flag of file/directory

        @param iterator iterator:    builded iterator
        @param boolean file:    if database file or not
        @return: boolean
        '''

        try:
            if(file):
                self.__exem('UPDATE FILE SET Obsolete=1 WHERE Id=?;', \
                                   iterator)
            else:
                self.__exem('UPDATE DIR SET Obsolete=1 WHERE Id=?;', \
                                   iterator)
            return True
        except PermissionError:
            pass
        except DatabaseError:
            return False

    def _upd_link(self, idd, file=True):
        '''
        set lin flag of file/directory

        @param integer idd:    database id
        @param boolean file:    if database file or not
        @return: boolean
        '''

        idd = int(idd)
        try:
            if(file):
                self.__exec('UPDATE FILE SET Lin=1,Ltime=? WHERE Id=?;', \
                            (self.time, idd))
            else:
                self.__exec('UPDATE DIR SET Lin=1,Ltime=? WHERE Id=?;', \
                            (self.time, idd))
            return True
        except DatabaseError:
            return False

    def _upd_delete(self, idd, file=True):
        '''
        set delete flag of file/directory

        @param integer idd:    database id
        @param boolean file:    if database file or not
        @return: boolean
        '''

        idd = int(idd)
        try:
            if(file):
                self.__exec('UPDATE FILE SET Del=1,Dtime=? WHERE Id=?;', \
                            (self.time, idd))
            else:
                self.__exec('UPDATE DIR SET Del=1,Dtime=? WHERE Id=?;', \
                            (self.time, idd))
            return True
        except DatabaseError:
            return False

    def _upd_multiple_delete(self, iterator, file=True, idd=False):
        '''
        set file/directory to be deleted

        @param iterator iterator:    builded iterator
        @param boolean file:    if database file or not
        @param boolean idd:    if UPDATE with Id param
        @return: boolean
        '''

        try:
            if(idd):
                if(file):
                    self.__exem('UPDATE FILE SET Del=1 WHERE Id=?;', \
                                       iterator)
                else:
                    self.__exem('UPDATE DIR SET Del=1 WHERE Id=?;', \
                                       iterator)
            else:
                if(file):
                    self.__exem('UPDATE FILE \
                                        SET Del=1,Dtime=? \
                                        WHERE Name=? AND Parent=?;', \
                                        iterator)
                else:
                    self.__exem('UPDATE DIR \
                                        SET Del=1,Dtime=? \
                                        WHERE Name=? AND Parent=?;', \
                                        iterator)
            return True
        except DatabaseError:
            return False

    def _upd_ko(self, lst, file=True):
        '''
        set delete flag + obsolete flag to a list of file/directory

        @param list lst:    parsed list
        @param boolean file:    if database file or not
        @return: void
        '''

        if(len(lst) > 0):
            lst = iter(lst)    # create iterator for multiple fx
            self._upd_multiple_delete(lst, file, True)
            self._upd_multiple_obsolete(lst, file)
            self._init_release()
        return

    def _upd_ok(self, data):
        '''
        add info received by source
        data = [pathname, size, atime, mtime, hash, file_counter, same_ash]

        @param list data:    file info
        @return: void
        '''

        name = u_dir_child(data[0])
        parent = self._add_parent(u_str_split(data[0]))
        self._add_single((name, parent), (data[1], data[2], data[3]), True, \
                       ash=data[4])
        self._init_release()
        return

    #==========================================================================
    # stat
    #==========================================================================
    def sta_is_empty(self, idd):
        '''
        return true if empty dir

        @param integer idd:    database id
        @return: boolean
        '''

        try:
            self.__exec('SELECT Id FROM FILE WHERE Parent=?;', (idd,))
            if (int(self.__one()[0])):
                return False
            else:
                return True
        except DatabaseError:
            return True

    def sta_count(self, file=True):
        '''
        return number of the directories/files

        @param boolean file:    if database file or not
        @return: integer
        '''

        if(file):
            self.__exec('SELECT COUNT(*) FROM FILE;')
        else:
            self.__exec('SELECT COUNT(*) FROM DIR;')
        return int(self.__one()[0])

    def sta_count_no_link(self, file=True):
        '''
        return number of the directories/files to be synchronized

        @param boolean file:    if database file or not
        @return: integer
        '''

        if(file):
            self.__exec('SELECT COUNT(*) FROM FILE WHERE Lin=0;')
        else:
            self.__exec('SELECT COUNT(*) FROM DIR WHERE Lin=0;')
        return int(self.__one()[0])

    def sta_count_link(self, file=True):
        '''
        return number of the directories/files already synchronized

        @param boolean file:    if database file or not
        @return: integer
        '''

        if(file):
            self.__exec('SELECT COUNT(*) FROM FILE WHERE Lin=1;')
        else:
            self.__exec('SELECT COUNT(*) FROM DIR WHERE Lin=1;')
        return int(self.__one()[0])

    def sta_count_del(self, file=True):
        '''
        return number of the directories/files to be deleted

        @param boolean file:    if database file or not
        @return: integer
        '''

        if(file):
            self.__exec('SELECT COUNT(*) FROM FILE WHERE Del=1;')
        else:
            self.__exec('SELECT COUNT(*) FROM DIR WHERE Del=1;')
        return int(self.__one()[0])

    def sta_count_obs(self, file=True):
        '''
        return number of obsolete directories/files

        @param boolean file:    if database file or not
        @return: integer
        '''

        if(file):
            self.__exec('SELECT COUNT(*) FROM FILE WHERE Obsolete=1;')
        else:
            self.__exec('SELECT COUNT(*) FROM DIR WHERE Obsolete=1;')
        return int(self.__one()[0])

    def sta_hash(self, data):
        '''
        return True if already get same file
        and copy it
        data = [pathname, size, atime, mtime, hash, file_counter, same_ash]

        @param list data:    file info
        @return: boolean
        '''

        self.__exec('SELECT Id FROM FILE WHERE Hash=?;', (data[4],))
        try:
            ide = int(self.__one()[0])    # if not found raise TypeError
            # build with DIR absolute pathname, and relative source pathname
            root = u_dir_join(self._dir, u_dir_abs(data[0]))
            frm = self._add_path(ide, True)    # get pathname of source
            # new transaction. do it rollback if shit happens
            self.__conn.commit()
            self._upd_restore(ide)
            u_file_copy(frm, root)    # copy file with same hash
            u_file_set_info(root, data[2], data[3])
            return True
        except IOError:    # error copy other file
            self.__conn.rollback()    # restore previous value
            return False
        except TypeError:
            return False

    #==========================================================================
    # loop
    #==========================================================================
    def __loop_insert(self):
        '''
        __loop for insert data into database
        only after Install
        always use Update

        @return: void
        '''

        # init
        cache = {u_dir_parent(self._dir): 0}
        ide = 0

        for root, dirs, files in walk(self._dir):

            # intro
            tail = u_dir_child(root)
            self.__exec('SELECT Id FROM DIR WHERE Name=? AND Parent=?;', \
                        (tail, cache[u_dir_parent(root)]))
            ide = int(self.__one()[0])
            cache[root] = ide    # add id path to cache

            # dir execution
            if(len(dirs) > 0):    # dir execution
                itd = IterDir((self.time, root), ide, dirs)
                self._add_multiple(itd, False)

            # file execution
            if(len(files) > 0):    # file execution
                itd = IterFile((self.time, root), ide, files)
                self._add_multiple(itd)

        self._init_release()
        return

    def __loop_info(self, record):
        '''
        update database with info returned from source
        record = ( koDir, okFile, koFile)
        return info list about file to been sended
        data = [pathname, size, atime, mtime, hash, file_counter, same_ash]


        @param list record:    info from source
        @return: list
        '''

        data = []
        data_a = data.append
        counter = 0

        for okk in record:    # id of accepted files
            self.__exec('SELECT Size,Atime,Mtime,Hash FROM FILE WHERE Id=?;', \
                        (okk,))
            info = self.__one()
            try:
                try_it = info[3]
                try:
                    self.__exec('SELECT Id FROM FILE WHERE Hash=?;', \
                                (try_it,))
                    same = int(self.__one()[0])
                except TypeError:
                    same = 0
                pathname = self._add_path(okk, True, False)
                data_a([pathname, info[0], info[1], info[2], \
                        try_it, counter, same])
                counter += 1
            except TypeError:
                pass
        return data

    #==========================================================================
    # xml
    #==========================================================================
    def xml_build(self):
        '''
        build XML tree

        @return: xml
        '''

        head = Xml.Element('home')
        tree = self._xml_build_loop(head, 1)    # 1 is id of Home
        self._init_release()
        Xml._xml_build_info(self, head, tree)
        return Xml.ElementTree(head)

    def _xml_build_loop(self, tree, ide):
        '''
        build branch of every directory
        RECURSIVE

        @param xml tree:    upper branch
        @param integer ide:    id of upper dir
        @return: list
        '''

        info = [0, 0, 0, 0]    # dir, file, new, del counter
        _set = '1'    # set flag to xml
        branch = None    # subelement of tree
        sub = None    # subelement of branch
        tupl_ = None    # list returned from loop
        sql_id = 0
        sql_del = 0
        sql_dtime = ''
        sql_lin = 0

        # build dir
        self.__exec('SELECT Id,Name,Mtime,Del,Dtime,Lin FROM DIR \
                    WHERE Obsolete=0 AND Parent=?;', \
                    (ide,))
        sql_get = self.__all()
        for lop in sql_get:
            sql_id = int(lop[0])
            sql_del = int(lop[3])
            sql_dtime = str(lop[4])
            sql_lin = int(lop[5])
            self._upd_link(sql_id, False)    # update link flag

            # build branch
            branch = Xml.SubElement(tree, XML_DI, {'id': str(sql_id)})
            info[0] += 1
            Xml._xml_build_dir(branch, lop)
            if(sql_del):
                branch.attrib['del'] = _set
                sub = Xml.SubElement(branch, 'dtime')
                sub.text = sql_dtime
                info[3] += 1
            elif(not sql_lin):
                branch.attrib['new'] = _set
                sub = Xml.SubElement(branch, 'ltime')
                sub.text = str(self.time)
                info[2] += 1

            # __loop
            tupl_ = self._xml_build_loop(branch, sql_id)
            info[0] += tupl_[0]    # update dir
            info[1] += tupl_[1]    # update file
            info[2] += tupl_[2]    # update new
            info[3] += tupl_[3]    # update del

        # build files
        self.__exec('SELECT Id,Name,Atime,Mtime,Hash,Del,Dtime,Lin FROM FILE \
                    WHERE Obsolete=0 AND Parent=?;', \
                    (ide,))
        sql_get = self.__all()
        for lop in sql_get:
            sql_id = int(lop[0])
            sql_del = int(lop[5])
            sql_dtime = str(lop[6])
            sql_lin = int(lop[7])
            self._upd_link(sql_id)    # update link flag

            # build tree
            branch = Xml.SubElement(tree, XML_FI, {'id': str(sql_id)})
            info[1] += 1
            Xml._xml_build_file(branch, lop)
            if(sql_del):
                branch.attrib['del'] = _set
                sub = Xml.SubElement(branch, 'dtime')
                sub.text = sql_dtime
                info[3] += 1
            elif(not sql_lin):
                branch.attrib['new'] = _set
                sub = Xml.SubElement(branch, 'ltime')
                sub.text = str(self.time)
                info[2] += 1

        return info
