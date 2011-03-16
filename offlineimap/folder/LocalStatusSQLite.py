# Local status cache virtual folder: SQLite backend
# Copyright (C) 2009-2011 Stewart Smith and contributors
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program; if not, write to the Free Software
#    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

from LocalStatus import LocalStatusFolder
from threading import RLock

try:
    from pysqlite2 import dbapi2 as sqlite
except:
    pass #fail only when needed later on

class LocalStatusSQLiteFolder(LocalStatusFolder):
    """LocalStatus backend implemented with an SQLite database

    As python-sqlite currently does not allow to access the same sqlite
    objects from various threads, we need to open get and close a db
    connection and cursor for all operations. This is a big disadvantage
    and we might want to investigate if we cannot hold an object open
    for a thread somehow."""
    def __deinit__(self):
        #TODO, need to invoke this when appropriate?
        self.save()
        self.cursor.close()
        self.connection.close()

    def __init__(self, root, name, repository, accountname, config):
        super(LocalStatusSQLiteFolder, self).__init__(root, name, repository, accountname, config)
        
        #Try to establish connection, no need for threadsafety in __init__
        try:
            connection = sqlite.connect(self.filename)
        except NameError:
            # sqlite import had failed
            raise UserWarning('SQLite backend chosen, but no sqlite python '
                              'bindings available. Please install.')

        #Test if the db version is current enough and if the db is
        #readable.
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT value from metadata WHERE key='db_version'")
        except sqlite.DatabaseError as e:
            #db file missing or corrupt, recreate it.
            connection.close()
            self.create_db()
        connection.close()

    def get_cursor(self):
        """Return a db (connection, cursor) that we can use

        You are responsible for connection.commit() and
        connection.close() yourself. Connection close() happens
        automatically when the connection variable is destroyed
        though. According to sqlite docs, you need to commit() before
        the connection is closed or your changes will be lost!"""
        #get db connection which autocommits
        connection = sqlite.connect(self.filename, isolation_level=None)        
        cursor = connection.cursor()
        return connection, cursor

    def isnewfolder(self):
        # testing the existence of the db file won't work. It is created
        # as soon as this class instance was intitiated. So say it is a
        # new folder when there are no messages at all recorded in it.
        return self.getmessagecount() > 0

    def create_db(self):
        """Create a new db file"""
        self.ui.warn('Creating new Local Status db for %s:%s' \
                         % (self.repository.getname(), self.getname()))
        conn, cursor = self.get_cursor()
        with conn:
            cursor.execute('CREATE TABLE metadata (key VARCHAR(50) PRIMARY KEY, value VARCHAR(128))')
            cursor.execute("INSERT INTO metadata VALUES('db_version', '1')")
            cursor.execute('CREATE TABLE status (id INTEGER PRIMARY KEY, flags VARCHAR(50))')
            conn.commit()

    def deletemessagelist(self):
        """delete all messages in the db"""
        conn, cursor = self.get_cursor()
        with conn:
            cursor.execute('DELETE FROM status')
            conn.commit()

    def cachemessagelist(self):
        self.messagelist = {}
        conn, cursor = self.get_cursor()
        with conn:
            cursor.execute('SELECT id,flags from status')
            for row in cursor:
                flags = [x for x in row[1]]
                self.messagelist[row[0]] = {'uid': row[0], 'flags': flags}

    def save(self):
        """Noop in this backend"""
        pass

    def getmessagelist(self):
        return self.messagelist

    # Following some pure SQLite functions, where we chose to use
    # BaseFolder() methods instead. Doing those on the in-memory list is
    # quicker anyway. If our db becomes so big that we don't want to
    # maintain the in-memory list anymore, these might come in handy
    # though.
    #
    #def uidexists(self,uid):
    #    conn, cursor = self.get_cursor()
    #    with conn:
    #        cursor.execute('SELECT id FROM status WHERE id=:id',{'id': uid})
    #        return cursor.fetchone()
    # This would be the pure SQLite solution, use BaseFolder() method,
    # to avoid threading with sqlite...
    #def getmessageuidlist(self):
    #    conn, cursor = self.get_cursor()
    #    with conn:
    #        cursor.execute('SELECT id from status')
    #        r = []
    #        for row in cursor:
    #            r.append(row[0])
    #        return r
    #def getmessagecount(self):
    #    conn, cursor = self.get_cursor()
    #    with conn:
    #        cursor.execute('SELECT count(id) from status');
    #        return cursor.fetchone()[0]
    #def getmessageflags(self, uid):
    #    conn, cursor = self.get_cursor()
    #    with conn:
    #        cursor.execute('SELECT flags FROM status WHERE id=:id',
    #                        {'id': uid})
    #        for row in cursor:
    #            flags = [x for x in row[0]]
    #            return flags
    #        assert False,"getmessageflags() called on non-existing message"

    def savemessage(self, uid, content, flags, rtime):
        if uid < 0:
            # We cannot assign a uid.
            return uid

        if self.uidexists(uid):     # already have it
            self.savemessageflags(uid, flags)
            return uid

        self.messagelist[uid] = {'uid': uid, 'flags': flags, 'time': rtime}
        flags.sort()
        flags = ''.join(flags)
        conn, cursor = self.get_cursor()
        while True:
            try:
                cursor.execute('INSERT INTO status (id,flags) VALUES (?,?)',
                               (uid,flags))
            except sqlite.OperationalError as e:
                if e.args[0] != 'database is locked':
                    raise
            else: #success
                break    
        conn.commit()
        cursor.close()
        conn.close()
        return uid

    def savemessageflags(self, uid, flags):
        self.messagelist[uid] = {'uid': uid, 'flags': flags}
        flags.sort()
        flags = ''.join(flags)
        conn, cursor = self.get_cursor()
        with conn:
            cursor.execute('UPDATE status SET flags=? WHERE id=?',(flags,uid))
            conn.commit()

    def deletemessages(self, uidlist):
        # Weed out ones not in self.messagelist
        uidlist = [uid for uid in uidlist if uid in self.messagelist]
        if not len(uidlist):
            return

        conn, cursor = self.get_cursor()
        with conn:
            for uid in uidlist:
                del(self.messagelist[uid])
                cursor.execute('DELETE FROM status WHERE id=:id',
                               {'id': uid})
            conn.commit()
