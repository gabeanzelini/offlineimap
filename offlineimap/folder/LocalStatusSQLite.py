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
    """LocalStatus backend implemented with an SQLite database"""
    def __deinit__(self):
        #TODO, need to invoke this when appropriate?
        self.save()
        self.cursor.close()
        self.connection.close()

    def __init__(self, root, name, repository, accountname, config):
        super(LocalStatusSQLiteFolder, self).__init__(root, name, repository, accountname, config)
        
        self.dblock = RLock()
        """dblock protects against concurrent forbidden access of the db
        object, e.g trying to test for existence and on-demand creation
        of the db."""

        #Try to establish connection
        try:
            self.connection = sqlite.connect(self.filename)
        except NameError:
            # sqlite import had failed
            raise UserWarning('SQLite backend chosen, but no sqlite python '
                              'bindings available. Please install.')

        #Test if the db version is current enough and if the db is
        #readable.  Lock, so that only one thread at a time can do this,
        #so we don't create them in parallel.
        with self.dblock:
            try:
                self.cursor = self.connection.cursor()
                self.cursor.execute('SELECT version from metadata')
            except sqlite.DatabaseError:
                #db file missing or corrupt, recreate it.
                self.create_db()

    def isnewfolder(self):
        # testing the existence of the db file won't work. It is created
        # as soon as this class instance was intitiated. So say it is a
        # new folder when there are no messages at all recorded in it.
        return self.getmessagecount() > 0

    def create_db(self):
        """Create a new db file"""
        self.ui.warn('Creating new Local Status db for %s:%s' \
                         % (self.repository.getname(), self.getname()))
        self.connection = sqlite.connect(self.filename)
        self.cursor = self.connection.cursor()
        self.cursor.execute('CREATE TABLE metadata (key VARCHAR(50) PRIMARY KEY, value VARCHAR(128))')
        self.cursor.execute("INSERT INTO metadata VALUES('db_version', '1')")
        self.cursor.execute('CREATE TABLE status (id INTEGER PRIMARY KEY, flags VARCHAR(50))')
        self.autosave() #commit if needed

    def deletemessagelist(self):
        """delete all messages in the db"""
        self.cursor.execute('DELETE FROM status')

    def cachemessagelist(self):
        self.messagelist = {}
        self.cursor.execute('SELECT id,flags from status')
        for row in self.cursor:
            flags = [x for x in row[1]]
            self.messagelist[row[0]] = {'uid': row[0], 'flags': flags}

    def save(self):
        self.connection.commit()

    def getmessagelist(self):
        return self.messagelist

    def uidexists(self,uid):
        self.cursor.execute('SELECT id FROM status WHERE id=:id',{'id': uid})
        for row in self.cursor:
            if(row[0]==uid):
                return 1
        return 0

    def getmessageuidlist(self):
        self.cursor.execute('SELECT id from status')
        r = []
        for row in self.cursor:
            r.append(row[0])
        return r

    def getmessagecount(self):
        self.cursor.execute('SELECT count(id) from status');
        row = self.cursor.fetchone()
        return row[0]

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
        self.cursor.execute('INSERT INTO status (id,flags) VALUES (?,?)',
                            (uid,flags))
        self.autosave()
        return uid

    def getmessageflags(self, uid):
        self.cursor.execute('SELECT flags FROM status WHERE id=:id',
                            {'id': uid})
        for row in self.cursor:
            flags = [x for x in row[0]]
            return flags
        assert False,"getmessageflags() called on non-existing message"

    def getmessagetime(self, uid):
        return self.messagelist[uid]['time']

    def savemessageflags(self, uid, flags):
        self.messagelist[uid] = {'uid': uid, 'flags': flags}
        flags.sort()
        flags = ''.join(flags)
        self.cursor.execute('UPDATE status SET flags=? WHERE id=?',(flags,uid))
        self.autosave()

    def deletemessages(self, uidlist):
        # Weed out ones not in self.messagelist
        uidlist = [uid for uid in uidlist if uid in self.messagelist]
        if not len(uidlist):
            return

        for uid in uidlist:
            del(self.messagelist[uid])
            #if self.uidexists(uid):
            self.cursor.execute('DELETE FROM status WHERE id=:id', {'id': uid})
