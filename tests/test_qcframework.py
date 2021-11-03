import unittest
import os
import stat
from MockDBI import MockConnection
from despydb import desdbi

os.environ['DES_DB_SECTION'] = "db-test"
os.environ['DES_SERVICES'] = "services.ini"
import qcframework.Messaging as qmsg
import qcframework.qcfdb as qcfdb
import qcframework.Search as qsrch

class Test_Messaging(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.logfile = 'Test.log'
        cls.execname = 'Testexec'
        cls.pfwid = 12345
        cls.taskid = 56789
        cls.sfile = 'services.ini'
        cls.files = [cls.sfile]
        open(cls.sfile, 'w').write("""

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP)))

    @classmethod
    def tearDownClass(cls):
        for fl in cls.files:
            try:
                os.unlink(fl)
            except:
                pass
        MockConnection.destroy()

    def test_init_basic(self):
        msg = qmsg.Messaging(None, self.execname, self.pfwid, usedb=False)
        self.assertFalse(msg._file)
        self.assertIsNone(msg.dbh)
        self.assertIsNone(msg._taskid)
        del msg
        try:
            msg = qmsg.Messaging(self.logfile, self.execname, self.pfwid, self.taskid, usedb=False)
            self.assertIsNotNone(msg._file)
            self.assertIsNotNone(msg._taskid)
            del msg
            self.assertTrue(os.path.exists(self.logfile))
        except:
            pass
        finally:
            if os.path.exists(self.logfile):
                os.remove(self.logfile)
        self.assertRaises(Exception, qmsg.Messaging, self.logfile, self.execname, self.pfwid, mode='r', usedb=False)

    def test_init_db(self):
        msg = qmsg.Messaging(None, self.execname, self.pfwid)
        self.assertIsNotNone(msg.dbh)
        del msg

    def test_init_manual_patterns(self):
        qcp = {}
        msg = qmsg.Messaging(None, self.execname, self.pfwid, qcf_patterns=qcp)
        self.assertIsNotNone(msg.dbh)
        del msg
        qcp["override"] = "False"
        msg = qmsg.Messaging(None, 'scamp', self.pfwid, qcf_patterns=qcp)
        self.assertIsNotNone(msg.dbh)
        del msg
        qcp["override"] = "True"
        msg = qmsg.Messaging(None, self.execname, self.pfwid, qcf_patterns=qcp)
        self.assertIsNotNone(msg.dbh)
        del msg
        qcp['patterns'] = {1: {'pattern': 'abcde'},
                           2: {},
                           3: {'pattern': 'efgh',
                               'lvl': 1,
                               'priority': '56',
                               'execname': self.execname,
                               'number_of"lines': 1,
                               'only_matched': True}
                           }
        msg = qmsg.Messaging(None, self.execname, self.pfwid, qcf_patterns=qcp)
        self.assertIsNotNone(msg.dbh)
        del msg

    def test_init_excludes(self):
        qcp = {}
        qcp['excludes'] = {1: {'exec': self.execname,
                               'pattern': 'abcde'},
                           2: {'exec': 'newexec',
                               'pattern': 'abcde'}
                           }
        msg = qmsg.Messaging(None, self.execname, self.pfwid, qcf_patterns=qcp)
        self.assertIsNotNone(msg.dbh)
        del msg

    def test_init_filter(self):
        qcp = {}
        qcp['filter'] = {1: {'exec': self.execname,
                             'replace_pattern': 'abcde',
                             'with_pattern': 'abde'},
                         2: {'exec': 'newexec',
                             'pattern': 'abcde'}
                         }
        msg = qmsg.Messaging(None, self.execname, self.pfwid, qcf_patterns=qcp)
        self.assertIsNotNone(msg.dbh)
        del msg

if __name__ == '__main__':
    unittest.main()
