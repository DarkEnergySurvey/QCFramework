import unittest
import os
import stat
import time
from MockDBI import MockConnection
from despydmdb import desdmdbi

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
        msg.close()
        del msg
        try:
            msg = qmsg.Messaging(self.logfile, self.execname, self.pfwid, self.taskid, usedb=False)
            self.assertIsNotNone(msg._file)
            self.assertIsNotNone(msg._taskid)
            msg.close()
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
        dbh = desdmdbi.DesDmDbi(threaded=True)
        msg = qmsg.Messaging(self.logfile, self.execname, self.pfwid, self.taskid, dbh=dbh)
        self.assertIsNotNone(msg.dbh)
        del msg

    def test_init_manual_patterns(self):
        qcp = {}
        msg = qmsg.Messaging(None, self.execname, self.pfwid, qcf_patterns=qcp)
        self.assertIsNotNone(msg.dbh)
        del msg
        qcp = {}
        msg = qmsg.Messaging(None, 'testignore', self.pfwid, qcf_patterns=qcp)
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
                           2: {'lvl': '5'},
                           3: {'pattern': 'efgh',
                               'lvl': 1,
                               'priority': '56',
                               'execname': self.execname,
                               'number_of_lines': 1,
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
                               'pattern': 'abcde'},
                           3: {'pattern': 'aabb'}
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
                             'replace_pattern': 'abcde'},
                         3: {'replace_pattern': 'aavv'}
                         }
        msg = qmsg.Messaging(None, self.execname, self.pfwid, qcf_patterns=qcp)
        self.assertIsNotNone(msg.dbh)
        del msg

    def test_pfw_message(self):
        dbh = desdmdbi.DesDmDbi(threaded=True)
        taskid = 22334455
        sql = f"select message_lvl, message, log_line from task_message where task_id={taskid}"
        curs = dbh.cursor()
        curs.execute(sql)
        results = curs.fetchall()
        self.assertEqual(0, len(results))

        qmsg.pfw_message(dbh, 109876, taskid, "This is a test", level=3, line_no=225)
        curs.execute(sql)
        results = curs.fetchall()
        self.assertEqual(1, len(results))
        self.assertEqual(results[0][0], 3)
        self.assertEqual(results[0][2], 225)
        self.assertTrue('This' in results[0][1])
        curs.close()
        dbh.close()

    def test_set_task_id(self):
        msg = qmsg.Messaging(None, self.execname, self.pfwid)
        self.assertIsNone(msg._taskid)
        msg.set_task_id(55)
        self.assertEqual(55, msg._taskid)
        msg.set_task_id(None)
        self.assertIsNone(msg._taskid)
        del msg

    def test_set_name(self):
        msg = qmsg.Messaging(None, self.execname, self.pfwid)
        self.assertEqual(msg.fname, '')
        msg.setname('Newname')
        self.assertEqual('Newname', msg.fname)
        del msg

    def test_write_no_file(self):
        messages = ["Hello",
                    "Traceback: an error occurred",
                    "curl exitcode",
                    "ORA-11011"
                    ]
        msg = qmsg.Messaging(None, self.execname, self.pfwid, self.taskid)
        for m in messages:
            msg.write(m)
        del msg
        time.sleep(10)
        dbh = desdmdbi.DesDmDbi(threaded=True)
        sql = f"select count(*) from task_message where task_id={self.taskid}"
        curs = dbh.cursor()
        curs.execute(sql)
        results = curs.fetchone()
        self.assertEqual(3, results[0])
        dbh.close()


if __name__ == '__main__':
    unittest.main()
