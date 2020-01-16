# $Id: qcfdb.py 48057 2019-01-08 19:57:59Z friedel $
# $Rev:: 48057                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-01-08 13:57:59 #$:  # Date of last commit.

"""
    Define a database utility class extending DM DB class with QCF specific functionality
"""

__version__ = "$Rev: 48057 $"

import despydmdb.desdmdbi as desdmdbi
import despydmdb.dmdb_defs as dmdbdefs
import despymisc.miscutils as miscutils

class QCFDB(desdmdbi.DesDmDbi):
    """
    Extend DM DB class with QCF specific functionality
    """

    def __init__(self, desfile=None, section=None, connection=None):
        try:
            desdmdbi.DesDmDbi.__init__(self, desfile, section, connection)
        except Exception as err:
            miscutils.fwdie(f"Error: problem connecting to database: {err}\n\tCheck desservices file and environment variables", 1)

    def get_qcf_messages_for_wrappers(self, wrapids, gtt=None, level=None):
        """ Query and return rows from QC_PROCESSED_MESSAGE table which are associated with the
            given wrapids. This assumes wrapids is a list of ids corresponding to the pfw_wrapper_id
            column.

            Parameters
            ----------
            wrapids : list
                List containing the wrapper ids.

            Returns
            -------
            Dictionary containing the messages (and associated data) from the requested ids
        """
        # generate the sql
        empty = False
        if gtt is None:
            gtt = self.load_id_gtt(wrapids)
            empty = True

        sql = f"select task_id,message,message_lvl,message_time from task_message, {gtt} where task_id={gtt}.id"
        #else:
        #    sql = "select task_id,message,message_lvl,message_time from task_message where task_id=%s" % (self.get_positional_bind_string(1))
        if level is not None:
            sql += f" and message_lvl<{level}"
        #miscutils.fwdebug(0, 'QCFDB_DEBUG', "sql = %s" % sql)
        #miscutils.fwdebug(0, 'QCFDB_DEBUG', "wrapids = %s" % wrapids)
        # get a cursor and prepare the query
        curs = self.cursor()
        #curs.prepare(sql)
        curs.execute(sql)
        qcmsg = {}
        # execute the query for each given id and collect the results
        #for wid in wrapids:
        #    curs.execute(None, [wid])
        desc = [d[0].lower() for d in curs.description]
        for line in curs:
            d = dict(zip(desc, line))
            if d['task_id'] not in qcmsg:
                qcmsg[d['task_id']] = []

            qcmsg[d['task_id']].append(d)
        if empty:
            self.empty_gtt(gtt)

        return qcmsg

    def get_qcf_messages_for_child_wrappers(self, wrapids, level=None):
        """ Query and return rows from QC_PROCESSED_MESSAGE table which are associated with the
            given wrapids. This assumes wrapids is a list of ids corresponding to the pfw_wrapper_id
            column.

            Parameters
            ----------
            wrapids : list
                List containing the wrapper ids.

            Returns
            -------
            Dictionary containing the messages (and associated data) from the requested ids
        """
        curs = self.cursor()
        qcmsg = {}
        for wid in wrapids:
            curs.execute(f"insert into {dmdbdefs.DB_GTT_ID} (select id from task where parent_task_id={wid:d})")
            qcmsg.update(self.get_qcf_messages_for_wrappers(None, dmdbdefs.DB_GTT_ID, level))
            self.empty_gtt(dmdbdefs.DB_GTT_ID)
        return qcmsg

    def get_all_qcf_messages_by_task_id(self, wrapids, level=None):
        qcmsg = self.get_qcf_messages_for_wrappers(wrapids, level=level)
        temp = self.get_qcf_messages_for_child_wrappers(wrapids, level)
        for tid, val in temp.items():
            if tid in qcmsg:
                qcmsg[tid] += val
            else:
                qcmsg[tid] = val
        return qcmsg
