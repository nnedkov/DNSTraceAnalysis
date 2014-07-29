#######################################
#   Filename: content_clusters.py     #
#   Nedko Stefanov Nedkov             #
#   nedko.stefanov.nedkov@gmail.com   #
#   April 2014                        #
#######################################

from config import CLUSTERING_RESULTS_DIR, CLUSTERING_RESULTS_SUBDIRS_NUM

from tester import res_miss_cluster_is_valid, res_arr_cluster_is_valid
from output import dump_users, dump_cluster, dump_data

import os, shutil, traceback



class Content_clusters:

    def __init__(self, content_id):
        domain_name = content_id[0]
        self.explicit_domain_name = domain_name[1:]
        assert self.explicit_domain_name.isdigit()

        self.class_type = content_id[1]

        self.ip_version = content_id[2]

        self.explicit_content_id = (self.explicit_domain_name, \
                                    self.class_type, \
                                    self.ip_version)
        self.dir = '%s/%s/content_%s_%s_%s' % \
                   (CLUSTERING_RESULTS_DIR, \
                    str(int(self.explicit_domain_name) % CLUSTERING_RESULTS_SUBDIRS_NUM), \
                    self.explicit_domain_name, \
                    self.ip_version, \
                    self.class_type)

        if os.path.isdir(self.dir):
            shutil.rmtree(self.dir)

        self.is_valid = True
        self.outcome_message = ''
        self.internal_users = set()
        self.external_users = set()
        self.internal_view_recs = list()
        self.internal_view_pending_recs_queue = list()
        self.external_view_recs = list()
        self.external_view_pending_recs_queue = list()
        self.transaction_status = dict()
        self.invalid_recs = list()
        self.open_user_transactions = list()
        self.recs_to_delete = list()


    def process_rec(self, rec):
        rec.fill_out()
        process_next = True

        if self.rec_is_duplicate(rec):
            self.record_invalid_rec(rec)

            return process_next

        try:
            self.record_rec(rec)
        except Exception:
            self.is_valid = False
            self.outcome_message = traceback.format_exc()
            process_next = False

        return process_next


    def get_outcome_status(self):
        res = dict()

        if not self.is_valid:
            res['outcome_message'] = self.outcome_message

        return self.is_valid, res


    def assert_and_dump_clusters(self, output_users=False):
        try:
            self.assert_clusters()
        except Exception:
            self.is_valid = False
            self.outcome_message = traceback.format_exc()

        if not self.internal_view_recs and \
           not self.internal_view_pending_recs_queue and \
           not self.external_view_recs and \
           not self.external_view_pending_recs_queue:
            self.is_valid = False
            self.outcome_message = 'Empty clusters!'

        if self.is_valid:
            self.dump_clusters_and_users(output_users)
            self.dump_invalid_recs()


    def rec_is_duplicate(self, rec):
        # handling the duplicate requests/responses
        # (which carry the same transaction_id)
        if rec.nameserver_is_dst:
            if rec.transaction_id in self.transaction_status:
                is_open, _, caution_data = self.transaction_status[rec.transaction_id]
                cd_src, cd_secs = caution_data

                if is_open and cd_src == rec.src and \
                   float(rec.secs)-float(cd_secs) < 1:
                    return True

                if not is_open and float(rec.secs)-float(cd_secs) < 1:
                    return True

        else:
            if rec.transaction_id in self.transaction_status:
                is_open, _, caution_data = self.transaction_status[rec.transaction_id]
                cd_src, cd_secs = caution_data

                if not is_open and float(rec.secs)-float(cd_secs) < 1:
                       return True


    def record_invalid_rec(self, rec):
        self.invalid_recs.append(rec.datetime)


    def record_rec(self, rec):
        if rec.type == 1:   # (1) req_arr
            self.record_req_arr(rec)
        elif rec.type == 2:   # (2) req_miss
            self.record_req_miss(rec)
        elif rec.type == 3:   # (3) res_miss
            self.record_res_miss(rec)
        elif rec.type == 4:   # (4) res_arr
            self.record_res_arr(rec)


    def record_req_arr(self, rec):
        # associating records
        self.open_user_transactions.append([rec.src, rec.transaction_id, None])
        is_open = True
        is_internal = rec.src_is_internal
        self.transaction_status[rec.transaction_id] = [is_open, \
                                                       is_internal, \
                                                       [rec.src, rec.secs]]

        # storing records
        pending = False
        if is_internal:
            if not self.internal_view_pending_recs_queue:
                self.internal_view_recs.append(rec)
            else:
                self.internal_view_pending_recs_queue.append([rec, pending])
        else:
            if not self.external_view_pending_recs_queue:
                self.external_view_recs.append(rec)
            else:
                self.external_view_pending_recs_queue.append([rec, pending])


    def record_req_miss(self, rec):
        # associating records
        not_ass_open_user_trans_exist = bool([1 for _, _, ass_trans_id in \
                                              self.open_user_transactions \
                                              if ass_trans_id is None])
        if not not_ass_open_user_trans_exist:
            self.recs_to_delete.append(rec.transaction_id)
            return
        last_open_user_transaction = self.open_user_transactions[-1]
        _, user_trans_id, ass_trans_id = last_open_user_transaction
        assert ass_trans_id is None
        self.open_user_transactions[-1][2] = rec.transaction_id
        user_trans_is_open, user_trans_is_internal, _ = self.transaction_status[user_trans_id]
        assert user_trans_is_open
        is_open = True
        is_internal = user_trans_is_internal
        self.transaction_status[rec.transaction_id] = [is_open,
                                                       is_internal,
                                                       [rec.src, rec.secs]]

        # storing records
        pending= True
        if is_internal:
            self.internal_view_pending_recs_queue.append([rec, pending])
        else:
            self.external_view_pending_recs_queue.append([rec, pending])


    def record_res_miss(self, rec):
        # associating records
        if rec.transaction_id in self.recs_to_delete:
            self.recs_to_delete.remove(rec.transaction_id)
            return
        if rec.answers_num == 0:
            for i, open_user_trans in enumerate(self.open_user_transactions):
                ass_trans_id = open_user_trans[2]
                if ass_trans_id == rec.transaction_id:
                    self.open_user_transactions[i][2] = None
                    break
        self.transaction_status[rec.transaction_id][0] = False

        # storing records
        is_internal = self.transaction_status[rec.transaction_id][1]
        if rec.answers_num == 0:
            if is_internal:
                for pending_queue_rec in list(self.internal_view_pending_recs_queue):
                    pending_rec, pending = pending_queue_rec
                    if rec.transaction_id == pending_rec.transaction_id:
                        assert pending
                        self.internal_view_pending_recs_queue.remove(pending_queue_rec)
                        break
            else:
                for pending_queue_rec in list(self.external_view_pending_recs_queue):
                    pending_rec, pending = pending_queue_rec
                    if rec.transaction_id == pending_rec.transaction_id:
                        assert pending
                        self.external_view_pending_recs_queue.remove(pending_queue_rec)
                        break
        else:
            if is_internal:
                for pending_queue_rec in self.internal_view_pending_recs_queue:
                    pending_rec = pending_queue_rec[0]
                    if rec.transaction_id == pending_rec.transaction_id:
                        pending_queue_rec[1] = False
                        break
                pending = False
                self.internal_view_pending_recs_queue.append([rec, pending])
                self.flush_buffer(self.internal_view_pending_recs_queue,
                                  self.internal_view_recs)
            else:
                for pending_queue_rec in self.external_view_pending_recs_queue:
                    pending_rec = pending_queue_rec[0]
                    if rec.transaction_id == pending_rec.transaction_id:
                        pending_queue_rec[1] = False
                        break
                pending = False
                self.external_view_pending_recs_queue.append([rec, pending])
                self.flush_buffer(self.external_view_pending_recs_queue,
                                  self.external_view_recs)


    def record_res_arr(self, rec):
        # associating records
        for open_trans in list(self.open_user_transactions):
            user_src, user_trans_id, _ = open_trans
            if user_src == rec.dst and user_trans_id == rec.transaction_id:
                self.open_user_transactions.remove(open_trans)
        self.transaction_status[rec.transaction_id][0] = False
        self.transaction_status[rec.transaction_id][2][1] = rec.secs

        # storing records
        is_internal = rec.dst_is_internal
        if is_internal:
            if not self.internal_view_pending_recs_queue:
                self.internal_view_recs.append(rec)
            else:
                for pending_queue_rec in self.internal_view_pending_recs_queue:
                    pending_rec = pending_queue_rec[0]
                    if rec.transaction_id == pending_rec.transaction_id:
                        pending_queue_rec[1] = False
                        break
                pending = False
                self.internal_view_pending_recs_queue.append([rec, pending])
                self.flush_buffer(self.internal_view_pending_recs_queue,
                                  self.internal_view_recs)

        else:
            if not self.external_view_pending_recs_queue:
                self.external_view_recs.append(rec)
            else:
                for pending_queue_rec in self.external_view_pending_recs_queue:
                    pending_rec = pending_queue_rec[0]
                    if rec.transaction_id == pending_rec.transaction_id:
                        pending_queue_rec[1] = False
                        break
                pending = False
                self.external_view_pending_recs_queue.append([rec, pending])
                self.flush_buffer(self.external_view_pending_recs_queue,
                                  self.external_view_recs)


    def flush_buffer(self, pending_recs_buffer, ready_recs, safe_mode=True):
        for pending_rec in list(pending_recs_buffer):
            rec, pending = pending_rec
            if pending:
                if safe_mode:
                    break
                else:
                    pending_recs_buffer.remove(pending_rec)
                    continue

            ready_recs.append(rec)
            pending_recs_buffer.remove(pending_rec)


    def assert_clusters(self):
        self.flush_buffer(self.internal_view_pending_recs_queue,
                          self.internal_view_recs,
                          safe_mode=False)
        self.flush_buffer(self.external_view_pending_recs_queue,
                          self.external_view_recs,
                          safe_mode=False)

        internal_view_recs_type_1_num = len([1 for rec in \
                                             self.internal_view_recs \
                                             if rec.type == 1])
        internal_view_recs_type_2_num = len([1 for rec in \
                                             self.internal_view_recs \
                                             if rec.type == 2])
        internal_view_recs_type_3 = [(rec.secs, rec.ttl) for rec in \
                                     self.internal_view_recs \
                                     if rec.type == 3]
        internal_view_recs_type_4 = [(rec.secs, rec.ttl) for rec in \
                                     self.internal_view_recs \
                                     if rec.type == 4]
        internal_view_recs_type_3_num = len(internal_view_recs_type_3)
        internal_view_recs_type_4_num = len(internal_view_recs_type_4)

        external_view_recs_type_1_num = len([1 for rec in \
                                             self.external_view_recs \
                                             if rec.type == 1])
        external_view_recs_type_2_num = len([1 for rec in \
                                             self.external_view_recs \
                                             if rec.type == 2])
        external_view_recs_type_3 = [(rec.secs, rec.ttl) for rec in \
                                     self.external_view_recs \
                                     if rec.type == 3]
        external_view_recs_type_4 = [(rec.secs, rec.ttl) for rec in \
                                     self.external_view_recs \
                                     if rec.type == 4]
        external_view_recs_type_3_num = len(external_view_recs_type_3)
        external_view_recs_type_4_num = len(external_view_recs_type_4)

        assert internal_view_recs_type_1_num == internal_view_recs_type_4_num
        assert internal_view_recs_type_2_num == internal_view_recs_type_3_num
        assert external_view_recs_type_1_num == external_view_recs_type_4_num
        assert external_view_recs_type_2_num == external_view_recs_type_3_num

        assert res_miss_cluster_is_valid(internal_view_recs_type_3)[0]
        assert res_arr_cluster_is_valid(internal_view_recs_type_4)[0]
        assert res_miss_cluster_is_valid(external_view_recs_type_3)[0]
        assert res_arr_cluster_is_valid(external_view_recs_type_4)[0]


    def dump_clusters_and_users(self, output_users):
        if self.internal_view_recs:
            assert not self.internal_view_pending_recs_queue
            self.dump_clusters(self.dir,
                               output_users,
                               is_internal_view=True)
            self.internal_users = set([rec.node for rec in \
                                       self.internal_view_recs \
                                       if rec.type == 1])
            dump_users(self.internal_users, self.dir, is_internal_view=True)

            if self.explicit_content_id == ('214', '0x0001', '0x0001'):
                dir_for_tests = '%s/%s' % (self.dir, 'for_tests')
                self.dump_clusters(dir_for_tests,
                                   output_users,
                                   is_internal_view=True,
                                   in_secs=False)

        if self.external_view_recs:
            assert not self.external_view_pending_recs_queue
            self.dump_clusters(self.dir,
                               output_users,
                               is_internal_view=False)
            self.external_users = set([rec.node for rec in \
                                       self.external_view_recs \
                                       if rec.type == 1])
            dump_users(self.external_users, self.dir, is_internal_view=False)


    def dump_clusters(self,
                      root_dir,
                      output_users,
                      is_internal_view=True,
                      in_secs=True):
        view_dir = 'internal_view' if is_internal_view else 'external_view'
        res_dir = '%s/%s' % (root_dir, view_dir)
        if not os.path.isdir(res_dir):
            os.makedirs(res_dir)

        recs = self.internal_view_recs if is_internal_view else self.external_view_recs
        clusters = dict()

        for rec in recs:
            try:
                if rec.type in [3, 4] and rec.ttl == 0:
                    continue
                clusters[rec.type].append(rec)
            except KeyError:
                clusters[rec.type] = [rec]

        filename_prefix = { 1: 'req_arr',
                            2: 'req_miss',
                            3: 'res_miss',
                            4: 'res_arr' }
        filename_suffix = '%s_%s_%s' % (self.explicit_domain_name, \
                                        self.ip_version, \
                                        self.class_type)

        for rec_type, cluster in clusters.iteritems():
            if cluster:
                filename = '%s/%s_%s.txt' % (res_dir, \
                                             filename_prefix[rec_type], \
                                             filename_suffix)
                dump_cluster(cluster, filename, in_secs, output_users)


    def dump_invalid_recs(self):
        if self.invalid_recs:
            filename = '%s/invalid_trace_recs.log' % self.dir
            dump_data(self.invalid_recs, filename)
