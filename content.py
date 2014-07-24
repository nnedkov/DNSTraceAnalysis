####################################
#   Filename: content.py           #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import CLUSTERING_RESULTS_DIR

from tester import res_miss_cluster_is_valid, res_arr_cluster_is_valid
from output import dump_users, dump_cluster, dump_data

import os, shutil, traceback



class Content:

    def __init__(self, content_id):
        self.raw_id = content_id

        self.raw_domain_name = self.raw_id[0]
        self.domain_name = self.raw_domain_name[1:]
        assert self.domain_name.isdigit()

        self.class_type = self.raw_id[1]

        self.raw_ip_version = self.raw_id[2]
        assert self.raw_ip_version == '0x0001' or \
               self.raw_ip_version == '0x001c' or \
               self.raw_ip_version == '0x000c'
        if self.raw_ip_version == '0x0001':
            self.ip_version = 'v4'
        elif self.raw_ip_version == '0x001c':
            self.ip_version = 'v6'
        else:
            self.ip_version = 'sec'

        self.id = (self.domain_name, \
                   self.ip_version, \
                   self.class_type)
        self.dir = '%s/%s/content_%s_%s_%s' % (CLUSTERING_RESULTS_DIR, \
                                               str(int(self.domain_name) % 50000), \
                                               self.domain_name, \
                                               self.ip_version, \
                                               self.class_type)
        if os.path.isdir(self.dir):
            shutil.rmtree(self.dir)

        if not os.path.isdir(self.dir):
            os.makedirs(self.dir)

        self.is_valid = True
        self.message = ''
        self.trace_recs_to_delete = list()
        self.transaction_status = dict()
        self.open_user_transactions = list()
        self.int_view_trace_recs = list()
        self.int_view_pending_trace_recs_queue = list()
        self.ext_view_trace_recs = list()
        self.ext_view_pending_trace_recs_queue = list()
        self.internal_users = set()
        self.external_users = set()
        self.invalid_trace_recs = list()

        self.ooop_int = None
        self.ooop_ext = None


    def is_reffered_in_trace_rec(self, trace_rec):
        if trace_rec.content == self.raw_id:
            return True

        return False


    def process_trace_rec(self, trace_rec):
        trace_rec.fill_out()
        process_next = True

        if self.trace_rec_is_duplicate(trace_rec):
            self.record_invalid_trace_rec(trace_rec)

            return process_next

        try:
            self.record_trace_rec(trace_rec)
        except Exception:
            self.is_valid = False
            self.message = traceback.format_exc()
            process_next = False

        return process_next


    def get_results(self):
        res_dict = dict()
        if self.is_valid:
            res_dict['internal_users'] = self.internal_users
            res_dict['external_users'] = self.external_users
        else:
            res_dict['message'] = self.message

        return self.is_valid, res_dict


    def assert_and_dump_clusters(self):
        try:
            self.assert_clustered_data()
        except Exception:
            self.is_valid = False
            self.message = traceback.format_exc()

        if not self.int_view_trace_recs and \
           not self.int_view_pending_trace_recs_queue and \
           not self.ext_view_trace_recs and \
           not self.ext_view_pending_trace_recs_queue:
            self.is_valid = False
            self.message = "It's empty!"

        if self.is_valid:
            self.dump_clusters_and_users()
            self.dump_invalid_trace_recs()


    def trace_rec_is_duplicate(self, trace_rec):
        # handling the req/res duplicates
        # (which carry the same transaction_id)
        if trace_rec.nameserver_is_dst:
            if trace_rec.transaction_id in self.transaction_status:
                is_open, _, caution_data = self.transaction_status[trace_rec.transaction_id]
                cd_src, cd_secs = caution_data

                if is_open and cd_src == trace_rec.src and \
                   float(trace_rec.secs)-float(cd_secs) < 1:
                       return True

                if not is_open and float(trace_rec.secs)-float(cd_secs) < 1:
                       return True

        else:
            if trace_rec.transaction_id in self.transaction_status:
                is_open, _, caution_data = self.transaction_status[trace_rec.transaction_id]
                cd_src, cd_secs = caution_data

                if not is_open and float(trace_rec.secs)-float(cd_secs) < 1:
                       return True


    def record_invalid_trace_rec(self, trace_rec):
        self.invalid_trace_recs.append(trace_rec.datetime)


    def record_trace_rec(self, trace_rec):
        if trace_rec.type == 1:   # (1) req_arr
            self.record_req_arr(trace_rec)
        elif trace_rec.type == 2:   # (2) req_miss
            self.record_req_miss(trace_rec)
        elif trace_rec.type == 3:   # (3) res_miss
            self.record_res_miss(trace_rec)
        elif trace_rec.type == 4:   # (4) res_arr
            self.record_res_arr(trace_rec)


    def record_req_arr(self, trace_rec):
        # mapping
        self.open_user_transactions.append([trace_rec.src, trace_rec.transaction_id, None])
        is_open = True
        is_internal = trace_rec.src_is_internal
        self.transaction_status[trace_rec.transaction_id] = [is_open, is_internal, [trace_rec.src, trace_rec.secs]]

        # writing
        pending = False
        if is_internal:
            if not self.int_view_pending_trace_recs_queue:
                self.int_view_trace_recs.append(trace_rec)
            else:
                self.int_view_pending_trace_recs_queue.append([trace_rec, pending])
        else:
            if not self.ext_view_pending_trace_recs_queue:
                self.ext_view_trace_recs.append(trace_rec)
            else:
                self.ext_view_pending_trace_recs_queue.append([trace_rec, pending])


    def record_req_miss(self, trace_rec):
        # mapping
        ass_open_user_transactions_num = len(set([True for _, _, ass_trans_id in self.open_user_transactions if ass_trans_id is None]))
        if ass_open_user_transactions_num == 0:
            self.trace_recs_to_delete.append(trace_rec.transaction_id)
            return
        assert ass_open_user_transactions_num == 1
        last_open_transaction = self.open_user_transactions[-1]
        _, user_trans_id, ass_trans_id = last_open_transaction
        assert ass_trans_id is None
        self.open_user_transactions[-1][2] = trace_rec.transaction_id
        user_trans_is_open, user_trans_is_internal, _ = self.transaction_status[user_trans_id]
        assert user_trans_is_open
        is_open = True
        is_internal = user_trans_is_internal
        self.transaction_status[trace_rec.transaction_id] = [is_open, is_internal, [trace_rec.src, trace_rec.secs]]

        # writing
        pending= True
        if is_internal:
            self.int_view_pending_trace_recs_queue.append([trace_rec, pending])
        else:
            self.ext_view_pending_trace_recs_queue.append([trace_rec, pending])


    def record_res_miss(self, trace_rec):
        # mapping
        if self.trace_recs_to_delete and trace_rec.transaction_id in self.trace_recs_to_delete:
            self.trace_recs_to_delete.remove(trace_rec.transaction_id)
            return
        if trace_rec.answers_num == 0:
            for j, open_user_trans in enumerate(self.open_user_transactions):
                ass_trans_id = open_user_trans[2]
                if ass_trans_id == trace_rec.transaction_id:
                    self.open_user_transactions[j][2] = None
                    break
        self.transaction_status[trace_rec.transaction_id][0] = False

        # writing
        is_internal = self.transaction_status[trace_rec.transaction_id][1]
        if trace_rec.answers_num == 0:
            if is_internal:
                for pending_rec in list(self.int_view_pending_trace_recs_queue):
                    pending_trace_rec, pending = pending_rec
                    if trace_rec.transaction_id == pending_trace_rec.transaction_id:
                        assert pending
                        self.int_view_pending_trace_recs_queue.remove(pending_rec)
                        self.ooop_int = pending_trace_rec
                        break
            else:
                for pending_rec in list(self.ext_view_pending_trace_recs_queue):
                    pending_trace_rec, pending = pending_rec
                    if trace_rec.transaction_id == pending_trace_rec.transaction_id:
                        assert pending
                        self.ext_view_pending_trace_recs_queue.remove(pending_rec)
                        self.ooop_ext = pending_trace_rec
                        break
        else:
            if is_internal:
                for pending_rec in self.int_view_pending_trace_recs_queue:
                    pending_trace_rec = pending_rec[0]
                    if trace_rec.transaction_id == pending_trace_rec.transaction_id:
                        pending_rec[1] = False
                        self.ooop_int = None
                        break
                pending = False
                self.int_view_pending_trace_recs_queue.append([trace_rec, pending])
                self.flush_buffer(self.int_view_pending_trace_recs_queue, self.int_view_trace_recs)
            else:
                for pending_rec in self.ext_view_pending_trace_recs_queue:
                    pending_trace_rec = pending_rec[0]
                    if trace_rec.transaction_id == pending_trace_rec.transaction_id:
                        pending_rec[1] = False
                        self.ooop_ext = None
                        break
                pending = False
                self.ext_view_pending_trace_recs_queue.append([trace_rec, pending])
                self.flush_buffer(self.ext_view_pending_trace_recs_queue, self.ext_view_trace_recs)


    def record_res_arr(self, trace_rec):
        # mapping
        for open_trans in list(self.open_user_transactions):
            user_src, user_trans_id, _ = open_trans
            if user_src == trace_rec.dst and user_trans_id == trace_rec.transaction_id:
                self.open_user_transactions.remove(open_trans)
        self.transaction_status[trace_rec.transaction_id][0] = False
        self.transaction_status[trace_rec.transaction_id][2][1] = trace_rec.secs

        # writing
        is_internal = trace_rec.dst_is_internal
        if is_internal:
            if not self.int_view_pending_trace_recs_queue:
                if self.ooop_int is not None:
                    self.int_view_trace_recs.append(self.ooop_int)
                    self.ooop_int = None
                self.int_view_trace_recs.append(trace_rec)
            else:
                for pending_rec in self.int_view_pending_trace_recs_queue:
                    pending_trace_rec = pending_rec[0]
                    if trace_rec.transaction_id == pending_trace_rec.transaction_id:
                        pending_rec[1] = False
                        break
                pending = False
                if self.ooop_int is not None:
                    self.int_view_pending_trace_recs_queue.append([self.ooop_int, pending])
                    self.ooop_int = None
                self.int_view_pending_trace_recs_queue.append([trace_rec, pending])
                self.flush_buffer(self.int_view_pending_trace_recs_queue, self.int_view_trace_recs)

        else:
            if not self.ext_view_pending_trace_recs_queue:
                if self.ooop_ext is not None:
                    self.ext_view_trace_recs.append(self.ooop_ext)
                    self.ooop_ext = None
                self.ext_view_trace_recs.append(trace_rec)
            else:
                for pending_rec in self.ext_view_pending_trace_recs_queue:
                    pending_trace_rec = pending_rec[0]
                    if trace_rec.transaction_id == pending_trace_rec.transaction_id:
                        pending_rec[1] = False
                        break
                pending = False
                if self.ooop_ext is not None:
                    self.ext_view_pending_trace_recs_queue.append([self.ooop_ext, pending])
                    self.ooop_ext = None
                self.ext_view_pending_trace_recs_queue.append([trace_rec, pending])
                self.flush_buffer(self.ext_view_pending_trace_recs_queue, self.ext_view_trace_recs)


    def flush_buffer(self, pending_trace_recs_buffer, ready_trace_recs, safe_mode=True):
        for pending_rec in list(pending_trace_recs_buffer):
            trace_rec, pending = pending_rec
            if pending:
                if safe_mode:
                    break
                else:
                    pending_trace_recs_buffer.remove(pending_rec)
                    continue

            ready_trace_recs.append(trace_rec)
            pending_trace_recs_buffer.remove(pending_rec)


    def assert_clustered_data(self):
        self.flush_buffer(self.int_view_pending_trace_recs_queue, self.int_view_trace_recs, safe_mode=False)
        self.flush_buffer(self.ext_view_pending_trace_recs_queue, self.ext_view_trace_recs, safe_mode=False)

        int_view_type_1_num = len([True for trace_rec in self.int_view_trace_recs if trace_rec.type == 1])
        #int_view_type_2_num = len([True for trace_rec in self.int_view_trace_recs if trace_rec.type == 2])
        int_view_type_3 = [(trace_rec.secs, trace_rec.ttl) for trace_rec in self.int_view_trace_recs if trace_rec.type == 3]
        int_view_type_4 = [(trace_rec.secs, trace_rec.ttl) for trace_rec in self.int_view_trace_recs if trace_rec.type == 4]
        #int_view_type_3_num = len(int_view_type_3)
        int_view_type_4_num = len(int_view_type_4)

        ext_view_type_1_num = len([True for trace_rec in self.ext_view_trace_recs if trace_rec.type == 1])
        #ext_view_type_2_num = len([True for trace_rec in self.ext_view_trace_recs if trace_rec.type == 2])
        ext_view_type_3 = [(trace_rec.secs, trace_rec.ttl) for trace_rec in self.ext_view_trace_recs if trace_rec.type == 3]
        ext_view_type_4 = [(trace_rec.secs, trace_rec.ttl) for trace_rec in self.ext_view_trace_recs if trace_rec.type == 4]
        #ext_view_type_3_num = len(ext_view_type_3)
        ext_view_type_4_num = len(ext_view_type_4)

        assert int_view_type_1_num == int_view_type_4_num
        #assert int_view_type_2_num == int_view_type_3_num
        assert ext_view_type_1_num == ext_view_type_4_num
        #assert ext_view_type_2_num == ext_view_type_3_num

        assert res_miss_cluster_is_valid(int_view_type_3)[0]
        assert res_arr_cluster_is_valid(int_view_type_4)[0]
        assert res_miss_cluster_is_valid(ext_view_type_3)[0]
        assert res_arr_cluster_is_valid(ext_view_type_4)[0]


    def dump_clusters_and_users(self):
        if self.int_view_trace_recs:
            assert not self.int_view_pending_trace_recs_queue, [trace_rec.datetime for trace_rec, _ in self.int_view_pending_trace_recs_queue]
            is_internal_view = True
            self.dump_clusters(is_internal_view, self.dir)
            self.internal_users = set([trace_rec.node for trace_rec in self.int_view_trace_recs if trace_rec.type == 1])
            dump_users(self.internal_users, self.dir, is_internal_view)

            if self.id == ('214', 'v4', '0x0001'):
                dir_for_tests = '%s/%s' % (self.dir, 'for_tests')
                self.dump_clusters(is_internal_view, dir_for_tests, in_secs=False)

        if self.ext_view_trace_recs:
            assert not self.ext_view_pending_trace_recs_queue, [trace_rec.datetime for trace_rec, _ in self.ext_view_pending_trace_recs_queue]
            is_internal_view = False
            self.dump_clusters(is_internal_view, self.dir)
            self.external_users = set([trace_rec.node for trace_rec in self.ext_view_trace_recs if trace_rec.type == 1])
            dump_users(self.external_users, self.dir, is_internal_view)


    def dump_clusters(self, is_internal_view, root_dir, in_secs=True):
        view_dir = 'internal_view' if is_internal_view else 'external_view'
        res_dir = '%s/%s' % (root_dir, view_dir)
        if not os.path.isdir(res_dir):
            os.makedirs(res_dir)

        trace_recs = self.int_view_trace_recs if is_internal_view else self.ext_view_trace_recs
        clusters = dict()

        for trace_rec in trace_recs:
            try:
                if trace_rec.type in [3, 4] and trace_rec.ttl == 0:
                    continue
                clusters[trace_rec.type].append(trace_rec)
            except KeyError:
                clusters[trace_rec.type] = [trace_rec]

        filename_prefix = { 1: 'req_arr',
                            2: 'req_miss',
                            3: 'res_miss',
                            4: 'res_arr'}
        filename_suffix = '%s_%s_%s' % (self.domain_name, \
                                        self.ip_version, \
                                        self.class_type)

        for trace_rec_type, cluster in clusters.iteritems():
            if cluster:
                filename = '%s/%s_%s.txt' % (res_dir, \
                                             filename_prefix[trace_rec_type], \
                                             filename_suffix)
                dump_cluster(cluster, filename, in_secs)


    def dump_invalid_trace_recs(self):
        if self.invalid_trace_recs:
            filename = '%s/invalid_trace_recs.log' % self.dir
            dump_data(self.invalid_trace_recs, filename)
