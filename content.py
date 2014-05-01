####################################
#   Filename: content.py           #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import RESULTS_DIR

from tester import res_miss_cluster_is_valid, res_arr_cluster_is_valid
from data_dumping import dump_users, dump_cluster, dump_data

import traceback, os



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
        self.dir = '%s/content_%s_%s_%s' % (RESULTS_DIR, \
                                            self.domain_name, \
                                            self.ip_version, \
                                            self.class_type)
        self.is_valid = True
        self.message = ''
        self.traces_to_delete = list()
        self.transaction_status = dict()
        self.trace_types_num = dict()
        self.open_user_transactions = list()
        self.int_view_traces = list()
        self.int_view_pending_traces_queue = list()
        self.ext_view_traces = list()
        self.ext_view_pending_traces_queue = list()
        self.internal_users = set()
        self.external_users = set()
        self.invalid_traces = list()


    def is_reffered_in_trace(self, trace):
        if trace.content_id == self.raw_id:
            return True

        return False


    def process_trace(self, trace):
        trace.fill_out()
        process_next = True

        if self.trace_is_duplicate(trace):
            self.record_invalid_trace(trace)

            return process_next

        try:
            self.record_trace(trace)
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

        if not self.int_view_traces and \
           not self.int_view_pending_traces_queue and \
           not self.ext_view_traces and \
           not self.ext_view_pending_traces_queue:
            self.is_valid = False
            self.message = 'It\'s empty!'

        self.dump_clusters_and_users()
        self.dump_invalid_traces()


    def trace_is_duplicate(self, trace):
        # handling the req/res duplicates
        # (which carry the same transaction_id)
        if trace.nameserver_is_dst:
            if trace.transaction_id in self.transaction_status:
                is_open, _, caution_data = self.transaction_status[trace.transaction_id]
                cd_is_user, cd_src, cd_secs = caution_data

                if is_open and cd_is_user and cd_src == trace.src and \
                   float(trace.secs)-float(cd_secs) < 1:
                       return True

                if not is_open and cd_is_user and cd_src == trace.src and \
                   float(trace.secs)-float(cd_secs) < 1:
                       return True

        else:
            if trace.transaction_id in self.transaction_status:
                is_open, _, caution_data = self.transaction_status[trace.transaction_id]
                cd_is_user, cd_src, cd_secs = caution_data

                if not is_open and cd_is_user and cd_src == trace.dst and \
                   float(trace.secs)-float(cd_secs) < 1:
                       return True


    def record_invalid_trace(self, trace):
        self.invalid_traces.append(trace.datetime)


    def record_trace(self, trace):
        print '%s --- %s\n%s\t%s\n' % (trace.datetime, trace.type, str([trac.datetime for trac, _ in self.ext_view_pending_traces_queue]), str([trac.datetime for trac in self.ext_view_traces]))
        try:
            self.trace_types_num[trace.type] += 1
        except:
            self.trace_types_num[trace.type] = 1

        if trace.type == 1:   # (1) req_arr
            self.record_req_arr(trace)
        elif trace.type == 2:   # (2) req_miss
            self.record_req_miss(trace)
        elif trace.type == 3:   # (3) res_miss
            self.record_res_miss(trace)
        elif trace.type == 4:   # (4) res_arr
            self.record_res_arr(trace)


    def record_req_arr(self, trace):
        # mapping
        self.open_user_transactions.append([trace.src, trace.transaction_id, None])
        is_open = is_user = True
        is_internal = trace.src_is_internal
        self.transaction_status[trace.transaction_id] = [is_open, is_internal, [is_user, trace.src, trace.secs]]

        # writing
        pending = False
        if is_internal:
            if not self.int_view_pending_traces_queue:
                self.int_view_traces.append(trace)
            else:
                self.int_view_pending_traces_queue.append([trace, pending])
        else:
            if not self.ext_view_pending_traces_queue:
                self.ext_view_traces.append(trace)
            else:
                self.ext_view_pending_traces_queue.append([trace, pending])


    def record_req_miss(self, trace):
        # mapping
        ass_open_user_transactions_num = len(set([True for _, _, ass_trans_id in self.open_user_transactions if ass_trans_id is None]))
        if ass_open_user_transactions_num == 0:
            self.traces_to_delete.append(trace.transaction_id)
            return
        assert ass_open_user_transactions_num == 1, "%s" % [self.open_user_transactions, trace.datetime]
        last_open_transaction = self.open_user_transactions[-1]
        _, user_trans_id, ass_trans_id = last_open_transaction
        assert ass_trans_id is None
        self.open_user_transactions[-1][2] = trace.transaction_id
        user_trans_is_open, user_trans_is_internal, _ = self.transaction_status[user_trans_id]
        assert user_trans_is_open
        is_open = True
        is_internal = user_trans_is_internal
        is_user = False
        self.transaction_status[trace.transaction_id] = [is_open, is_internal, [is_user, trace.src, trace.secs]]

        # writing
        pending= True
        if is_internal:
            self.int_view_pending_traces_queue.append([trace, pending])
        else:
            self.ext_view_pending_traces_queue.append([trace, pending])


    def record_res_miss(self, trace):
        # mapping
        if self.traces_to_delete and trace.transaction_id in self.traces_to_delete:
            self.traces_to_delete.remove(trace.transaction_id)
            return
        if trace.answers_num == 0:
            for j, open_user_trans in enumerate(self.open_user_transactions):
                ass_trans_id = open_user_trans[2]
                if ass_trans_id == trace.transaction_id:
                    self.open_user_transactions[j][2] = None
                    break
        self.transaction_status[trace.transaction_id][0] = False

        # writing
        is_internal = self.transaction_status[trace.transaction_id][1]
        if trace.answers_num == 0:
            if is_internal:
                for pending_rec in list(self.int_view_pending_traces_queue):
                    pending_trace, pending = pending_rec
                    if trace.transaction_id == pending_trace.transaction_id:
                        assert pending
                        self.int_view_pending_traces_queue.remove(pending_rec)
                        break
            else:
                for pending_rec in list(self.ext_view_pending_traces_queue):
                    pending_trace, pending = pending_rec
                    if trace.transaction_id == pending_trace.transaction_id:
                        assert pending
                        self.ext_view_pending_traces_queue.remove(pending_rec)
                        break
        else:
            if is_internal:
                self.internal_users.add(trace.node)
                for pending_rec in self.int_view_pending_traces_queue:
                    pending_trace = pending_rec[0]
                    if trace.transaction_id == pending_trace.transaction_id:
                        pending_rec[1] = False
                        break
                pending = False
                self.int_view_pending_traces_queue.append([trace, pending])
                self.flush_buffer(self.int_view_pending_traces_queue, self.int_view_traces)
            else:
                self.external_users.add(trace.node)
                for pending_rec in self.ext_view_pending_traces_queue:
                    pending_trace = pending_rec[0]
                    if trace.transaction_id == pending_trace.transaction_id:
                        pending_rec[1] = False
                        break
                pending = False
                self.ext_view_pending_traces_queue.append([trace, pending])
                self.flush_buffer(self.ext_view_pending_traces_queue, self.ext_view_traces)


    def record_res_arr(self, trace):
        # mapping
        for open_trans in list(self.open_user_transactions):
            user_src, user_trans_id, _ = open_trans
            if user_src == trace.dst and user_trans_id == trace.transaction_id:
                self.open_user_transactions.remove(open_trans)
        self.transaction_status[trace.transaction_id][0] = False
        self.transaction_status[trace.transaction_id][2][2] = trace.secs

        # writing
        is_internal = trace.dst_is_internal
        if is_internal:
            self.internal_users.add(trace.node)

            if not self.int_view_pending_traces_queue:
                self.int_view_traces.append(trace)
            else:
                for pending_rec in self.int_view_pending_traces_queue:
                    pending_trace = pending_rec[0]
                    if trace.transaction_id == pending_trace.transaction_id:
                        pending_rec[1] = False
                        break
                pending = False
                self.int_view_pending_traces_queue.append([trace, pending])
                self.flush_buffer(self.int_view_pending_traces_queue, self.int_view_traces)

        else:
            self.external_users.add(trace.node)
            if not self.ext_view_pending_traces_queue:
                self.ext_view_traces.append(trace)
            else:
                for pending_rec in self.ext_view_pending_traces_queue:
                    pending_trace = pending_rec[0]
                    if trace.transaction_id == pending_trace.transaction_id:
                        pending_rec[1] = False
                        break
                pending = False
                self.ext_view_pending_traces_queue.append([trace, pending])
                self.flush_buffer(self.ext_view_pending_traces_queue, self.ext_view_traces)


    def flush_buffer(self, pending_traces_buffer, ready_traces):
        for pending_rec in list(pending_traces_buffer):
            trace, pending = pending_rec
            if pending:
                break

            ready_traces.append(trace)
            pending_traces_buffer.remove(pending_rec)


    def assert_clustered_data(self):
        int_view_type_1_num = len([True for trace in self.int_view_traces if trace.type == 1])
        int_view_type_2_num = len([True for trace in self.int_view_traces if trace.type == 2])
        int_view_type_3 = [(trace.secs, trace.ttl) for trace in self.int_view_traces if trace.type == 3]
        int_view_type_4 = [(trace.secs, trace.ttl) for trace in self.int_view_traces if trace.type == 4]
        int_view_type_3_num = len(int_view_type_3)
        int_view_type_4_num = len(int_view_type_4)

        ext_view_type_1_num = len([True for trace in self.ext_view_traces if trace.type == 1])
        ext_view_type_2_num = len([True for trace in self.ext_view_traces if trace.type == 2])
        ext_view_type_3 = [(trace.secs, trace.ttl) for trace in self.ext_view_traces if trace.type == 3]
        ext_view_type_4 = [(trace.secs, trace.ttl) for trace in self.ext_view_traces if trace.type == 4]
        ext_view_type_3_num = len(ext_view_type_3)
        ext_view_type_4_num = len(ext_view_type_4)

        if 1 in self.trace_types_num or 4 in self.trace_types_num:
            assert self.trace_types_num[1] == self.trace_types_num[4]
        if 2 in self.trace_types_num or 3 in self.trace_types_num:
            assert self.trace_types_num[2] == self.trace_types_num[3], [self.trace_types_num[2], self.trace_types_num[3]]

        assert int_view_type_1_num == int_view_type_4_num
        assert int_view_type_2_num == int_view_type_3_num
        assert ext_view_type_1_num == ext_view_type_4_num
        assert ext_view_type_2_num == ext_view_type_3_num

        assert res_miss_cluster_is_valid(int_view_type_3)[0]
        assert res_arr_cluster_is_valid(int_view_type_4)[0]
        assert res_miss_cluster_is_valid(ext_view_type_3)[0]
        assert res_arr_cluster_is_valid(ext_view_type_4)[0]


    def dump_clusters_and_users(self):
        if self.int_view_traces:
            assert not self.int_view_pending_traces_queue, [trace.datetime for trace, _ in self.int_view_pending_traces_queue]
            is_internal_view = True
            self.dump_clusters(is_internal_view, self.dir)
            dump_users(self.internal_users, self.dir, is_internal_view)

            if self.id == ('214', 'v4', '0x0001'):
                dir_for_tests = '%s/%s' % (self.dir, 'for_tests')
                self.dump_clusters(is_internal_view, dir_for_tests, in_secs=False)

        if self.ext_view_traces:
            assert not self.ext_view_pending_traces_queue, [trace.datetime for trace, _ in self.ext_view_pending_traces_queue]
            is_internal_view = False
            self.dump_clusters(is_internal_view, self.dir)
            dump_users(self.external_users, self.dir, is_internal_view)


    def dump_clusters(self, is_internal_view, root_dir, in_secs=True):
        view_dir = 'internal_view' if is_internal_view else 'external_view'
        res_dir = '%s/%s' % (root_dir, view_dir)
        if not os.path.isdir(res_dir):
            os.makedirs(res_dir)

        traces = self.int_view_traces if is_internal_view else self.ext_view_traces
        clusters = dict()

        for trace in traces:
            try:
                clusters[trace.type].append(trace)
            except KeyError:
                clusters[trace.type] = [trace]

        filename_prefix = { 1: 'req_arr',
                            2: 'req_miss',
                            3: 'res_miss',
                            4: 'res_arr'}
        filename_suffix = '%s_%s_%s' % (self.domain_name, \
                                        self.ip_version, \
                                        self.class_type)

        for trace_type, cluster in clusters.iteritems():
            if cluster:
                filename = '%s/%s_%s.txt' % (res_dir, \
                                             filename_prefix[trace_type], \
                                             filename_suffix)
                dump_cluster(cluster, filename, in_secs)


    def dump_invalid_traces(self):
        if self.invalid_traces:
            filename = '%s/invalid_traces.log' % self.dir
            dump_data(self.invalid_traces, filename)
