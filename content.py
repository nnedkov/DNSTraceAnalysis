####################################
#   Filename: content.py           #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

''' docstring to be filled '''

from config import RESULTS_DIR

from tester import res_arr_are_valid, res_miss_are_valid
from data_dumping import dump_results, dump_distinct_users, dump_invalid_data

import os



class Content:
    def __init__(self, content_id):
        self.raw_id = content_id
        
        self.raw_domain_name = self.raw_id[0]
        self.domain_name = self.raw_domain_name[1:]
        assert self.domain_name.isdigit()
        
        self.class_type = self.raw_id[1]
#            assert self.class_type == '0x0001'
        
        self.raw_ip_version = self.raw_id[2]
        assert self.raw_ip_version == '0x0001' or \
               self.raw_ip_version == '0x001c'
        self.ip_version = 'v4' if self.raw_ip_version == '0x0001' else 'v6'
        self.id = (self.domain_name, \
                   self.ip_version, \
                   self.class_type)
        
        self.dir = '%s/content_%s_%s_%s' % (RESULTS_DIR, \
                                            self.domain_name, \
                                            self.ip_version, \
                                            self.class_type)
        if not os.path.isdir(self.dir):
            os.makedirs(self.dir)
            
        self.trace_types_num = dict()
        self.transaction_status = dict()
        self.invalid_traces = list()
        self.open_user_transactions = list()
        self.int_view_traces = list()
        self.int_view_pending_traces_queue = list()
        self.ext_view_traces = list()
        self.ext_view_pending_traces_queue = list()
        self.internal_users = set()
        self.external_users = set()
        self.is_valid = True
        

    # MAJOR HYPOTHESIS: I assume that for each transaction there is one
    #                   request and its corresponding response
    def record_trace(self, trace):
        try:
            self.trace_types_num[trace.type] += 1
        except:
            self.trace_types_num[trace.type] = 1
        # (1) req_arr
        if trace.type == 1:
            self.record_req_arr(trace)
        # (2) req_miss
        elif trace.type == 2:
            self.record_req_miss(trace)
        # (3) res_miss
        elif trace.type == 3:
            self.record_res_miss(trace)
        # (4) res_arr
        elif trace.type == 4:
            self.record_res_arr(trace)
                            
                            
    def record_req_arr(self, trace):
    
        # mapping
        self.open_user_transactions.append([trace.src, trace.transaction_id, None])
        is_open = is_user = True
        is_internal = bool(int(trace.args[11]))
        self.transaction_status[trace.transaction_id] = [is_open, is_internal, [is_user, trace.src, trace.acc_secs_since_epoch]]
    
        # writing
        if is_internal:
            if not self.int_view_pending_traces_queue:
                self.int_view_traces.append(trace.rec)
            else:
                pending = False
                self.int_view_pending_traces_queue.append([trace.rec, trace.transaction_id, pending])
        else:
            if not self.ext_view_pending_traces_queue:
                self.ext_view_traces.append(trace.rec)
            else:
                pending = False
                self.ext_view_pending_traces_queue.append([trace.rec, trace.transaction_id, pending])
    
    
    def record_req_miss(self, trace):
    
        # mapping
        assert len(set([True for _, _, ass_trans_id in self.open_user_transactions if ass_trans_id is None])) == 1, "%s" % [self.open_user_transactions, trace.datetime]
        last_open_transaction = self.open_user_transactions[-1]
        _, user_trans_id, ass_trans_id = last_open_transaction
        assert ass_trans_id is None
        self.open_user_transactions[-1][2] = trace.transaction_id
        user_trans_is_open, user_trans_is_internal, _ = self.transaction_status[user_trans_id]
        assert user_trans_is_open
        is_open = True
        is_internal = user_trans_is_internal
        is_user = False
        self.transaction_status[trace.transaction_id] = [is_open, is_internal, [is_user, trace.src, trace.acc_secs_since_epoch]]
    
        # writing
        pending= True
        if is_internal:
            self.int_view_pending_traces_queue.append([trace.rec, trace.transaction_id, pending])
        else:
            self.ext_view_pending_traces_queue.append([trace.rec, trace.transaction_id, pending])
    
    
    def record_res_miss(self, trace):
    
        # mapping
        if trace.answers_count == 0:
            for j, open_user_trans in enumerate(self.open_user_transactions):
                ass_trans_id = open_user_trans[2]
                if ass_trans_id == trace.transaction_id:
                    self.open_user_transactions[j][2] = None
                    break
        self.transaction_status[trace.transaction_id][0] = False
    
        # writing
        is_internal = self.transaction_status[trace.transaction_id][1]
        if trace.answers_count == 0:
            if is_internal:
                for rec in list(self.int_view_pending_traces_queue):
                    pending_trans_id = rec[1]
                    if trace.transaction_id == pending_trans_id:
                        self.int_view_pending_traces_queue.remove(rec)
                        break
            else:
                for rec in list(self.ext_view_pending_traces_queue):
                    pending_trans_id = rec[1]
                    if trace.transaction_id == pending_trans_id:
                        self.ext_view_pending_traces_queue.remove(rec)
                        break
        else:
            if is_internal:
                self.internal_users.add(trace.node)
                for rec in self.int_view_pending_traces_queue:
                    pending_trans_id = rec[1]
                    if trace.transaction_id == pending_trans_id:
                        rec[2] = False
                        break
                pending = False
                self.int_view_pending_traces_queue.append([trace.rec, trace.transaction_id, pending])
                self.flush_buffer(self.int_view_pending_traces_queue, self.int_view_traces)
            else:
                self.external_users.add(trace.node)
                for rec in self.ext_view_pending_traces_queue:
                    pending_trans_id = rec[1]
                    if trace.transaction_id == pending_trans_id:
                        rec[2] = False
                        break
                pending = False
                self.ext_view_pending_traces_queue.append([trace.rec, trace.transaction_id, pending])
                self.flush_buffer(self.ext_view_pending_traces_queue, self.ext_view_traces)
    
    
    def record_res_arr(self, trace):
        # mapping
        for open_trans in list(self.open_user_transactions):
            user_src, user_trans_id, _ = open_trans
            if user_src == trace.dest and user_trans_id == trace.transaction_id:
                self.open_user_transactions.remove(open_trans)
        self.transaction_status[trace.transaction_id][0] = False
    
        # writing
        is_internal = bool(int(trace.args[12]))
        if is_internal:
            self.internal_users.add(trace.node)
            if not self.int_view_pending_traces_queue:
                self.int_view_traces.append(trace.rec)
            else:
                pending= False
                self.int_view_pending_traces_queue.append([trace.rec, trace.transaction_id, pending])
        else:
            self.external_users.add(trace.node)
            if not self.ext_view_pending_traces_queue:
                self.ext_view_traces.append(trace.rec)
            else:
                pending = False
                self.ext_view_pending_traces_queue.append([trace.rec, trace.transaction_id, pending])
                
                
    def is_trace_duplicate(self, trace):
        # handling the req/res duplicates
        # (which carry the same transaction_id)
        if trace.dns_is_dest:
            if trace.transaction_id in self.transaction_status:
                is_open, _, caution_data = self.transaction_status[trace.transaction_id]
                cd_is_user, cd_src, cd_secs = caution_data
    
                if is_open and cd_is_user == True and cd_src == trace.src and \
                   float(trace.acc_secs_since_epoch)-float(cd_secs) < 1:
                       return True
    
                if not is_open and cd_is_user == True and cd_src == trace.src and \
                   float(trace.acc_secs_since_epoch)-float(cd_secs) < 1 and trace.answers_count > 0:
                       return True
    
        else:
            if trace.transaction_id in self.transaction_status:
                is_open, _, caution_data = self.transaction_status[trace.transaction_id]
                cd_is_user, cd_src, cd_secs = caution_data
    
                if not is_open and cd_is_user == True and cd_src == trace.dest and \
                   float(trace.acc_secs_since_epoch)-float(cd_secs) < 1:
                       return True
                       
                      
    def record_invalid_trace(self, trace):
        self.invalid_traces.append(trace.datetime)
        
        
    def is_reffered_in_trace(self, trace):
        if trace.domain_name == self.raw_domain_name and \
           trace.class_type == self.class_type and \
           trace.ip_version == self.raw_ip_version:
            return True
            
        return False
        

    def assert_clustered_data(self):
    
        # (trace_args_dict['trace_type'], [trace_args_dict['datetime']])
        int_view_type_1_num = len([True for t_type, _ in self.int_view_traces if t_type == 1])
        int_view_type_2_num = len([True for t_type, _ in self.int_view_traces if t_type == 2])
        int_view_type_3 = [t_data for t_type, t_data in self.int_view_traces if t_type == 3]
        int_view_type_4 = [t_data for t_type, t_data in self.int_view_traces if t_type == 4]
        int_view_type_3_num = len(int_view_type_3)
        int_view_type_4_num = len(int_view_type_4)
    
        ext_view_type_1_num = len([True for t_type, _ in self.ext_view_traces if t_type == 1])
        ext_view_type_2_num = len([True for t_type, _ in self.ext_view_traces if t_type == 2])
        ext_view_type_3 = [t_data for t_type, t_data in self.ext_view_traces if t_type == 3]
        ext_view_type_4 = [t_data for t_type, t_data in self.ext_view_traces if t_type == 4]
        ext_view_type_3_num = len(ext_view_type_3)
        ext_view_type_4_num = len(ext_view_type_4)
    
        assert self.trace_types_num[1] == self.trace_types_num[4]
        assert self.trace_types_num[2] == self.trace_types_num[3]
        assert int_view_type_1_num == int_view_type_4_num
        assert int_view_type_2_num == int_view_type_3_num
        assert ext_view_type_1_num == ext_view_type_4_num
        assert ext_view_type_2_num == ext_view_type_3_num
    
        assert res_miss_are_valid(int_view_type_3)
        assert res_arr_are_valid(int_view_type_3)
        assert res_miss_are_valid(ext_view_type_3)
        assert res_arr_are_valid(ext_view_type_3)
        
        
    def dump_clustered_data(self):
        if self.int_view_traces:
            assert not self.int_view_pending_traces_queue
            is_internal_view = True
            dump_results(is_internal_view, self.int_view_traces, self.dir, self.id)
            dump_distinct_users(self.internal_users, is_internal_view, self.dir)
    
            if self.id == ('214', 'v4', '0x0001'):
                for_tests_dir = '%s/%s' % (self.dir, 'for_tests')
                dump_results(is_internal_view, self.int_view_traces, for_tests_dir, self.id, in_secs=False)
    
        if self.ext_view_traces:
            assert not self.ext_view_pending_traces_queue
            is_internal_view = False
            dump_results(is_internal_view, self.ext_view_traces, self.dir, self.id)
            dump_distinct_users(self.external_users, is_internal_view, self.dir)
            
            
    def dump_invalid_traces(self):

        if self.invalid_traces:
            filename_suffix = 'operations'
            dump_invalid_data(self.invalid_traces, self.dir, filename_suffix)
            
            
    def get_results(self):
        if self.is_valid:
            return self.is_valid, self.internal_users, self.external_users
        else:
            return self.is_valid, None, None
            
    def flush_buffer(self, trace_buffer, trace_dest):
        for rec in list(trace_buffer):
            trace_rec, transaction_id, pending = rec
            if pending == True:
                break
            trace_dest.append(trace_rec)
            trace_buffer.remove(rec)
            