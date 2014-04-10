#############################
#   Filename: script.py     #
#   Nedko Stefanov Nedkov   #
#   nedko.nedkov@inria.fr   #
#   April 2014              #
#############################

''' docstring to be filled '''

import os
from time import strptime
from calendar import timegm

trace_files_dir = '/home/nedko/Inria/DNStraces/'
trace_files_name_prefix = 'dns2-sop-00'
trace_files_path_prefix = '%s%s' % (trace_files_dir, trace_files_name_prefix)



def convert_datetime_to_secs(datetime):
    millisecs = datetime.split('.')[1]
    time_obj = strptime(datetime, "%b %d, %Y %H:%M:%S.%f000")   # millisecs are lost in the object
    secs_since_epoch = timegm(time_obj)
    acc_secs_since_epoch = '%s.%s' % (secs_since_epoch, millisecs)

    return float(acc_secs_since_epoch)



if __name__ == '__main__':

    contents = set()

#    for i in range(9):
#        trace_file = '%s%s' % (trace_files_path_prefix, str(i))
#
#        with open(trace_file) as fp:
#
#            for line in fp:
#                trace_args = line.rstrip().split('\t')
#
#                domain = trace_args[7]
#                class_type = trace_args[8]
#                ip_version = trace_args[9]
#
#                contents.add((domain, class_type, ip_version))

    contents = [('N2062', '0x0001', '0x0001'), ('N214', '0x0001', '0x0001'), ('N4992', '0x0001', '0x0001'), ('N11845', '0x0001', '0x0001'), ('N19787', '0x0001', '0x0001'), ('N344', '0x0001', '0x0001')]
    contents_len = len(contents)

    for content in list(contents):
        domain, class_type, ip_version = content

        domain = domain[1:]
#        assert class_type == '0x0001'
        assert ip_version == '0x0001' or ip_version == '0x001c'
        ip_version = 'v4' if ip_version == '0x0001' else 'v6'

        contents.remove(content)
        contents.append((domain, class_type, ip_version))
        content_domain, content_class_type, content_ip_version = content

        content_dir = './content_%s_%s' % (domain, ip_version)
        if not os.path.isdir(content_dir):
            os.makedirs(content_dir)
        
        # MAJOR HYPOTHESIS: I assume that for each transaction there is a
        #                   request and a corresponding response
        transaction_is_open_status = dict()
        open_user_transactions = list()
        in_view_traces = list()
        in_view_pending_traces_queue = list()
        ex_view_traces = list()
        ex_view_pending_traces_queue = list()

        for i in range(9):
            trace_file = '%s%s' % (trace_files_path_prefix, str(i))
            
            if i == 0:
                print 'Processing content "%s":'
            print '\tCurrently processing file %s' % trace_file

            with open(trace_file) as fp:

                for line in fp:
                    trace_args = line.rstrip().split('\t')

                    trace_domain = trace_args[7]
                    trace_class_type = trace_args[8]
                    trace_ip_version = trace_args[9]

                    if trace_domain == content_domain and \
                       trace_class_type == content_class_type and \
                       trace_ip_version == content_ip_version:
                           
                        trace_rec = ''
#                        trace_rec = './%s%s:%s\t' % (trace_files_name_prefix, str(i), trace_args[0])
#                        trace_rec += '%s\t%s\t%s\t%s\t%s\t' % (trace_args[1], trace_args[2], trace_args[3], trace_args[4], trace_args[5])
#                        trace_rec += '%s\t%s\t%s\t%s\t%s\t' % (trace_args[6], trace_args[7], trace_args[8], trace_args[9], trace_args[10])
#                        try:
#                            trace_rec += '%s\n' % trace_args[13]
#                        except IndexError:
#                            trace_rec += '\n'

#                       datetime = trace_args[1]
#                       acc_secs_since_epoch = convert_datetime_to_secs(datetime)
                        src = trace_args[2]
                        dest = trace_args[3]
                        dns_is_dest = trace_args[3] == 'dns2-sop'
                        transaction_id = trace_args[4]
                        is_request = not int(trace_args[5])
                        answers_count = int(trace_args[6])
                        is_user = False
                        is_internal = None

                        # TODO: check the filtering of name resolution queries
                        # procedure for any cases that it can fail

                        if dns_is_dest:
                            if transaction_id in transaction_is_open_status:
                                is_open = transaction_is_open_status[transaction_id]
                                is_user = not is_open
                            else:
                                is_user = True
                                
                            if is_user:
                                is_internal = bool(int(trace_args[10]))

                        else:
                            if transaction_id in transaction_is_open_status:
                                is_open = transaction_is_open_status[transaction_id]
                                is_user = not not is_open
                            else:
                                is_user = False

                            if is_user:
                                is_internal = bool(int(trace_args[11]))


                        if is_user and is_request:
                            
                            # mapping
                            open_user_transactions.append((src, transaction_id, None))
                            transaction_is_open_status[transaction_id] = True
                            
                            # writing
                            if is_internal:
                                if not in_view_pending_traces_queue:
                                    in_view_traces.append(trace_rec)
                                else:
                                    pending = False
                                    in_view_pending_traces_queue.append((trace_rec, transaction_id, pending))
                            else:
                                if not ex_view_pending_traces_queue:
                                    ex_view_traces.append(trace_rec)
                                else:
                                    pending = False
                                    ex_view_pending_traces_queue.append((trace_rec, transaction_id, pending))
                            
                        elif is_user and not is_request:
                            
                            # mapping
                            for open_trans in list(open_user_transactions):
                                open_trans_dest, open_trans_id, _ = open_trans
                                if open_trans_dest == dest and open_trans_id == transaction_id:
                                    open_user_transactions.remove(open_trans)
                            transaction_is_open_status[transaction_id] = False

                            # writing
                            if is_internal:
                                if not in_view_pending_traces_queue:
                                    in_view_traces.append(trace_rec)
                                else:
                                    pending= False
                                    in_view_pending_traces_queue.append((trace_rec, transaction_id, pending))
                            elif not is_internal:
                                if not ex_view_pending_traces_queue:
                                    ex_view_traces.append(trace_rec)
                                else:
                                    pending = False
                                    ex_view_pending_traces_queue.append((trace_rec, transaction_id, pending))
                            else:
                                assert False

                        elif not is_user and is_request:
                            
                            # mapping
                            assert len(set([True for _, _, trans_id2 in open_user_transactions if trans_id2 is None])) == 1, "%s\n%s" % (line, open_user_transactions)
                            last_open_transaction = open_user_transactions[-1]
                            src, trans_id1, trans_id2 = last_open_transaction
                            assert trans_id2 is None
                            open_user_transactions[-1] = (src, trans_id1, transaction_id)
                            transaction_is_open_status[transaction_id] = True
                            
                            # writing
                            if is_internal:
                                pass
                            elif not is_internal:
                                pass
                            else:
                                assert False
                            
                        elif not is_request and not is_user:
                            
                            # mapping
                            if answers_count == 0:
                                for j, open_trans in enumerate(open_user_transactions):
                                    open_trans_dest, open_trans_id, associated_dns_trans_id = open_trans
                                    if associated_dns_trans_id == transaction_id:
                                        open_user_transactions[j] = (open_trans_dest, open_trans_id, None)
                                        break
                            transaction_is_open_status[transaction_id] = False
                            
                            # writing
                            if is_internal:
                                pass
                            elif not is_internal:
                                pass
                            else:
                                assert False
                                

                        # check the following: the transaction is not always consist of 2 operations. maybe even 4
#                        if dns_is_dest:
#                            if transaction in transactions:
#                                is_over, old_frame_num, file_num = transactions[transaction]
#                                assert is_over and ((file_num == i and frame_num - old_frame_num > 1000) or file_num != i), [is_over, file_num == i, frame_num - old_frame_num > 1000, file_num != i]
#
#                            transactions[transaction] = (False, frame_num, i)
#                        else:
#                            if transaction in transactions:
#                                is_over, old_frame_num, file_num = transactions[transaction]
#                                assert not is_over and ((file_num == i and frame_num - old_frame_num < 1000) or file_num != i), trace_args
#                                transactions[transaction] = (True, frame_num, i)
#                            else:
#                                assert i == 0 and frame_num < 1000, trace_args

#                        src_is_internal = int(trace_args[11])
#                        dest_is_internal = int(trace_args[12])
#                        subview_is_internal = src_is_internal and dest_is_internal
#
#                        if subview_is_internal:
#                            for_internal_subview.append(line_to_be_written)
#                        else:
#                            for_external_subview.append(line_to_be_written)
#
#        if for_internal_subview:
#            internal_subview_dir = '%s/%s' % (content_dir, 'internal_subview')
#            if not os.path.isdir(internal_subview_dir):
#                os.makedirs(internal_subview_dir)
#            internal_subview_raw_data_file = '%s/raw_data_file_%s_%s' % (internal_subview_dir, domain, ip_version)
#            
#            with open(internal_subview_raw_data_file, 'w') as tfp:
#                for line in for_internal_subview:
#                    tfp.write(line)
#
#        if for_external_subview:
#            external_subview_dir = '%s/%s' % (content_dir, 'external_subview')
#            if not os.path.isdir(external_subview_dir):
#                os.makedirs(external_subview_dir)
#            external_subview_raw_data_file = '%s/raw_data_file_%s_%s' % (external_subview_dir, domain, ip_version)
#            
#            with open(external_subview_raw_data_file, 'w') as tfp:
#                for line in for_external_subview:
#                    tfp.write(line)
#
#    assert contents_len == len(contents)
#    
#    for content in contents:
#        domain, class_type, ip_version = content
#        
#        internal_subview_raw_data_file = './content_%s_%s/internal_subview/raw_data_file_%s_%s' % (domain, ip_version)*2        
#        if os.path.isfile(internal_subview_raw_data_file):
#            with open(internal_subview_raw_data_file) as fp:
#                for line in fp:
#                    trace_args = line.rstrip().split('\t')
#                    
#                    source = trace_args[2]
#                    destination = trace_args[3]
#                    is_response = trace_args[5]
#                    
#                    datetime = trace_args[1]
#                    acc_secs_since_epoch = convert_datetime_to_secs(datetime)
#                    try:
#                        ttl = trace_args[11]
#                    except IndexError:
#                        ttl = None
#
#                    if source == 'dns2-sop' and is_response == 1:
#                        filename = './content_%s_%s/external_subview/res_arr_file_%s_%s' % (domain, ip_version)*2
#                        to_be_written = '%s\n' % acc_secs_since_epoch
#                        with open(filename, 'a') as tfp:
#                            tfp.write(to_be_written)
#                    elif source == 'dns2-sop' and is_response == 0:
#                        filename = ''
#                        to_be_written = ''
#                        with open(filename, 'a') as tfp:
#                            tfp.write(to_be_written)
#                    elif destination == 'dns2-sop' and is_response == 1:
#                        filename = ''
#                        to_be_written = ''
#                        with open(filename, 'a') as tfp:
#                            tfp.write(to_be_written)
#                    elif destination == 'dns2-sop' and is_response == 0:
#                        filename = ''
#                        to_be_written = ''
#                        with open(filename, 'a') as tfp:
#                            tfp.write(to_be_written)
#        
#        external_subview_raw_data_file = './content_%s_%s/external_subview/raw_data_file_%s_%s' % (domain, ip_version)*2
#        if os.path.isfile(external_subview_raw_data_file):
#            with open(external_subview_raw_data_file) as fp:
#                for line in fp:
#                    trace_args = line.rstrip().split('\t')
#                    source = trace_args[2]
#                    destination = trace_args[3]
#                    is_response = trace_args[5]
#                    
#                    if source == 'dns2-sop' and is_response == 1:
#                        pass
#                    elif source == 'dns2-sop' and is_response == 0:
#                        pass
#                    elif destination == 'dns2-sop' and is_response == 1:
#                        pass
#                    elif destination == 'dns2-sop' and is_response == 0:
#                        pass
