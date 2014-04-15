####################################
#   Filename: data_clustering.py   #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

''' docstring to be filled '''

import os
from time import strptime
from calendar import timegm

trace_files_dir = '/home/nedko/Inria/DNS_traces/'
trace_files_name_prefix = 'dns2-sop-00'
trace_files_path_prefix = '%s%s' % (trace_files_dir, trace_files_name_prefix)



def flush_buffer(trace_buffer, trace_dest):
    for rec in list(trace_buffer):
        trace_rec, transaction_id, pending = rec
        if pending == True:
            break
        trace_dest.append(trace_rec)
        trace_buffer.remove(rec)


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

    contents = [('N214', '0x0001', '0x0001'), ('N2062', '0x0001', '0x0001'), ('N4992', '0x0001', '0x0001'), ('N11845', '0x0001', '0x0001'), ('N19787', '0x0001', '0x0001'), ('N344', '0x0001', '0x0001')]
#    contents_len = len(contents)

    for content in list(contents):
        domain, class_type, ip_version = content

        domain = domain[1:]
#        assert class_type == '0x0001'
        assert ip_version == '0x0001' or ip_version == '0x001c'
        ip_version = 'v4' if ip_version == '0x0001' else 'v6'

        contents.remove(content)
        contents.append((domain, class_type, ip_version))
        content_domain, content_class_type, content_ip_version = content

        content_dir = './content_%s_%s_%s' % (domain, ip_version, class_type)
        if not os.path.isdir(content_dir):
            os.makedirs(content_dir)

        # MAJOR HYPOTHESIS: I assume that for each transaction there is a
        #                   request and a corresponding response
        transaction_status = dict()
        trace_types_num = dict()
        open_user_transactions = list()
        invalid_operations = list()
        in_view_traces = list()
        in_view_pending_traces_queue = list()
        ex_view_traces = list()
        ex_view_pending_traces_queue = list()

        for i in range(9):
            trace_file = '%s%s' % (trace_files_path_prefix, str(i))

            if i == 0:
                print 'Processing content "%s":' % str(content)
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

                        #print '%s\n%s\n%s\n%s\n\n%s\n\n\n' % (in_view_traces, in_view_pending_traces_queue, ex_view_traces, ex_view_pending_traces_queue, open_user_transactions)

                        datetime = trace_args[1]
                        acc_secs_since_epoch = convert_datetime_to_secs(datetime)
                        src = trace_args[2]
                        dest = trace_args[3]
                        assert src == 'dns2-sop' or dest == 'dns2-sop'
                        dns_is_dest = dest == 'dns2-sop'
                        transaction_id = trace_args[4]
                        is_request = not int(trace_args[5])
                        answers_count = int(trace_args[6])
                        try:
                            ttl = trace_args[13]
                        except IndexError:
                            ttl = None
                        is_user = False
                        is_internal = None

                        # (trace_type, [secs_since_epoch, ttl])
                        if is_request and dns_is_dest:
                            trace_type = 1
                            assert ttl is None
                        elif is_request and not dns_is_dest:
                            trace_type = 2
                            assert ttl is None
                        elif not is_request and dns_is_dest:
                            trace_type = 3
                            assert ttl is not None
                        elif not is_request and not dns_is_dest:
                            trace_type = 4
                            assert ttl is not None

                        try:
                            trace_types_num[trace_type] += 1
                        except:
                            trace_types_num[trace_type] = 1

                        trace_rec = (trace_type, [datetime])
                        if ttl is not None:
                            trace_rec[1].append(ttl)

                        # TODO: check the filtering of name resolution queries
                        # procedure for any cases that it can fail

                        if dns_is_dest:
                            if transaction_id in transaction_status:
                                is_open, _, caution_data = transaction_status[transaction_id]

                                cd_is_user, cd_src, cd_secs = caution_data
                                if is_open and cd_is_user == True and cd_src == src and \
                                   acc_secs_since_epoch-cd_secs < 3:
                                    trace_types_num[trace_type] -= 1
                                    invalid_operations.append(datetime)
                                    continue
                                if not is_open and cd_is_user == True and cd_src == src and \
                                   acc_secs_since_epoch-cd_secs < 3 and answers_count > 0:
                                    trace_types_num[trace_type] -= 1
                                    invalid_operations.append(datetime)
                                    continue

                                is_user = not is_open
                            else:
                                is_user = True

                            if is_user:
                                is_internal = bool(int(trace_args[11]))

                        else:
                            if transaction_id in transaction_status:
                                is_open, _, caution_data = transaction_status[transaction_id]

                                cd_is_user, cd_src, cd_secs = caution_data
                                if not is_open and cd_is_user == True and cd_src == dest and \
                                   acc_secs_since_epoch-cd_secs < 3:
                                    trace_types_num[trace_type] -= 1
                                    invalid_operations.append(datetime)
                                    continue

                                is_user = not not is_open
                            else:
                                is_user = False

                            if is_user:
                                is_internal = bool(int(trace_args[12]))


                        #assert not (src == 'H153818' and transaction_id == '0x90a8' and trace_domain == 'N214' and trace_args[0] == '4310608'), [transaction_status[transaction_id], is_open, is_internal, is_user, is_request]
#                        if answers_count > 0 and ttl is not None:
#                            assert False, trace_args
#                            invalid_requests.append(datetime)
#                            continue

                        if is_user and is_request:

                            # mapping
                            open_user_transactions.append([src, transaction_id, None])
                            is_open = True
                            transaction_status[transaction_id] = [is_open, is_internal, [is_user, src, acc_secs_since_epoch]]

                            # writing
                            if is_internal == True:
                                if not in_view_pending_traces_queue:
                                    in_view_traces.append(trace_rec)
                                else:
                                    pending = False
                                    in_view_pending_traces_queue.append([trace_rec, transaction_id, pending])
                            elif is_internal == False:
                                if not ex_view_pending_traces_queue:
                                    ex_view_traces.append(trace_rec)
                                else:
                                    pending = False
                                    ex_view_pending_traces_queue.append([trace_rec, transaction_id, pending])
                            else:
                                assert False

                        elif is_user and not is_request:

                            # mapping
                            for open_trans in list(open_user_transactions):
                                user_src, user_trans_id, _ = open_trans
                                if user_src == dest and user_trans_id == transaction_id:
                                    open_user_transactions.remove(open_trans)
                            transaction_status[transaction_id][0] = False

                            # writing
                            if is_internal == True:
                                if not in_view_pending_traces_queue:
                                    in_view_traces.append(trace_rec)
                                else:
                                    pending= False
                                    in_view_pending_traces_queue.append([trace_rec, transaction_id, pending])
                            elif is_internal == False:
                                if not ex_view_pending_traces_queue:
                                    ex_view_traces.append(trace_rec)
                                else:
                                    pending = False
                                    ex_view_pending_traces_queue.append([trace_rec, transaction_id, pending])
                            else:
                                assert False

                        elif not is_user and is_request:

                            # mapping
                            assert len(set([True for _, _, ass_trans_id in open_user_transactions if ass_trans_id is None])) == 1, "%s\n%s" % (line, open_user_transactions)
                            last_open_transaction = open_user_transactions[-1]
                            _, user_trans_id, ass_trans_id = last_open_transaction
                            assert ass_trans_id is None
                            open_user_transactions[-1][2] = transaction_id
                            user_trans_is_open, user_trans_is_internal, _ = transaction_status[user_trans_id]
                            assert user_trans_is_open
                            is_open = True
                            is_internal = user_trans_is_internal
                            transaction_status[transaction_id] = [is_open, is_internal, [is_user, src, acc_secs_since_epoch]]   # I am not sure if there is point in keeping the 3rd data

                            # writing
                            if is_internal == True:
                                pending= True
                                in_view_pending_traces_queue.append([trace_rec, transaction_id, pending])
                            elif is_internal == False:
                                pending= True
                                ex_view_pending_traces_queue.append([trace_rec, transaction_id, pending])
                            else:
                                assert False

                        elif not is_request and not is_user:

                            # mapping
                            if answers_count == 0:
                                for j, open_trans in enumerate(open_user_transactions):
                                    ass_trans_id = open_trans[2]
                                    if ass_trans_id == transaction_id:
                                        open_user_transactions[j][2] = None
                                        break
                            transaction_status[transaction_id][0] = False
                            is_internal = transaction_status[transaction_id][1]

                            #print '%s\n%s\n%s\n%s\n%s\n' % (is_internal, in_view_pending_traces_queue, ex_view_pending_traces_queue, open_user_transactions, [len(in_view_traces), len(ex_view_traces)])

                            # writing
                            if answers_count == 0:
                                if is_internal == True:
                                    for rec in list(in_view_pending_traces_queue):
                                        pending_trans_id = rec[1]
                                        if transaction_id == pending_trans_id:
                                            in_view_pending_traces_queue.remove(rec)
                                            break
                                    flush_buffer(in_view_pending_traces_queue, in_view_traces)
                                elif is_internal == False:
                                    for rec in list(ex_view_pending_traces_queue):
                                        pending_trans_id = rec[1]
                                        if transaction_id == pending_trans_id:
                                            ex_view_pending_traces_queue.remove(rec)
                                            break
                                    flush_buffer(ex_view_pending_traces_queue, ex_view_traces)
                                else:
                                    assert False
                            else:
                                if is_internal == True:
                                    for rec in in_view_pending_traces_queue:
                                        pending_trans_id = rec[1]
                                        if transaction_id == pending_trans_id:
                                            rec[2] = False
                                            break
                                    pending = False
                                    in_view_pending_traces_queue.append([trace_rec, transaction_id, pending])
                                    flush_buffer(in_view_pending_traces_queue, in_view_traces)
                                elif is_internal == False:
                                    for rec in ex_view_pending_traces_queue:
                                        pending_trans_id = rec[1]
                                        if transaction_id == pending_trans_id:
                                            rec[2] = False
                                            break
                                    pending = False
                                    ex_view_pending_traces_queue.append([trace_rec, transaction_id, pending])
                                    flush_buffer(ex_view_pending_traces_queue, ex_view_traces)
                                else:
                                    assert False

                            #print '%s\n%s\n%s\n%s\n%s\n\n' % (is_internal, in_view_pending_traces_queue, ex_view_pending_traces_queue, open_user_transactions, [len(in_view_traces), len(ex_view_traces), trace_type])

        print 'Number of type "1" traces: %s' % trace_types_num[1]
        print 'Number of type "2" traces: %s' % trace_types_num[2]
        print 'Number of type "3" traces: %s' % trace_types_num[3]
        print 'Number of type "4" traces: %s\n\n' % trace_types_num[4]

        print 'Number of type "1" traces: %s' % len([True for trace_type, data in in_view_traces if trace_type == 1])
        print 'Number of type "2" traces: %s' % len([True for trace_type, data in in_view_traces if trace_type == 2])
        print 'Number of type "3" traces: %s' % len([True for trace_type, data in in_view_traces if trace_type == 3])
        print 'Number of type "4" traces: %s\n\n' % len([True for trace_type, data in in_view_traces if trace_type == 4])

        print 'Number of type "1" traces: %s' % len([True for trace_type, data in ex_view_traces if trace_type == 1])
        print 'Number of type "2" traces: %s' % len([True for trace_type, data in ex_view_traces if trace_type == 2])
        print 'Number of type "3" traces: %s' % len([True for trace_type, data in ex_view_traces if trace_type == 3])
        print 'Number of type "4" traces: %s\n\n' % len([True for trace_type, data in ex_view_traces if trace_type == 4])


        if in_view_traces:
            assert not in_view_pending_traces_queue
            in_view_dir = '%s/%s' % (content_dir, 'internal_view')
            if not os.path.isdir(in_view_dir):
                os.makedirs(in_view_dir)
            in_view_req_arr_file_name = '%s/req_arr_%s_%s_%s.txt' % (in_view_dir, domain, ip_version, class_type)
            in_view_req_miss_file_name = '%s/req_miss_%s_%s_%s.txt' % (in_view_dir, domain, ip_version, class_type)
            in_view_res_arr_file_name = '%s/res_arr_%s_%s_%s.txt' % (in_view_dir, domain, ip_version, class_type)
            in_view_res_miss_file_name = '%s/res_miss_%s_%s_%s.txt' % (in_view_dir, domain, ip_version, class_type)

            for trace_type, data in in_view_traces:
                if trace_type == 1:
                    with open(in_view_req_arr_file_name, 'a') as fp:
                        fp.write('%s\n' % data[0])
                elif trace_type == 2:
                    with open(in_view_req_miss_file_name, 'a') as fp:
                        fp.write('%s\n' % data[0])
                elif trace_type == 3:
                    with open(in_view_res_miss_file_name, 'a') as fp:
                        fp.write('%s\t%s\n' % (data[0], data[1]))
                        #fp.write('%s\n' % data[0])
                elif trace_type == 4:
                    with open(in_view_res_arr_file_name, 'a') as fp:
                        fp.write('%s\t%s\n' % (data[0], data[1]))
                        #fp.write('%s\n' % data[0])

        if ex_view_traces:
            assert not ex_view_pending_traces_queue
            ex_view_dir = '%s/%s' % (content_dir, 'external_view')
            if not os.path.isdir(ex_view_dir):
                os.makedirs(ex_view_dir)
            ex_view_req_arr_file_name = '%s/req_arr_%s_%s_%s.txt' % (ex_view_dir, domain, ip_version, class_type)
            ex_view_req_miss_file_name = '%s/req_miss_%s_%s_%s.txt' % (ex_view_dir, domain, ip_version, class_type)
            ex_view_res_arr_file_name = '%s/res_arr_%s_%s_%s.txt' % (ex_view_dir, domain, ip_version, class_type)
            ex_view_res_miss_file_name = '%s/res_miss_%s_%s_%s.txt' % (ex_view_dir, domain, ip_version, class_type)

            for trace_type, data in ex_view_traces:
                if trace_type == 1:
                    with open(ex_view_req_arr_file_name, 'a') as fp:
                        fp.write('%s\n' % data[0])
                elif trace_type == 2:
                    with open(ex_view_req_miss_file_name, 'a') as fp:
                        fp.write('%s\n' % data[0])
                elif trace_type == 3:
                    with open(ex_view_res_arr_file_name, 'a') as fp:
                        fp.write('%s\t%s\n' % (data[0], data[1]))
                elif trace_type == 4:
                    with open(ex_view_res_miss_file_name, 'a') as fp:
                        fp.write('%s\t%s\n' % (data[0], data[1]))

        if invalid_operations:
            invalid_operations_log_name = '%s/invalid_operations.log' % content_dir

            with open(invalid_operations_log_name, 'w') as fp:
                for datatime in invalid_operations:
                    fp.write('%s\n' % datatime)
