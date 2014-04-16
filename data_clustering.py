####################################
#   Filename: data_clustering.py   #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

''' docstring to be filled '''

from config import trace_files_dir, trace_files_name_prefix, processors_num

import os
from Queue import Queue
from threading import Thread
from time import strptime
from calendar import timegm

trace_records = list()
trace_records_num = 0
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
    time_obj = strptime(datetime, "%b %d, %Y %H:%M:%S.%f000")   # the object loses the millisecs
    secs_since_epoch = timegm(time_obj)
    acc_secs_since_epoch = '%s.%s' % (secs_since_epoch, millisecs)

    return acc_secs_since_epoch


def process_contents(contents, queue):

    def _process_contents(contents):

        for content in list(contents):

            print 'Processing content "%s":' % str(content)

            domain, class_type, ip_version = content

            domain = domain[1:]
            assert domain.isdigit()
    #        assert class_type == '0x0001'
            assert ip_version == '0x0001' or ip_version == '0x001c'
            ip_version = 'v4' if ip_version == '0x0001' else 'v6'

            contents.remove(content)
            contents.append((domain, class_type, ip_version))
            content_domain, content_class_type, content_ip_version = content

            content_dir = './content_%s_%s_%s' % (domain, ip_version, class_type)
            if not os.path.isdir(content_dir):
                os.makedirs(content_dir)

            # MAJOR HYPOTHESIS: I assume that for each transaction there is one
            #                   request and her corresponding response
            trace_types_num = dict()
            transaction_status = dict()
            invalid_operations = list()
            open_user_transactions = list()
            int_view_traces = list()
            int_view_pending_traces_queue = list()
            ext_view_traces = list()
            ext_view_pending_traces_queue = list()

            for i in range(trace_records_num):
                line = trace_records[i]
                trace_args = line.rstrip().split('\t')

                trace_domain = trace_args[7]
                trace_class_type = trace_args[8]
                trace_ip_version = trace_args[9]

                if trace_domain == content_domain and \
                   trace_class_type == content_class_type and \
                   trace_ip_version == content_ip_version:

                    datetime = trace_args[1]
                    src = trace_args[2]
                    dest = trace_args[3]
                    transaction_id = trace_args[4]
                    is_request = not int(trace_args[5])
                    answers_count = int(trace_args[6])
                    try:
                        ttl = trace_args[13]
                    except IndexError:
                        ttl = None

                    acc_secs_since_epoch = convert_datetime_to_secs(datetime)
                    assert src == 'dns2-sop' or dest == 'dns2-sop'
                    dns_is_dest = dest == 'dns2-sop'
                    is_internal = None

                    # (1) req_arr
                    if is_request and dns_is_dest:
                        trace_type = 1
                        assert ttl is None
                    # (2) req_miss
                    elif is_request and not dns_is_dest:
                        trace_type = 2
                        assert ttl is None
                    # (3) res_miss
                    elif not is_request and dns_is_dest:
                        trace_type = 3
                        assert ttl is not None
                    # (4) res_arr
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

                    # handling the duplicated req/res
                    # (which carry the same transaction_id)
                    if dns_is_dest:
                        if transaction_id in transaction_status:
                            is_open, _, caution_data = transaction_status[transaction_id]
                            cd_is_user, cd_src, cd_secs = caution_data

                            if is_open and cd_is_user == True and cd_src == src and \
                               float(acc_secs_since_epoch)-float(cd_secs) < 1:
                                trace_types_num[trace_type] -= 1
                                invalid_operations.append(datetime)
                                continue

                            if not is_open and cd_is_user == True and cd_src == src and \
                               float(acc_secs_since_epoch)-float(cd_secs) < 1 and answers_count > 0:
                                trace_types_num[trace_type] -= 1
                                invalid_operations.append(datetime)
                                continue

                    else:
                        if transaction_id in transaction_status:
                            is_open, _, caution_data = transaction_status[transaction_id]
                            cd_is_user, cd_src, cd_secs = caution_data

                            if not is_open and cd_is_user == True and cd_src == dest and \
                               float(acc_secs_since_epoch)-float(cd_secs) < 1:
                                trace_types_num[trace_type] -= 1
                                invalid_operations.append(datetime)
                                continue

                    # TODO: check the filtering of name resolution queries
                    #       procedure for any cases that it can fail

                    # (1) req_arr
                    if trace_type == 1:

                        # mapping
                        open_user_transactions.append([src, transaction_id, None])
                        is_open = is_user = True
                        is_internal = bool(int(trace_args[11]))
                        transaction_status[transaction_id] = [is_open, is_internal, [is_user, src, acc_secs_since_epoch]]

                        # writing
                        if is_internal:
                            if not int_view_pending_traces_queue:
                                int_view_traces.append(trace_rec)
                            else:
                                pending = False
                                int_view_pending_traces_queue.append([trace_rec, transaction_id, pending])
                        else:
                            if not ext_view_pending_traces_queue:
                                ext_view_traces.append(trace_rec)
                            else:
                                pending = False
                                ext_view_pending_traces_queue.append([trace_rec, transaction_id, pending])

                    # (2) req_miss
                    elif trace_type == 2:

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
                        is_user = False
                        transaction_status[transaction_id] = [is_open, is_internal, [is_user, src, acc_secs_since_epoch]]

                        # writing
                        pending= True
                        if is_internal:
                            int_view_pending_traces_queue.append([trace_rec, transaction_id, pending])
                        else:
                            ext_view_pending_traces_queue.append([trace_rec, transaction_id, pending])

                    # (3) res_miss
                    elif trace_type == 3:

                        # mapping
                        if answers_count == 0:
                            for j, open_user_trans in enumerate(open_user_transactions):
                                ass_trans_id = open_user_trans[2]
                                if ass_trans_id == transaction_id:
                                    open_user_transactions[j][2] = None
                                    break
                        transaction_status[transaction_id][0] = False

                        # writing
                        is_internal = transaction_status[transaction_id][1]
                        if answers_count == 0:
                            if is_internal:
                                for rec in list(int_view_pending_traces_queue):
                                    pending_trans_id = rec[1]
                                    if transaction_id == pending_trans_id:
                                        int_view_pending_traces_queue.remove(rec)
                                        break
                            else:
                                for rec in list(ext_view_pending_traces_queue):
                                    pending_trans_id = rec[1]
                                    if transaction_id == pending_trans_id:
                                        ext_view_pending_traces_queue.remove(rec)
                                        break
                        else:
                            if is_internal:
                                for rec in int_view_pending_traces_queue:
                                    pending_trans_id = rec[1]
                                    if transaction_id == pending_trans_id:
                                        rec[2] = False
                                        break
                                pending = False
                                int_view_pending_traces_queue.append([trace_rec, transaction_id, pending])
                                flush_buffer(int_view_pending_traces_queue, int_view_traces)
                            else:
                                for rec in ext_view_pending_traces_queue:
                                    pending_trans_id = rec[1]
                                    if transaction_id == pending_trans_id:
                                        rec[2] = False
                                        break
                                pending = False
                                ext_view_pending_traces_queue.append([trace_rec, transaction_id, pending])
                                flush_buffer(ext_view_pending_traces_queue, ext_view_traces)

                    # (4) res_arr
                    elif trace_type == 4:

                        # mapping
                        for open_trans in list(open_user_transactions):
                            user_src, user_trans_id, _ = open_trans
                            if user_src == dest and user_trans_id == transaction_id:
                                open_user_transactions.remove(open_trans)
                        transaction_status[transaction_id][0] = False

                        # writing
                        is_internal = bool(int(trace_args[12]))
                        if is_internal:
                            if not int_view_pending_traces_queue:
                                int_view_traces.append(trace_rec)
                            else:
                                pending= False
                                int_view_pending_traces_queue.append([trace_rec, transaction_id, pending])
                        else:
                            if not ext_view_pending_traces_queue:
                                ext_view_traces.append(trace_rec)
                            else:
                                pending = False
                                ext_view_pending_traces_queue.append([trace_rec, transaction_id, pending])

            int_view_type_1_num = len([True for trace_type, data in int_view_traces if trace_type == 1])
            int_view_type_2_num = len([True for trace_type, data in int_view_traces if trace_type == 2])
            int_view_type_3_num = len([True for trace_type, data in int_view_traces if trace_type == 3])
            int_view_type_4_num = len([True for trace_type, data in int_view_traces if trace_type == 4])

            ext_view_type_1_num = len([True for trace_type, data in ext_view_traces if trace_type == 1])
            ext_view_type_2_num = len([True for trace_type, data in ext_view_traces if trace_type == 2])
            ext_view_type_3_num = len([True for trace_type, data in ext_view_traces if trace_type == 3])
            ext_view_type_4_num = len([True for trace_type, data in ext_view_traces if trace_type == 4])

            assert trace_types_num[1] == trace_types_num[4]
            assert trace_types_num[2] == trace_types_num[3]
            assert int_view_type_1_num == int_view_type_4_num
            assert int_view_type_2_num == int_view_type_3_num
            assert ext_view_type_1_num == ext_view_type_4_num
            assert ext_view_type_2_num == ext_view_type_3_num

#        print 'Number of type "1" traces: %s' % trace_types_num[1]
#        print 'Number of type "2" traces: %s' % trace_types_num[2]
#        print 'Number of type "3" traces: %s' % trace_types_num[3]
#        print 'Number of type "4" traces: %s\n\n' % trace_types_num[4]
#
#        print 'Number of type "1" traces: %s' % len([True for trace_type, data in int_view_traces if trace_type == 1])
#        print 'Number of type "2" traces: %s' % len([True for trace_type, data in int_view_traces if trace_type == 2])
#        print 'Number of type "3" traces: %s' % len([True for trace_type, data in int_view_traces if trace_type == 3])
#        print 'Number of type "4" traces: %s\n\n' % len([True for trace_type, data in int_view_traces if trace_type == 4])
#
#        print 'Number of type "1" traces: %s' % len([True for trace_type, data in ext_view_traces if trace_type == 1])
#        print 'Number of type "2" traces: %s' % len([True for trace_type, data in ext_view_traces if trace_type == 2])
#        print 'Number of type "3" traces: %s' % len([True for trace_type, data in ext_view_traces if trace_type == 3])
#        print 'Number of type "4" traces: %s\n\n' % len([True for trace_type, data in ext_view_traces if trace_type == 4])

            if int_view_traces:
                assert not int_view_pending_traces_queue
                int_view_dir = '%s/%s' % (content_dir, 'internal_view')
                if not os.path.isdir(int_view_dir):
                    os.makedirs(int_view_dir)
                int_view_req_arr_file_name = '%s/req_arr_%s_%s_%s.txt' % (int_view_dir, domain, ip_version, class_type)
                int_view_req_miss_file_name = '%s/req_miss_%s_%s_%s.txt' % (int_view_dir, domain, ip_version, class_type)
                int_view_res_arr_file_name = '%s/res_arr_%s_%s_%s.txt' % (int_view_dir, domain, ip_version, class_type)
                int_view_res_miss_file_name = '%s/res_miss_%s_%s_%s.txt' % (int_view_dir, domain, ip_version, class_type)

                trace_type_dict = dict()

                for trace_type, data in int_view_traces:

                    try:
                        trace_type_dict[trace_type].append(data)
                    except KeyError:
                        trace_type_dict[trace_type] = data

                for trace_type in trace_type_dict.keys():

                    if trace_type == 1:
                        with open(int_view_req_arr_file_name, 'w') as fp:
                            for data in trace_type_dict[1]:
                                datetime = data[0]
                                fp.write('%s\n' % datetime)
                    elif trace_type == 2:
                        with open(int_view_req_miss_file_name, 'w') as fp:
                            for data in trace_type_dict[2]:
                                datetime = data[0]
                                fp.write('%s\n' % datetime)
                    elif trace_type == 3:
                        with open(int_view_res_miss_file_name, 'w') as fp:
                            for data in trace_type_dict[3]:
                                datetime = data[0]
                                ttl = data[1]
                                fp.write('%s\t%s\n' % (data[0], data[1]))
                    elif trace_type == 4:
                        with open(int_view_res_arr_file_name, 'w') as fp:
                            for data in trace_type_dict[4]:
                                datetime = data[0]
                                ttl = data[1]
                                fp.write('%s\t%s\n' % (data[0], data[1]))

            if ext_view_traces:
                assert not ext_view_pending_traces_queue
                ext_view_dir = '%s/%s' % (content_dir, 'external_view')
                if not os.path.isdir(ext_view_dir):
                    os.makedirs(ext_view_dir)
                ext_view_req_arr_file_name = '%s/req_arr_%s_%s_%s.txt' % (ext_view_dir, domain, ip_version, class_type)
                ext_view_req_miss_file_name = '%s/req_miss_%s_%s_%s.txt' % (ext_view_dir, domain, ip_version, class_type)
                ext_view_res_arr_file_name = '%s/res_arr_%s_%s_%s.txt' % (ext_view_dir, domain, ip_version, class_type)
                ext_view_res_miss_file_name = '%s/res_miss_%s_%s_%s.txt' % (ext_view_dir, domain, ip_version, class_type)

                trace_type_dict = dict()

                for trace_type, data in int_view_traces:

                    try:
                        trace_type_dict[trace_type].append(data)
                    except KeyError:
                        trace_type_dict[trace_type] = data

                for trace_type in trace_type_dict.keys():

                    if trace_type == 1:
                        with open(ext_view_req_arr_file_name, 'w') as fp:
                            for data in trace_type_dict[1]:
                                datetime = data[0]
                                fp.write('%s\n' % datetime)
                    elif trace_type == 2:
                        with open(ext_view_req_miss_file_name, 'w') as fp:
                            for data in trace_type_dict[2]:
                                datetime = data[0]
                                fp.write('%s\n' % datetime)
                    elif trace_type == 3:
                        with open(ext_view_res_miss_file_name, 'w') as fp:
                            for data in trace_type_dict[3]:
                                datetime = data[0]
                                ttl = data[1]
                                fp.write('%s\t%s\n' % (data[0], data[1]))
                    elif trace_type == 4:
                        with open(ext_view_res_arr_file_name, 'w') as fp:
                            for data in trace_type_dict[4]:
                                datetime = data[0]
                                ttl = data[1]
                                fp.write('%s\t%s\n' % (data[0], data[1]))

            if invalid_operations:
                invalid_operations_log_name = '%s/invalid_operations.log' % content_dir

                with open(invalid_operations_log_name, 'w') as fp:
                    for datatime in invalid_operations:
                        fp.write('%s\n' % datatime)


    result = (True, None)

    try:
        _process_contents(contents)
    except Exception as e:
        result = (False, e)

    queue.put(result)


def chunks(seq, n):
    return (seq[i:i+n] for i in xrange(0, len(seq), n))



if __name__ == '__main__':

    for i in range(9):
        trace_file = '%s%s' % (trace_files_path_prefix, str(i))
        with open(trace_file) as fp:

            for line in fp:
                trace_records.append(line)

    trace_records_num = len(trace_records)

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
#
#    contents = list(contents)
#    contents.remove(('N214', '0x0001', '0x0001'))
#    contents.insert(0, ('N214', '0x0001', '0x0001'))

    #contents = [('N214', '0x0001', '0x0001'), ('N2062', '0x0001', '0x0001'), ('N4992', '0x0001', '0x0001'), ('N11845', '0x0001', '0x0001'), ('N19787', '0x0001', '0x0001'), ('N344', '0x0001', '0x0001')]

    contents = [('N214', '0x0001', '0x0001'), ('N2062', '0x0001', '0x0001')]

    if not contents:
        raise Exception('No contents')

    threads = list()

    # If an exception/error occurs in any of the threads it is
    # not detectable, hence inter-thread communication is used.
    queue = Queue()

    content_chunks = chunks(contents, len(contents)/processors_num)
    for content_chunk in content_chunks:
        threads.append(Thread(target=process_contents, args=(content_chunk, queue)))

    for t in threads:
        t.start()

    for t in threads:
        all_ok, error = queue.get(block=True)
        if not all_ok:
            raise error
        queue.task_done()

    for t in threads:
        t.join()
