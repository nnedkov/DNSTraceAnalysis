####################################
#   Filename: data_clustering.py   #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

''' docstring to be filled '''

from config import trace_files_dir, trace_files_name_prefix

import os
from time import strptime
from calendar import timegm

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


def assert_results(trace_types_num, int_view_traces, ext_view_traces):
    int_view_type_1_num = len([True for t_type, _ in int_view_traces if t_type == 1])
    int_view_type_2_num = len([True for t_type, _ in int_view_traces if t_type == 2])
    int_view_type_3_num = len([True for t_type, _ in int_view_traces if t_type == 3])
    int_view_type_4_num = len([True for t_type, _ in int_view_traces if t_type == 4])

    ext_view_type_1_num = len([True for t_type, _ in ext_view_traces if t_type == 1])
    ext_view_type_2_num = len([True for t_type, _ in ext_view_traces if t_type == 2])
    ext_view_type_3_num = len([True for t_type, _ in ext_view_traces if t_type == 3])
    ext_view_type_4_num = len([True for t_type, _ in ext_view_traces if t_type == 4])

    assert trace_types_num[1] == trace_types_num[4]
    assert trace_types_num[2] == trace_types_num[3]
    assert int_view_type_1_num == int_view_type_4_num
    assert int_view_type_2_num == int_view_type_3_num
    assert ext_view_type_1_num == ext_view_type_4_num
    assert ext_view_type_2_num == ext_view_type_3_num


def dump_results(is_internal_view, traces, content_dir, updated_content):
    view_dir = 'internal_view' if is_internal_view else 'external_view'
    res_dir = '%s/%s' % (content_dir, view_dir)
    if not os.path.isdir(res_dir):
        os.makedirs(res_dir)
    trace_type_dict = dict()
    domain, ip_version, class_type = updated_content

    for i in [1, 2, 3, 4]:
        trace_type_dict[i] = list()
        if i == 1:
            filename = '%s/req_arr_%s_%s_%s.txt' % (res_dir, domain, ip_version, class_type)
        elif i == 2:
            filename = '%s/req_miss_%s_%s_%s.txt' % (res_dir, domain, ip_version, class_type)
        elif i == 3:
            filename = '%s/res_miss_%s_%s_%s.txt' % (res_dir, domain, ip_version, class_type)
        elif i == 4:
            filename = '%s/res_arr_%s_%s_%s.txt' % (res_dir, domain, ip_version, class_type)
        trace_type_dict[i].append(filename)
        trace_type_dict[i].append(list())

    for trace_type, data in traces:
        trace_recs = trace_type_dict[trace_type][1]
        trace_recs.append(data)

    for key, value in trace_type_dict.iteritems():
        filename, trace_recs = value
        if trace_recs:
            dump_data_in_file(filename, trace_recs)


def dump_data_in_file(filename, data):
    with open(filename, 'w') as fp:
        for elements in data:
            for i, element in enumerate(elements):
                if i == 0:
                    fp.write('%s' % element)
                else:
                    fp.write('\t%s' % element)

            fp.write('\n')


def dump_invalid_traces(invalid_traces, content_dir):
    invalid_operations_log_name = '%s/invalid_operations.log' % content_dir

    with open(invalid_operations_log_name, 'w') as fp:
        for datatime in invalid_traces:
            fp.write('%s\n' % datatime)


def get_trace_args_dict(trace_args):
    trace_args_dict = dict()
    trace_args_dict['trace_args'] = trace_args
    trace_args_dict['datetime'] = trace_args[1]
    trace_args_dict['src'] = trace_args[2]
    trace_args_dict['dest'] = trace_args[3]
    trace_args_dict['transaction_id'] = trace_args[4]
    trace_args_dict['is_request'] = not int(trace_args[5])
    trace_args_dict['answers_count'] = int(trace_args[6])
    try:
        trace_args_dict['ttl'] = trace_args[13]
    except IndexError:
        trace_args_dict['ttl'] = None
    trace_args_dict['acc_secs_since_epoch'] = convert_datetime_to_secs(trace_args_dict['datetime'])
    trace_args_dict['dns_is_dest'] = trace_args_dict['dest'] == 'dns2-sop'

    # (1) req_arr
    if trace_args_dict['is_request'] and trace_args_dict['dns_is_dest']:
        trace_args_dict['trace_type'] = 1
        assert trace_args_dict['ttl'] is None
    # (2) req_miss
    elif trace_args_dict['is_request'] and not trace_args_dict['dns_is_dest']:
        trace_args_dict['trace_type'] = 2
        assert trace_args_dict['ttl'] is None
    # (3) res_miss
    elif not trace_args_dict['is_request'] and trace_args_dict['dns_is_dest']:
        trace_args_dict['trace_type'] = 3
        assert trace_args_dict['ttl'] is not None
    # (4) res_arr
    elif not trace_args_dict['is_request'] and not trace_args_dict['dns_is_dest']:
        trace_args_dict['trace_type'] = 4
        assert trace_args_dict['ttl'] is not None

    return trace_args_dict


def is_duplicated_trace(trace_args_dict, transaction_status):
    if trace_args_dict['dns_is_dest']:
        if trace_args_dict['transaction_id'] in transaction_status:
            is_open, _, caution_data = transaction_status[trace_args_dict['transaction_id']]
            cd_is_user, cd_src, cd_secs = caution_data

            if is_open and cd_is_user == True and cd_src == trace_args_dict['src'] and \
               float(trace_args_dict['acc_secs_since_epoch'])-float(cd_secs) < 1:
                   return True

            if not is_open and cd_is_user == True and cd_src == trace_args_dict['src'] and \
               float(trace_args_dict['acc_secs_since_epoch'])-float(cd_secs) < 1 and trace_args_dict['answers_count'] > 0:
                   return True

    else:
        if trace_args_dict['transaction_id'] in transaction_status:
            is_open, _, caution_data = transaction_status[trace_args_dict['transaction_id']]
            cd_is_user, cd_src, cd_secs = caution_data

            if not is_open and cd_is_user == True and cd_src == trace_args_dict['dest'] and \
               float(trace_args_dict['acc_secs_since_epoch'])-float(cd_secs) < 1:
                   return True


def get_content_metadata():
    cmd = dict()

    cmd['trace_types_num'] = dict()
    cmd['transaction_status'] = dict()
    cmd['invalid_traces'] = list()
    cmd['open_user_transactions'] = list()
    cmd['int_view_traces'] = list()
    cmd['int_view_pending_traces_queue'] = list()
    cmd['ext_view_traces'] = list()
    cmd['ext_view_pending_traces_queue'] = list()

    return cmd


def handle_req_arr(cmd, trace_args_dict, trace_rec):

    # mapping
    cmd['open_user_transactions'].append([trace_args_dict['src'], trace_args_dict['transaction_id'], None])
    is_open = is_user = True
    is_internal = bool(int(trace_args_dict['trace_args'][11]))
    cmd['transaction_status'][trace_args_dict['transaction_id']] = [is_open, is_internal, [is_user, trace_args_dict['src'], trace_args_dict['acc_secs_since_epoch']]]

    # writing
    if is_internal:
        if not cmd['int_view_pending_traces_queue']:
            cmd['int_view_traces'].append(trace_rec)
        else:
            pending = False
            cmd['int_view_pending_traces_queue'].append([trace_rec, trace_args_dict['transaction_id'], pending])
    else:
        if not cmd['ext_view_pending_traces_queue']:
            cmd['ext_view_traces'].append(trace_rec)
        else:
            pending = False
            cmd['ext_view_pending_traces_queue'].append([trace_rec, trace_args_dict['transaction_id'], pending])


def handle_req_miss(cmd, trace_args_dict, trace_rec):

    # mapping
    assert len(set([True for _, _, ass_trans_id in cmd['open_user_transactions'] if ass_trans_id is None])) == 1, "%s" % cmd['open_user_transactions']
    last_open_transaction = cmd['open_user_transactions'][-1]
    _, user_trans_id, ass_trans_id = last_open_transaction
    assert ass_trans_id is None
    cmd['open_user_transactions'][-1][2] = trace_args_dict['transaction_id']
    user_trans_is_open, user_trans_is_internal, _ = cmd['transaction_status'][user_trans_id]
    assert user_trans_is_open
    is_open = True
    is_internal = user_trans_is_internal
    is_user = False
    cmd['transaction_status'][trace_args_dict['transaction_id']] = [is_open, is_internal, [is_user, trace_args_dict['src'], trace_args_dict['acc_secs_since_epoch']]]

    # writing
    pending= True
    if is_internal:
        cmd['int_view_pending_traces_queue'].append([trace_rec, trace_args_dict['transaction_id'], pending])
    else:
        cmd['ext_view_pending_traces_queue'].append([trace_rec, trace_args_dict['transaction_id'], pending])


def handle_res_miss(cmd, trace_args_dict, trace_rec):

    # mapping
    if trace_args_dict['answers_count'] == 0:
        for j, open_user_trans in enumerate(cmd['open_user_transactions']):
            ass_trans_id = open_user_trans[2]
            if ass_trans_id == trace_args_dict['transaction_id']:
                cmd['open_user_transactions'][j][2] = None
                break
    cmd['transaction_status'][trace_args_dict['transaction_id']][0] = False

    # writing
    is_internal = cmd['transaction_status'][trace_args_dict['transaction_id']][1]
    if trace_args_dict['answers_count'] == 0:
        if is_internal:
            for rec in list(cmd['int_view_pending_traces_queue']):
                pending_trans_id = rec[1]
                if trace_args_dict['transaction_id'] == pending_trans_id:
                    cmd['int_view_pending_traces_queue'].remove(rec)
                    break
        else:
            for rec in list(cmd['ext_view_pending_traces_queue']):
                pending_trans_id = rec[1]
                if trace_args_dict['transaction_id'] == pending_trans_id:
                    cmd['ext_view_pending_traces_queue'].remove(rec)
                    break
    else:
        if is_internal:
            for rec in cmd['int_view_pending_traces_queue']:
                pending_trans_id = rec[1]
                if trace_args_dict['transaction_id'] == pending_trans_id:
                    rec[2] = False
                    break
            pending = False
            cmd['int_view_pending_traces_queue'].append([trace_rec, trace_args_dict['transaction_id'], pending])
            flush_buffer(cmd['int_view_pending_traces_queue'], cmd['int_view_traces'])
        else:
            for rec in cmd['ext_view_pending_traces_queue']:
                pending_trans_id = rec[1]
                if trace_args_dict['transaction_id'] == pending_trans_id:
                    rec[2] = False
                    break
            pending = False
            cmd['ext_view_pending_traces_queue'].append([trace_rec, trace_args_dict['transaction_id'], pending])
            flush_buffer(cmd['ext_view_pending_traces_queue'], cmd['ext_view_traces'])


def handle_res_arr(cmd, trace_args_dict, trace_rec):
    # mapping
    for open_trans in list(cmd['open_user_transactions']):
        user_src, user_trans_id, _ = open_trans
        if user_src == trace_args_dict['dest'] and user_trans_id == trace_args_dict['transaction_id']:
            cmd['open_user_transactions'].remove(open_trans)
    cmd['transaction_status'][trace_args_dict['transaction_id']][0] = False

    # writing
    is_internal = bool(int(trace_args_dict['trace_args'][12]))
    if is_internal:
        if not cmd['int_view_pending_traces_queue']:
            cmd['int_view_traces'].append(trace_rec)
        else:
            pending= False
            cmd['int_view_pending_traces_queue'].append([trace_rec, trace_args_dict['transaction_id'], pending])
    else:
        if not cmd['ext_view_pending_traces_queue']:
            cmd['ext_view_traces'].append(trace_rec)
        else:
            pending = False
            cmd['ext_view_pending_traces_queue'].append([trace_rec, trace_args_dict['transaction_id'], pending])


def process_content(content):
    print 'Processing content "%s":' % str(content)

    domain, class_type, ip_version = content

    domain = domain[1:]
    assert domain.isdigit()
#    assert class_type == '0x0001'
    assert ip_version == '0x0001' or ip_version == '0x001c'
    ip_version = 'v4' if ip_version == '0x0001' else 'v6'

    updated_content = (domain, ip_version, class_type)

    content_domain, content_class_type, content_ip_version = content

    content_dir = './content_%s_%s_%s' % (domain, ip_version, class_type)
    if not os.path.isdir(content_dir):
        os.makedirs(content_dir)

    # MAJOR HYPOTHESIS: I assume that for each transaction there is one
    #                   request and her corresponding response

    cmd = get_content_metadata()

    for i in range(9):
        trace_file = '%s%s' % (trace_files_path_prefix, str(i))
        print '\tProcessing file %s' % str(trace_file)

        with open(trace_file) as fp:

            for line in fp:
                trace_args = line.rstrip().split('\t')

                trace_domain = trace_args[7]
                trace_class_type = trace_args[8]
                trace_ip_version = trace_args[9]

                if trace_domain == content_domain and \
                   trace_class_type == content_class_type and \
                   trace_ip_version == content_ip_version:

                    trace_args_dict = get_trace_args_dict(trace_args)

                    try:
                        cmd['trace_types_num'][trace_args_dict['trace_type']] += 1
                    except:
                        cmd['trace_types_num'][trace_args_dict['trace_type']] = 1

                    trace_rec = (trace_args_dict['trace_type'], [trace_args_dict['datetime']])
                    if trace_args_dict['ttl'] is not None:
                        trace_rec[1].append(trace_args_dict['ttl'])

                    # handling the duplicated req/res
                    # (which carry the same transaction_id)
                    if is_duplicated_trace(trace_args_dict, cmd['transaction_status']):
                        cmd['trace_types_num'][trace_args_dict['trace_type']] -= 1
                        cmd['invalid_traces'].append(trace_args_dict['datetime'])
                        continue

                    # TODO: check the filtering of name resolution queries
                    #       procedure for any cases that it can fail

                    # (1) req_arr
                    if trace_args_dict['trace_type'] == 1:
                        handle_req_arr(cmd, trace_args_dict, trace_rec)
                    # (2) req_miss
                    elif trace_args_dict['trace_type'] == 2:
                        handle_req_miss(cmd, trace_args_dict, trace_rec)
                    # (3) res_miss
                    elif trace_args_dict['trace_type'] == 3:
                        handle_res_miss(cmd, trace_args_dict, trace_rec)
                    # (4) res_arr
                    elif trace_args_dict['trace_type'] == 4:
                        handle_res_arr(cmd, trace_args_dict, trace_rec)

            assert_results(cmd['trace_types_num'], cmd['int_view_traces'], cmd['ext_view_traces'])

            if cmd['int_view_traces']:
                assert not cmd['int_view_pending_traces_queue']
                is_internal_view = True
                dump_results(is_internal_view, cmd['int_view_traces'], content_dir, updated_content)

            if cmd['ext_view_traces']:
                assert not cmd['ext_view_pending_traces_queue']
                is_internal_view = False
                dump_results(is_internal_view, cmd['ext_view_traces'], content_dir, updated_content)

            if cmd['invalid_traces']:
                dump_invalid_traces(cmd['invalid_traces'], content_dir)



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
#
#    contents = list(contents)
#    contents.remove(('N214', '0x0001', '0x0001'))
#    contents.insert(0, ('N214', '0x0001', '0x0001'))

    contents = [('N214', '0x0001', '0x0001'), ('N2062', '0x0001', '0x0001'), ('N4992', '0x0001', '0x0001'), ('N11845', '0x0001', '0x0001'), ('N19787', '0x0001', '0x0001'), ('N344', '0x0001', '0x0001')]

    if not contents:
        raise Exception('No contents')

    for content in contents:
        process_content(content)
