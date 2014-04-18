####################################
#   Filename: data_dumping.py      #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

''' docstring to be filled '''

import os

from time_converter import convert_datetime_to_secs



def dump_results(is_internal_view, traces, content_dir, content_id, in_secs=True):
    view_dir = 'internal_view' if is_internal_view else 'external_view'
    res_dir = '%s/%s' % (content_dir, view_dir)
    if not os.path.isdir(res_dir):
        os.makedirs(res_dir)
    trace_type_dict = dict()
    domain, ip_version, class_type = content_id

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
            dump_data_in_file(filename, trace_recs, in_secs)


def dump_data_in_file(filename, data, in_secs):
    with open(filename, 'w') as fp:
        for elements in data:
            for i, element in enumerate(elements):
                if i == 0:
                    if in_secs:
                        fp.write('%s' % convert_datetime_to_secs(element))
                    else:
                        fp.write('%s' % element)
                else:
                    fp.write('\t%s' % element)

            fp.write('\n')


def dump_invalid_data(invalid_data, content_dir, filename_suffix):
    invalid_data_log_name = '%s/invalid_%s.log' % (content_dir, filename_suffix)

    with open(invalid_data_log_name, 'w') as fp:
        for rec in invalid_data:
            fp.write('%s\n' % rec)


def dump_distinct_users(distinct_users, content_dir, is_internal_view):
    view_dir = 'internal_view' if is_internal_view else 'external_view'
    distinct_users_dir = '%s/%s' % (content_dir, view_dir)
    if not os.path.isdir(distinct_users_dir):
        os.makedirs(distinct_users_dir)

    distinct_users_filename = '%s/distinct_users.txt' % distinct_users_dir

    with open(distinct_users_filename, 'w') as fp:
        for user in distinct_users:
            fp.write('%s\n' % user)
