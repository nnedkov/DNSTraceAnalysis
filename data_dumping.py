####################################
#   Filename: data_dumping.py      #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

import os



def dump_cluster_in_file(cluster, filename, in_secs):
    with open(filename, 'w') as fp:

        for trace in cluster:
            to_write = trace.secs_since_epoch if in_secs else trace.datetime
            if trace.ttl is not None:
                to_write += '\t%s' % trace.ttl

            fp.write('%s\n' % to_write)


def dump_invalid_data(invalid_data, content_dir, filename):
    invalid_data_log_name = '%s/%s.log' % (content_dir, filename)

    with open(invalid_data_log_name, 'w') as fp:
        for rec in invalid_data:
            fp.write('%s\n' % str(rec))


def dump_distinct_users(distinct_users, content_dir, is_internal_view):
    view_dir = 'internal_view' if is_internal_view else 'external_view'
    distinct_users_dir = '%s/%s' % (content_dir, view_dir)
    if not os.path.isdir(distinct_users_dir):
        os.makedirs(distinct_users_dir)

    distinct_users_filename = '%s/distinct_users.txt' % distinct_users_dir

    with open(distinct_users_filename, 'w') as fp:
        for user in distinct_users:
            fp.write('%s\n' % user)
