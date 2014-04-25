####################################
#   Filename: data_dumping.py      #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import RUNNING_ON_HADOOP

import os



def dump_data(output, filename):
    if RUNNING_ON_HADOOP:
        separator = '\n%s' % filename
        output = '%s%s' % (filename, separator.join(output))
        os.sys.stdout.write(output)
    else:
        output = '\n'.join(output)
        with open(filename, 'w') as fp:
            fp.write(output)


def dump_cluster(cluster, filename, in_secs):
    to_write = list()

    for trace in cluster:
        line = ''
        if RUNNING_ON_HADOOP:
            line += filename

        if in_secs:
            line += trace.secs
        else:
            line += trace.datetime

        if trace.ttl is not None:
            line += '\t%s' % trace.ttl

        to_write.append(line)

    dump_data(to_write, filename)


def dump_users(users, root_dir, is_internal_view):
    view_dir = 'internal_view' if is_internal_view else 'external_view'
    res_dir = '%s/%s' % (root_dir, view_dir)
    if not os.path.isdir(res_dir):
        os.makedirs(res_dir)

    filename = '%s/users.txt' % res_dir

    dump_data(users, filename)
