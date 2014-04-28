####################################
#   Filename: data_dumping.py      #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import RUNNING_ON_HADOOP, SEPARATOR

import os



def dump_data(output, filename):
    if RUNNING_ON_HADOOP:
        separator = '\n%s%s' % (filename, SEPARATOR)
        output = '%s%s%s\n' % (filename, SEPARATOR, separator.join(output))
        os.sys.stdout.write(output)
    else:
        output = '\n'.join(output) + '\n'
        with open(filename, 'w') as fp:
            fp.write(output)


def dump_cluster(cluster, filename, in_secs):
    to_write = list()

    for trace in cluster:
        line = ''

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
