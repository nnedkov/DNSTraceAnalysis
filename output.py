#####################################
#   Filename: output.py             #
#   Nedko Stefanov Nedkov           #
#   nedko.nedkov@inria.fr           #
#   April 2014                      #
#####################################

from config import RUNNING_ON_HADOOP, SEPARATOR_2, SEPARATOR_1

from sys import stdout
import os



def dump_data(output, filepath):
    if RUNNING_ON_HADOOP:
        separator = '\n%s%s' % (filepath, SEPARATOR_2)
        formatted_output = '%s%s%s\n' % (filepath,
                                         SEPARATOR_2,
                                         separator.join(output))
        stdout.write(formatted_output)
    else:
        formatted_output = '%s%s' % ('\n'.join(output), '\n')

        with open(filepath, 'a') as fp:
            fp.write(formatted_output)


def dump_cluster(cluster, filepath, in_secs):
    output = list()

    for trace_rec in cluster:
        line = ''

        if in_secs:
            line += trace_rec.secs
        else:
            line += trace_rec.datetime

        if trace_rec.ttl is not None:
            line += '%s%s' % (SEPARATOR_1, trace_rec.ttl)

        output.append(line)

    dump_data(output, filepath)


def dump_users(users, root_dirname, is_internal_view):
    view_dirname = 'internal_view' if is_internal_view else 'external_view'
    dirpath = '%s/%s' % (root_dirname, view_dirname)
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)

    filepath = '%s/users.txt' % dirpath

    dump_data(users, filepath)
