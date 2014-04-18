####################################
#   Filename: data_clustering.py   #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

''' docstring to be filled '''

from config import TRACE_FILES_DIR, TRACE_FILES_NAME_PREFIX, \
                   TRACE_FILES_NUMBER, RESULTS_DIR, VERBOSITY_IS_ON

from data_dumping import dump_invalid_data, dump_distinct_users

from trace import Trace
from content import Content

trace_files_path_prefix = '%s%s' % (TRACE_FILES_DIR, TRACE_FILES_NAME_PREFIX)



# TODO: check the filtering of name resolution queries
#       procedure for any cases that it can fail
# NOTICE: it is not taking account users of invalid content
def get_all_content_ids():
    content_ids = set()

    for i in range(TRACE_FILES_NUMBER):
        trace_file = '%s%s' % (trace_files_path_prefix, str(i))

        with open(trace_file) as fp:

            for trace_str in fp:
                trace = Trace(trace_str)
                content_ids.add(trace.content_id)

    content_ids = list(content_ids)
    content_ids.remove(('N214', '0x0001', '0x0001'))
    content_ids.insert(0, ('N214', '0x0001', '0x0001'))


def process_content(content_id):
    if VERBOSITY_IS_ON:
        print 'Processing content with id %s:' % str(content_id)

    content = Content(content_id)

    for i in range(TRACE_FILES_NUMBER):
        trace_file = '%s%s' % (trace_files_path_prefix, str(i))

        if VERBOSITY_IS_ON:
            print '\tProcessing file %s' % str(trace_file)

        with open(trace_file) as fp:

            for trace_str in fp:
                trace = Trace(trace_str)

                if content.is_reffered_in_trace(trace):
                    trace.fill_out()

                    if content.is_trace_duplicate(trace):
                        content.record_invalid_trace(trace)
                        continue

                    try:
                        content.record_trace(trace)
                    except AssertionError:
                        content.is_valid = False

                        return content.get_results()

    try:
        content.assert_clustered_data()
    except AssertionError:
        content.is_valid = False

    content.dump_clustered_data()
    content.dump_invalid_traces()

    return content.get_results()


if __name__ == '__main__':

#    content_ids = get_all_content_ids()
    content_ids = [('N214', '0x0001', '0x0001'), ('N2062', '0x0001', '0x0001'), ('N11845', '0x0001', '0x0001')]#, ('N4992', '0x0001', '0x0001'), ('N19787', '0x0001', '0x0001'), ('N344', '0x0001', '0x0001')]

    if not content_ids:
        raise Exception('No contents')

    invalid_content_ids = list()
    int_users = set()
    ext_users = set()

    for content_id in content_ids:
        cnt_is_valid, cnt_int_users, cnt_ext_users = process_content(content_id)

        if not cnt_is_valid:
            invalid_content_ids.append(content_id)
            continue

        int_users |= cnt_int_users
        ext_users |= cnt_ext_users

    dump_invalid_data(invalid_content_ids, RESULTS_DIR, filename_suffix='contents')

    dump_distinct_users(int_users, RESULTS_DIR, is_internal_view=True)
    dump_distinct_users(ext_users, RESULTS_DIR, is_internal_view=False)
