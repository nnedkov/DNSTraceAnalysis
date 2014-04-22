####################################
#   Filename: data_clustering.py   #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import TRACE_FILES_DIR, TRACE_FILES_NAME_PREFIX, \
                   TRACE_FILES_NUMBER, VERBOSITY_IS_ON, RESULTS_DIR

from trace import Trace
from content import Content
from data_dumping import dump_invalid_data, dump_distinct_users

trace_files_path_prefix = '%s%s' % (TRACE_FILES_DIR, TRACE_FILES_NAME_PREFIX)



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


def process_traces_for_content(content_id, traces):
    content = Content(content_id)

    for trace in traces:
        process_next = content.process_trace(trace)

        if not process_next:
            return content.get_results()

    content.assert_and_dump_clusters()

    return content.get_results()


def process_content(content_id):
    if VERBOSITY_IS_ON:
        print 'Processing content with id %s:' % str(content_id)

    content = Content(content_id)
    traces = list()

    for i in range(TRACE_FILES_NUMBER):
        trace_file = '%s%s' % (trace_files_path_prefix, str(i))

        if VERBOSITY_IS_ON:
            print '\tFiltering traces from file %s' % str(trace_file)

        with open(trace_file) as fp:

            for trace_str in fp:
                trace = Trace(trace_str)

                if content.is_reffered_in_trace(trace):
                    traces.append(trace)

    return process_traces_for_content(content_id, traces)


def main():
#    content_ids = get_all_content_ids()
    content_ids = [('N214', '0x0001', '0x0001'), ('N2062', '0x0001', '0x0001'), ('N11845', '0x0001', '0x0001')]#, ('N4992', '0x0001', '0x0001'), ('N19787', '0x0001', '0x0001'), ('N344', '0x0001', '0x0001')]

    if not content_ids:
        raise Exception('No contents')

    invalid_content_ids = list()
    all_int_users = set()
    all_ext_users = set()

    for content_id in content_ids:
        is_valid, int_users, ext_users = process_content(content_id)

        if not is_valid:
            invalid_content_ids.append(content_id)
            continue

        all_int_users |= int_users
        all_ext_users |= ext_users

    dump_invalid_data(invalid_content_ids, RESULTS_DIR, filename='invalid_contents')

    dump_distinct_users(all_int_users, RESULTS_DIR, is_internal_view=True)
    dump_distinct_users(all_ext_users, RESULTS_DIR, is_internal_view=False)


if __name__ == '__main__':
    main()
