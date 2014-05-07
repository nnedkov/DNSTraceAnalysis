####################################
#   Filename: reducer.py           #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import RESULTS_DIR, SEPARATOR, DEBUG_HADOOP, TEST_CONTENTS_STRINGS

from trace import Trace
from data_clustering import process_traces_for_content
from data_dumping import dump_data, dump_users

from sys import stdin



def main(separator='\t'):

    def process_content(content, traces):
        content_id = tuple(content.split('_'))
        traces = sorted(traces, key=lambda trace: trace.get_secs_value())
        res = process_traces_for_content(content_id, traces)

        return res


    last_content = None
    traces = list()
    filename = '%s/invalid_contents.log' % RESULTS_DIR
    invalid_contents = list()
    all_int_users = set()
    all_ext_users = set()

    for input_line in stdin:
        content, trace_str = input_line.strip().split(separator, 1)
        trace_str = separator.join(trace_str.split(SEPARATOR))

        if DEBUG_HADOOP and content not in TEST_CONTENTS_STRINGS:
            continue

        trace = Trace(trace_str)

        if last_content is None:
            last_content = content

        if last_content == content:
            traces.append(trace)
            continue

        try:
            is_valid, res = process_content(last_content, traces)
            if not is_valid:
                if res['message'] == "It's empty!":
                    invalid_contents.append('%s: %s' % (last_content, res['message']))
                else:
                    invalid_contents.append(last_content)
        except Exception:
            is_valid = False
            invalid_contents.append(last_content)

        if is_valid:
            all_int_users |= res['internal_users']
            all_ext_users |= res['external_users']

        last_content = content
        traces = [trace]

    if traces:
        try:
            is_valid, res = process_content(last_content, traces)
            if not is_valid:
                if res['message'] == "It's empty!":
                    invalid_contents.append('%s: %s' % (last_content, res['message']))
                else:
                    invalid_contents.append(last_content)
        except Exception:
            is_valid = False
            invalid_contents.append(last_content)

        if is_valid:
            all_int_users |= res['internal_users']
            all_ext_users |= res['external_users']

    if invalid_contents:
        dump_data(invalid_contents, filename)
    if all_int_users:
        dump_users(all_int_users, RESULTS_DIR, is_internal_view=True)
    if all_ext_users:
        dump_users(all_ext_users, RESULTS_DIR, is_internal_view=False)


if __name__ == '__main__':
    main()
