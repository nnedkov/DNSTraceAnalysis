####################################
#   Filename: reducer.py           #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import ANALYSIS_RESULTS_DIR, SEPARATOR

from trace import Trace
from data_clustering import process_traces_for_content
from data_dumping import dump_data

from sys import stdin



def main(separator='\t'):

    def process_content(content, traces):
        content_id = tuple(content.split('_'))
        traces = sorted(traces, key=lambda trace: trace.get_secs_value())
        res = process_traces_for_content(content_id, traces)

        return res


    last_content = None
    traces = list()
    filename = '%s/invalid_contents.log' % ANALYSIS_RESULTS_DIR
    processed_contents_filename = '%s/processed_contents.log' % ANALYSIS_RESULTS_DIR
    i = 1
#    start = False

    for input_line in stdin:
        content, trace_str = input_line.strip().split(separator, 1)
        trace_str = separator.join(trace_str.split(SEPARATOR))

#        if not start:
#            if content == 'N965487_0x0001_0x0001':
#                start = True
#            else:
#                continue

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
                    message = '%s: %s' % (last_content, res['message'])
                else:
                    message = str(last_content)
        except Exception:
            is_valid = False
            message = str(last_content)

        if not is_valid:
            dump_data([message], filename)

        print i
        i += 1
        dump_data([str(last_content)], processed_contents_filename)
        last_content = content
        traces = [trace]

    if traces:
        try:
            is_valid, res = process_content(last_content, traces)
            if not is_valid:
                if res['message'] == "It's empty!":
                    message = '%s: %s' % (last_content, res['message'])
                else:
                    message = str(last_content)
        except Exception:
            is_valid = False
            message = str(last_content)

        if not is_valid:
            dump_data([message], filename)

        print i
        dump_data([str(last_content)], processed_contents_filename)


if __name__ == '__main__':
    main()
