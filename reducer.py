####################################
#   Filename: reducer.py           #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import ANALYSIS_RESULTS_DIR, SEPARATOR_1, SEPARATOR_2

from trace import Trace
from clustering import process_traces_for_content
from output import dump_data

from sys import stdin



def process_and_log_content(content, traces):
    invalid_contents_filename = '%s/invalid_contents.log' % ANALYSIS_RESULTS_DIR
    processed_contents_filename = '%s/processed_contents.log' % ANALYSIS_RESULTS_DIR

    is_valid, res = process_content(content, traces)

    if not is_valid:
        message = 'Invalid content (%s): %s' % (content, res['message'])
        dump_data([message], invalid_contents_filename)

    dump_data([content], processed_contents_filename)


def process_content(content, traces):
    content = tuple(content.split('_'))

    for trace in traces:
        trace.fill_out()

    traces = sorted(traces, key=lambda trace: trace.secs)
    res = process_traces_for_content(content, traces)

    return res



def main():

    last_content = None
    traces = list()

    for input_line in stdin:
        content, trace_str = input_line.strip().split(SEPARATOR_1, 1)
        trace_str = trace_str.replace(SEPARATOR_2, SEPARATOR_1)

        trace = Trace(trace_str)

        if last_content is None:
            last_content = content

        if last_content == content:
            traces.append(trace)
            continue

        process_and_log_content(last_content, traces)
        last_content = content
        traces = [trace]

    if traces:
        process_and_log_content(last_content, traces)



if __name__ == '__main__':
    main()
