####################################
#   Filename: reducer.py           #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from trace import Trace
from data_clustering import process_traces_for_content

from sys import stdin



def main(separator='\t'):

    def process_content(content, traces):
        content_id = tuple(content.split('_'))
        traces = sorted(traces, key=lambda trace: trace.get_secs())
        process_traces_for_content(content_id, traces)

    last_content = None
    traces = list()

    for input_line in stdin:
        input_line = input_line.strip()
        content, trace_str = input_line.split(separator, 1)
        trace = Trace(trace_str)

        if last_content == content:
            traces.append(trace)
            continue

        if last_content is not None:
            process_content(last_content, traces)

        last_content = content
        traces = list()

    if last_content is not None:
        process_content(last_content, traces)


if __name__ == '__main__':
    main()
