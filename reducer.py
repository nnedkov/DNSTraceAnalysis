####################################
#   Filename: reducer.py           #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import DEBUG_HADOOP

from trace import Trace
from data_clustering import process_traces_for_content

from sys import stdin



def main(separator='\t'):

    def process_content(content, traces):
        content_id = tuple(content.split('_'))
        traces = sorted(traces, key=lambda trace: trace.get_secs_value())
        process_traces_for_content(content_id, traces)


    last_content = None
    traces = list()

    for input_line in stdin:
        content, trace_str = input_line.strip().split(separator, 1)

        if DEBUG_HADOOP and content != 'N214_0x0001_0x0001':
            continue

        trace = Trace(trace_str)

        if last_content is None:
            last_content = content

        if last_content == content:
            traces.append(trace)
            continue

        process_content(last_content, traces)
        last_content = content
        traces = [trace]

    if traces:
        process_content(last_content, traces)


if __name__ == '__main__':
    main()
