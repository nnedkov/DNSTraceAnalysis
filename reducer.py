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
    last_content_id = None
    traces = None

    for input_line in stdin:
        input_line = input_line.strip()
        content_id, trace_str = input_line.split(separator, 1)
        trace = Trace(trace_str)

        if last_content_id == content_id:
            traces.append(trace)
            continue

        if last_content_id is not None:
            process_traces_for_content(last_content_id, traces)

        last_content_id = content_id
        traces = list()

    if last_content_id is not None:
        process_traces_for_content(last_content_id, traces)


if __name__ == '__main__':
    main()
