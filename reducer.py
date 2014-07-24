####################################
#   Filename: reducer.py           #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import ANALYSIS_RESULTS_DIR, SEPARATOR_1, SEPARATOR_2

from trace_record import Trace_rec
from output import dump_data
from clustering import process_trace_recs_for_content

from sys import stdin



def process_and_log_content(content, trace_recs):
    invalid_contents_filename = '%s/invalid_contents.log' % ANALYSIS_RESULTS_DIR
    processed_contents_filename = '%s/processed_contents.log' % ANALYSIS_RESULTS_DIR

    is_valid, res = process_content(content, trace_recs)

    if not is_valid:
        message = 'Invalid content (%s): %s' % (content, res['message'])
        dump_data([message], invalid_contents_filename)

    dump_data([content], processed_contents_filename)


def process_content(content, trace_recs):
    content = tuple(content.split('_'))

    for trace_rec in trace_recs:
        trace_rec.fill_out()

    trace_recs = sorted(trace_recs, key=lambda trace_rec: trace_rec.secs)
    res = process_trace_recs_for_content(content, trace_recs)

    return res



def main():

    last_content = None
    trace_recs = list()

    for input_line in stdin:
        content, trace_rec_str = input_line.strip().split(SEPARATOR_1, 1)
        trace_rec_str = trace_rec_str.replace(SEPARATOR_2, SEPARATOR_1)

        trace_rec = Trace_rec(trace_rec_str)

        if last_content is None:
            last_content = content

        if last_content == content:
            trace_recs.append(trace_rec)
            continue

        process_and_log_content(last_content, trace_recs)
        last_content = content
        trace_recs = [trace_rec]

    if trace_recs:
        process_and_log_content(last_content, trace_recs)



if __name__ == '__main__':
    main()
