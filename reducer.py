#######################################
#   Filename: reducer.py              #
#   Nedko Stefanov Nedkov             #
#   nedko.stefanov.nedkov@gmail.com   #
#   April 2014                        #
#######################################

from config import ANALYSIS_RESULTS_DIR, SEPARATOR_1, SEPARATOR_2

from trace_record import Trace_rec
from output import dump_data
from clustering import process_trace_recs_for_content

import traceback



def process_and_log_content(content_name, trace_recs):
    content_id = tuple(content_name.split('_'))
    invalid_cids_filename = '%s/invalid_content_ids.log' % ANALYSIS_RESULTS_DIR
    processed_cids_filename = '%s/processed_content_ids.log' % ANALYSIS_RESULTS_DIR

    try:
        is_valid, res = process_content(content_id, trace_recs)
    except Exception:
        is_valid = False
        res = {'outcome_message': traceback.format_exc()}

    if not is_valid:
        message = 'Invalid content id (%s): %s' % (str(content_id),
                                                   res['outcome_message'])
        dump_data([message], invalid_cids_filename)

    dump_data([str(content_id)], processed_cids_filename)


def process_content(content_id, trace_recs):
    for trace_rec in trace_recs:
        trace_rec.fill_out()

    trace_recs = sorted(trace_recs, key=lambda trace_rec: trace_rec.secs)

    res = process_trace_recs_for_content(content_id, trace_recs)

    return res



def main():

    last_content_name = None
    trace_recs = list()

    with open('./reducer_input.txt', 'r') as fp:

        for input_line in fp:

            content_name, trace_rec_str = input_line.strip().split(SEPARATOR_1, 1)
            trace_rec_str = trace_rec_str.replace(SEPARATOR_2, SEPARATOR_1)

            trace_rec = Trace_rec(trace_rec_str)

            if last_content_name is None:
                last_content_name = content_name

            if last_content_name == content_name:
                trace_recs.append(trace_rec)
                continue

            process_and_log_content(last_content_name, trace_recs)
            last_content_name = content_name
            trace_recs = [trace_rec]

    if trace_recs:
        process_and_log_content(last_content_name, trace_recs)



if __name__ == '__main__':
    main()
