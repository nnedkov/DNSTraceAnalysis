#####################################
#   Filename: clustering.py         #
#   Nedko Stefanov Nedkov           #
#   nedko.nedkov@inria.fr           #
#   April 2014                      #
#####################################

from config import VERBOSITY_IS_ON, TRACE_FILES_NUMBER, TRACE_FILES_DIR, \
                   TRACE_FILES_NAME_PREFIX, OUTPUT_USERS, \
                   ANALYSIS_RESULTS_DIR, FEW_CONTENT_IDS


from content_clusters import Content_clusters
from trace_record import Trace_rec
from output import dump_data



def process_trace_recs_for_content(content_id, trace_recs, output_users=False):
    content_clusters = Content_clusters(content_id)

    for trace_rec in trace_recs:
        process_next_rec = content_clusters.process_trace_rec(trace_rec)

        if not process_next_rec:
            return content_clusters.get_results()

    content_clusters.assert_and_dump_clusters(output_users)

    return content_clusters.get_results()


def process_content(content_id):
    if VERBOSITY_IS_ON:
        print 'Processing content with id: %s' % str(content_id)

    trace_recs = list()

    for tfn in range(TRACE_FILES_NUMBER):
        trace_filepath = '%s%s%s' % (TRACE_FILES_DIR,
                                     TRACE_FILES_NAME_PREFIX,
                                     tfn)
        if VERBOSITY_IS_ON:
            print '\tFiltering trace records from file %s' % trace_filepath

        with open(trace_filepath) as fp:

            for trace_rec_str in fp:
                trace_rec = Trace_rec(trace_rec_str)

                if trace_rec.is_referring_to_content(content_id):
                    trace_recs.append(trace_rec)

    res = process_trace_recs_for_content(content_id, trace_recs, output_users=OUTPUT_USERS)

    return res



def main():

    invalid_cids_filename = '%s/invalid_content_ids.log' % ANALYSIS_RESULTS_DIR

    for content_id in FEW_CONTENT_IDS:
        is_valid, res = process_content(content_id)

        if not is_valid:
            message = 'Invalid content id (%s):\n%s\n' % (str(content_id),
                                                          res['message'])
            dump_data([message], invalid_cids_filename)



if __name__ == '__main__':
    main()
