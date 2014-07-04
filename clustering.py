####################################
#   Filename: clustering.py        #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import VERBOSITY_IS_ON, TRACE_FILES_NUMBER, TRACE_FILES_DIR, \
                   TRACE_FILES_NAME_PREFIX, OUTPUT_USERS, \
                   ANALYSIS_RESULTS_DIR, FEW_CONTENTS


from content import Content
from trace import Trace
from output import dump_data



def process_traces_for_content(content, traces, output_users=False):
    content = Content(content)

    for trace in traces:
        process_next = content.process_trace(trace)

        if not process_next:
            return content.get_results()

    content.assert_and_dump_clusters(output_users)

    return content.get_results()


def process_content(content):
    if VERBOSITY_IS_ON:
        print 'Processing content %s:' % str(content)

    content_inst = Content(content)
    traces = list()

    for tfn in range(TRACE_FILES_NUMBER):
        trace_filepath = '%s%s%s' % (TRACE_FILES_DIR,
                                     TRACE_FILES_NAME_PREFIX,
                                     tfn)
        if VERBOSITY_IS_ON:
            print '\tFiltering traces from file %s' % trace_filepath

        with open(trace_filepath) as fp:

            for trace_str in fp:
                trace = Trace(trace_str)

                if content_inst.is_reffered_in_trace(trace):
                    traces.append(trace)

    res = process_traces_for_content(content, traces, output_users=OUTPUT_USERS)

    return res



def main():

    invalid_contents_filename = '%s/invalid_contents.log' % ANALYSIS_RESULTS_DIR

    for content in FEW_CONTENTS:
        is_valid, res = process_content(content)

        if not is_valid:
            message = 'Invalid content: %s\n%s\n' % (str(content), res['message'])
            dump_data([message], invalid_contents_filename)



if __name__ == '__main__':
    main()
