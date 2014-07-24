#####################################
#   Filename: config.py             #
#   Nedko Stefanov Nedkov           #
#   nedko.nedkov@inria.fr           #
#   April 2014                      #
#####################################

TRACE_FILES_DIR = '/home/nedko/Inria/DNS_trace/'
TRACE_FILES_NAME_PREFIX = 'dns2-sop-00'
TRACE_FILES_NUMBER = 9
TRACE_DURATION = 861598.520376000

SEPARATOR_1 = '\t'
SEPARATOR_2 = '#'

CLUSTERING_RESULTS_DIR = './clustering_results'
ANALYSIS_RESULTS_DIR = './analysis_results'

FEW_CONTENT_IDS = [('N214', '0x0001', '0x0001'),
                   ('N49', '0x0001', '0x0001')]
OUTPUT_USERS = False
USER_CLUSTERS_NUMBER = 4
REQ_ARR_FILE_TO_SPLIT = '%s/%s/%s/%s/%s' % (CLUSTERING_RESULTS_DIR,
                                            '214',
                                            'content_214_v4_0x0001',
                                            'internal_view',
                                            'req_arr_214_v4_0x0001.txt')

VERBOSITY_IS_ON = True
NAMESERVER_NAME = 'dns2-sop'
RUNNING_ON_HADOOP = False
