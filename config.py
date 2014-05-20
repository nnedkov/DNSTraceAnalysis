####################################
#   Filename: config.py            #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

VERBOSITY_IS_ON = True

TRACE_FILES_DIR = '/home/nedko/Inria/DNS_traces/'
TRACE_FILES_NAME_PREFIX = 'dns2-sop-00'
TRACE_FILES_NUMBER = 9

RUNNING_ON_HADOOP = False
DEBUG_HADOOP = False
SEPARATOR = '#'

RESULTS_DIR = './new_results'
TEST_CONTENTS = [ ('N214', '0x0001', '0x0001'), \
                  ('N2062', '0x0001', '0x0001'), \
                  ('N838185', '0x0001', '0x0001'), \
                  ('N122505', '0x0001', '0x000c'), \
                  ('N508060', '0x0001', '0x0001'), \
                  ('N411581', '0x0001', '0x001c'), \
                  ('N801955', '0x0001', '0x001c'), \
                  ('N605093', '0x0001', '0x001c'), \
                  ('N519857', '0x0001', '0x001c') ]
TEST_CONTENTS_STRINGS = ['_'.join(i) for i in TEST_CONTENTS]
