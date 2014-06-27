################################
#   Filename: mapper.py        #
#   Nedko Stefanov Nedkov      #
#   nedko.nedkov@inria.fr      #
#   April 2014                 #
################################

from config import SEPARATOR_1, SEPARATOR_2

from trace import Trace

from sys import stdin, stdout



def main():

    for trace_str in stdin:
        trace = Trace(trace_str)

        output_trace_str = trace_str.strip().replace(SEPARATOR_1, SEPARATOR_2)
        output_line = '%s_%s_%s%s%s\n' % (trace.domain_name, \
                                          trace.class_type, \
                                          trace.ip_version, \
                                          SEPARATOR_1, \
                                          output_trace_str)
        stdout.write(output_line)



if __name__ == '__main__':
    main()
