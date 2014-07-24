#####################################
#   Filename: mapper.py             #
#   Nedko Stefanov Nedkov           #
#   nedko.nedkov@inria.fr           #
#   April 2014                      #
#####################################

from config import SEPARATOR_1, SEPARATOR_2

from trace_record import Trace_rec

from sys import stdin, stdout



def main():

    for trace_rec_str in stdin:
        trace_rec = Trace_rec(trace_rec_str)

        output_trace_rec_str = trace_rec_str.strip().replace(SEPARATOR_1, SEPARATOR_2)
        output_line = '%s_%s_%s%s%s\n' % (trace_rec.hostname, \
                                          trace_rec.class_type, \
                                          trace_rec.ip_version, \
                                          SEPARATOR_1, \
                                          output_trace_rec_str)
        stdout.write(output_line)



if __name__ == '__main__':
    main()
