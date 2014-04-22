####################################
#   Filename: mapper.py            #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from trace import Trace

from sys import stdin



def main(separator='\t'):
    for trace_str in stdin:
        trace = Trace(trace_str)
        print '%s%s%s' % (trace.content.id, separator, trace_str.strip())


if __name__ == '__main__':
    main()
