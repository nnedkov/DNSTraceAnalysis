####################################
#   Filename: mapper.py            #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from trace import Trace

from sys import stdin, stdout



def main(separator='\t'):
    for trace_str in stdin:
        trace = Trace(trace_str)
        
        if trace.content_id != ('N214', '0x0001', '0x0001'):
            continue
        
        output_line = '%s_%s_%s%s%s\n' % (trace.domain_name, \
                                          trace.class_type, \
                                          trace.ip_version, \
                                          separator, \
                                          trace_str.strip())
        stdout.write(output_line)


if __name__ == '__main__':
    main()
