####################################
#   Filename: trace.py             #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

''' docstring to be filled '''

from time_converter import convert_datetime_to_secs



class Trace:
    def __init__(self, trace_str):
        self.args = trace_str.rstrip().split('\t')
        self.domain_name = self.args[7]
        self.class_type = self.args[8]
        self.ip_version = self.args[9]
        self.content_id = (self.domain_name, self.class_type, self.ip_version)
    
    def fill_out(self):
        self.datetime = self.args[1]
        self.src = self.args[2]
        self.dest = self.args[3]
        self.transaction_id = self.args[4]
        self.is_request = not int(self.args[5])
        self.answers_count = int(self.args[6])
        try:
            self.ttl = self.args[13]
        except IndexError:
            self.ttl = None
        self.acc_secs_since_epoch = convert_datetime_to_secs(self.datetime)
        self.dns_is_dest = self.dest == 'dns2-sop'
    
        if self.dns_is_dest:
            self.node = self.src
        else:
            self.node = self.dest
    
        # (1) req_arr
        if self.is_request and self.dns_is_dest:
            self.type = 1
            assert self.ttl is None
        # (2) req_miss
        elif self.is_request and not self.dns_is_dest:
            self.type = 2
            assert self.ttl is None
        # (3) res_miss
        elif not self.is_request and self.dns_is_dest:
            self.type = 3
            assert self.ttl is not None
        # (4) res_arr
        elif not self.is_request and not self.dns_is_dest:
            self.type = 4
            assert self.ttl is not None
            
        self.rec = (self.type, [self.datetime])
        if self.ttl is not None:
            self.rec[1].append(self.ttl)
