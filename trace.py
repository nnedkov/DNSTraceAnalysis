####################################
#   Filename: trace.py             #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

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
        self.dst = self.args[3]
        self.transaction_id = self.args[4]
        self.is_request = not int(self.args[5])
        self.answers_num = int(self.args[6])
        self.src_is_internal = bool(int(self.args[11]))
        self.dst_is_internal = bool(int(self.args[12]))
        try:
            self.ttl = self.args[13]
        except IndexError:
            self.ttl = None

        self.secs_since_epoch = convert_datetime_to_secs(self.datetime)
        self.nameserver_is_dst = self.dst == 'dns2-sop'

        if self.nameserver_is_dst:
            self.node = self.src
        else:
            self.node = self.dst

        # (1) req_arr
        if self.is_request and self.nameserver_is_dst:
            self.type = 1
            assert self.ttl is None
        # (2) req_miss
        elif self.is_request and not self.nameserver_is_dst:
            self.type = 2
            assert self.ttl is None
        # (3) res_miss
        elif not self.is_request and self.nameserver_is_dst:
            self.type = 3
            assert self.ttl is not None
        # (4) res_arr
        elif not self.is_request and not self.nameserver_is_dst:
            self.type = 4
            assert self.ttl is not None
