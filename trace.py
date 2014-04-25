####################################
#   Filename: trace.py             #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from time_converter import convert_datetime_to_secs



class Trace:

    def __init__(self, trace_str, separator='\t'):
        self.args = trace_str.strip().split(separator)
        self.domain_name = self.args[7]
        self.class_type = self.args[8]
        self.ip_version = self.args[9]
        self.content_id = (self.domain_name, self.class_type, self.ip_version)


    def get_secs_value(self):
        if hasattr(self, 'secs'):
            return self.secs

        self.datetime = self.args[1]
        self.secs = convert_datetime_to_secs(self.datetime)

        return float(self.secs)


    def fill_out(self):
        if not hasattr(self, 'datetime'):
            self.datetime = self.args[1]
            self.secs = convert_datetime_to_secs(self.datetime)
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

        self.nameserver_is_dst = self.dst == 'dns2-sop'
        if self.nameserver_is_dst:
            self.node = self.src
        else:
            self.node = self.dst

        if self.is_request and self.nameserver_is_dst:   # (1) req_arr
            self.type = 1
            assert self.ttl is None
        elif self.is_request and not self.nameserver_is_dst:   # (2) req_miss
            self.type = 2
            assert self.ttl is None
        elif not self.is_request and self.nameserver_is_dst:   # (3) res_miss
            self.type = 3
            assert self.ttl is not None
        elif not self.is_request and not self.nameserver_is_dst:   # (4) res_arr
            self.type = 4
            assert self.ttl is not None
