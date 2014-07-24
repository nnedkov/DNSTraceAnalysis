####################################
#   Filename: trace_record.py      #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import SEPARATOR_1, NAMESERVER_NAME

from time_conversion import convert_datetime_to_secs



class Trace_rec:

    def __init__(self, trace_rec_str):
        self.attributes = trace_rec_str.strip().split(SEPARATOR_1)
        self.hostname = self.attributes[7]
        self.class_type = self.attributes[8]
        self.ip_version = self.attributes[9]
        self.content = (self.hostname,
                        self.class_type,
                        self.ip_version)
        self.is_filled_out = False


    def fill_out(self):
        if self.is_filled_out:
            return

        self.datetime = self.attributes[1]
        self.secs = convert_datetime_to_secs(self.datetime)
        self.src = self.attributes[2]
        self.dst = self.attributes[3]
        self.transaction_id = self.attributes[4]
        self.is_request = not bool(int(self.attributes[5]))
        self.answers_num = int(self.attributes[6])
        self.src_is_internal = bool(int(self.attributes[11]))
        self.dst_is_internal = bool(int(self.attributes[12]))
        try:
            self.ttl = self.attributes[13]
        except IndexError:
            self.ttl = None

        self.nameserver_is_dst = (self.dst == NAMESERVER_NAME)
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
            if self.answers_num == 0:
                self.ttl = 0
            assert self.ttl is not None
        elif not self.is_request and not self.nameserver_is_dst:   # (4) res_arr
            self.type = 4
            if self.answers_num == 0:
                self.ttl = 0
            assert self.ttl is not None

        self.is_filled_out = True
