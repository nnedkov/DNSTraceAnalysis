####################################
#   Filename: tester.py            #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

''' docstring to be filled '''

from time_converter import convert_datetime_to_secs



def res_arr_are_valid(res_arr_traces):
    previous_secs = None
    previous_ttl = None

    for datetime, ttl in res_arr_traces:
        acc_secs_since_epoch = float(convert_datetime_to_secs(datetime))
        ttl = float(ttl)
        if previous_secs is not None:
            secs_diff = acc_secs_since_epoch - previous_secs
            ttl_diff = ttl - previous_ttl
            if secs_diff > ttl_diff:
                return False

        previous_secs = acc_secs_since_epoch
        previous_ttl = ttl

    return True


def res_miss_are_valid(res_miss_traces):
    previous_secs = None

    for datetime, ttl in res_miss_traces:
        acc_secs_since_epoch = float(convert_datetime_to_secs(datetime))
        if previous_secs is not None and previous_secs > acc_secs_since_epoch:
            return False

        previous_secs = acc_secs_since_epoch + float(ttl)

    return True
    