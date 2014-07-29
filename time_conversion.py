#######################################
#   Filename: time_conversion.py      #
#   Nedko Stefanov Nedkov             #
#   nedko.stefanov.nedkov@gmail.com   #
#   April 2014                        #
#######################################

from time import strptime
from calendar import timegm



def convert_datetime_to_secs(datetime_str, ms_separator='.'):
    # millisecs are stored because function strptime discards them
    millisecs = datetime_str.split(ms_separator)[1]
    datetime = strptime(datetime_str, "%b %d, %Y %H:%M:%S.%f000")
    secs_since_epoch = timegm(datetime)
    secs_since_epoch = '%s.%s' % (secs_since_epoch, millisecs)

    return secs_since_epoch
