####################################
#   Filename: time_converter.py    #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from time import strptime
from calendar import timegm



def convert_datetime_to_secs(datetime):
    millisecs = datetime.split('.')[1]
    # the millisecs accuracy is lost in the below object
    time_obj = strptime(datetime, "%b %d, %Y %H:%M:%S.%f000")
    secs_since_epoch = timegm(time_obj)
    secs_since_epoch = '%s.%s' % (secs_since_epoch, millisecs)

    return secs_since_epoch
