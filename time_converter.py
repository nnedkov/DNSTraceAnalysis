####################################
#   Filename: time_converter.py    #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

''' docstring to be filled '''

from time import strptime
from calendar import timegm



def convert_datetime_to_secs(datetime):
    millisecs = datetime.split('.')[1]
    time_obj = strptime(datetime, "%b %d, %Y %H:%M:%S.%f000")   # the object loses the millisecs
    secs_since_epoch = timegm(time_obj)
    acc_secs_since_epoch = '%s.%s' % (secs_since_epoch, millisecs)

    return acc_secs_since_epoch
    