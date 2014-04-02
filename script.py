#############################
#   Filename: script.py     #
#   Nedko Stefanov Nedkov   #
#   nedko.nedkov@inria.fr   #
#   April 2014              #
#############################

''' Parser '''

import time, calendar



def convert_datetime_to_secs(datetime):
    millisecs = datetime.split('.')[1]
    time_obj = time.strptime(datetime, "%b %d, %Y %H:%M:%S.%f000")   # it loses the millisecs
    secs_since_epoch = calendar.timegm(time_obj)
    
    accurate_secs = '%s.%s' % (secs_since_epoch, millisecs)
    
    return float(accurate_secs)
    
    
    
if __name__ == '__main__':

    trace_files_prefix = '/home/nedko/Inria/DNStraces/dns2-sop-00'
    
    for i in range(2, 9):
        trace_file = '%s%s' % (trace_files_prefix, str(i))
        with open(trace_file) as fp:
            transactions = dict()
            weird_trans = list()
            
            for j, line in enumerate(fp):
                trace_args = line.rstrip().split('\t')
                
                source = trace_args[2]
                dest = trace_args[3]
                transaction = trace_args[4]
                domain = trace_args[7]
                
                try:
                    transactions[transaction].append((source, dest, domain, j))
                except KeyError:
                    if source == 'dns2-sop':
                        continue
                    transactions[transaction] = [(source, dest, domain, j)]
                    
                if len(transactions[transaction]) >= 2:
                    src1, dest1, d1, l1 = transactions[transaction][-2]
                    src2, dest2, d2, l2 = transactions[transaction][-1]
                    if len(set([src1, dest1, src2, dest2])) == 3 and l2-l1 < 100 and dest1 == src2 and d1 == d2:
                        weird_trans.append(transaction)


                #assert len(transactions[transaction]) < 3, transactions[transaction]


                #datetime = trace_args[1]
                #acc_secs_since_epoch = convert_datetime_to_secs(datetime)

                if j == 200000:
                    break

            for i in weird_trans:
                print '*** %s' % transactions[i]
             
            #for key, value in transactions.iteritems():
            #    print '%s: %s' % (key, value)
            
            
