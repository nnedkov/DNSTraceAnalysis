####################################
#   Filename: popularity.py        #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import RESULTS_DIR

from operator import itemgetter

from os import walk, remove

popularity_filename = '%s/popularity' % RESULTS_DIR



def get_number_of_lines(filepath):
    fp = open(filepath)
    num_of_lines = sum(1 for line in fp)
    fp.close()

    return num_of_lines


def get_content_name(dirpath, filename):
    content = 'N' + filename.split('_', 2)[2].split('.')[0] + '_'
    content += 'int' if dirpath.split('/')[4] == 'internal_view' else 'ext'

    return content


def main(separator='\t'):
#
#    total_req_arr_num = 0
#    total_req_miss_num = 0
#
#    for dirpath, _, filenames in walk(RESULTS_DIR):
#        if not filenames:
#            continue
#
#        try:
#            req_arr_filenames = [filename for filename in filenames if 'req_arr' in filename]
#            assert len(req_arr_filenames) <= 1
#            req_arr_filename = req_arr_filenames[0]
#        except IndexError:
#            req_arr_filename = None
#
#        req_arr_num = 0
#        if req_arr_filename:
#            req_arr_num = get_number_of_lines(dirpath + '/' + req_arr_filename)
#            total_req_arr_num += req_arr_num
#
#        try:
#            req_miss_filenames = [filename for filename in filenames if 'req_miss' in filename]
#            assert len(req_miss_filenames) <= 1
#            req_miss_filename = req_miss_filenames[0]
#        except IndexError:
#            req_miss_filename = None
#
#        req_miss_num = 0
#        if req_miss_filename:
#            req_miss_num = get_number_of_lines(dirpath + '/' + req_miss_filename)
#            total_req_miss_num += req_miss_num
#
#        if req_arr_filename is None and req_miss_filename is None:
#            continue
#
#        if req_miss_num != 0:
#            assert req_arr_num != 0
#
#        try:
#            content = get_content_name(dirpath, req_arr_filename if req_arr_filename is not None else req_miss_filename)
#        except:
#            print dirpath, req_arr_filename
#
#        with open(popularity_filename + '_temp.txt', 'a') as fp:
#            fp.write('%s\t%s\t%s\n' % (content, req_arr_num, req_miss_num))

    total_req_arr_num = 0
    total_req_miss_num = 0
    recs = list()
    print 'opening file!'
    with open(popularity_filename + '_temp.txt') as fp:

        for line in fp:
            line = line.strip()
            args = line.split('\t')
            content = args[0]
            req_arr_num = int(args[1])
            req_miss_num = int(args[2])

            total_req_arr_num += req_arr_num
            total_req_miss_num += req_miss_num

            recs.append((content, req_arr_num, req_miss_num))

    print 'total_req_arr_num: %s' % total_req_arr_num
    print 'total_req_miss_num: %s' % total_req_miss_num
    print 'computing ratios!'
    final_recs = list()
    for rec in recs:
        content, req_arr_num, req_miss_num = rec

        req_arr_ratio = float(req_arr_num)/float(total_req_arr_num) if req_arr_num else 0
        req_miss_ratio = float(req_miss_num)/float(total_req_miss_num) if req_miss_num else 0

        final_recs.append((content, req_arr_num, req_miss_num, req_arr_ratio, req_miss_ratio))

    final_recs = sorted(final_recs, key=itemgetter(1), reverse=True)

    #remove(popularity_filename + '_temp.txt')
    print 'writing results!'
    with open(popularity_filename + '.txt', 'a') as fp:
        for rec in final_recs:
            fp.write('%s\t%s\t%s\t%f\t%f\n' % (rec[0], rec[1], rec[2], rec[3], rec[4]))



if __name__ == '__main__':
    main()
