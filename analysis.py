####################################
#   Filename: analysis.py          #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import ANALYSIS_RESULTS_DIR, CLUST_RESULTS_DIR, TRACE_DURATION

from total_users import get_users

from os import walk, path
from operator import itemgetter



def get_filename_with_infix(filenames, infix):
    try:
        filenames_with_infix = [f for f in filenames if infix in f]
        assert len(filenames_with_infix) <= 1
        filename_with_infix = filenames_with_infix[0]
    except IndexError:
        filename_with_infix = None

    return filename_with_infix


def get_number_of_lines(filepath):
    fp = open(filepath)
    num_of_lines = sum(1 for line in fp)
    fp.close()

    return num_of_lines


def get_ttl_values(filepath):
    ttl_values = set()

    with open(filepath) as fp:
        ttl_values |= set([int(line.strip().split('\t')[1]) for line in fp])

    return list(ttl_values)


def get_content_name(dirpath, filename):
    content_name = 'N' + filename.split('_', 2)[2].split('.')[0] + '_'
    view = dirpath.split('/')[4]
    assert view in ['internal_view', 'external_view']
    content_name += 'int' if view == 'internal_view' else 'ext'

    return content_name



def main():

    total_users = set()
    analysis_filepath_prefix = '%s/analysis_results' % ANALYSIS_RESULTS_DIR

    for root, _, filenames in walk(CLUST_RESULTS_DIR):
        if not filenames or 'for_tests' in root:
            continue

        req_arr_filename = get_filename_with_infix(filenames, 'req_arr')
        req_miss_filename = get_filename_with_infix(filenames, 'req_miss')
        res_arr_filename = get_filename_with_infix(filenames, 'res_arr')
        res_miss_filename = get_filename_with_infix(filenames, 'res_miss')
        users_filename = get_filename_with_infix(filenames, 'users')

        if req_arr_filename is None and users_filename is None:
            continue

        assert req_arr_filename is not None and users_filename is not None

        req_arr_num = get_number_of_lines(path.join(root, req_arr_filename))
        assert req_arr_num != 0

        req_miss_num = 0
        if req_miss_filename:
            req_miss_num = get_number_of_lines(path.join(root, req_miss_filename))
            assert req_miss_num != 0

        ttl_values = list()
        if res_miss_filename:
            ttl_values = get_ttl_values(path.join(root, res_miss_filename))
            assert ttl_values
        elif res_arr_filename:
            max_ttl_value = max(get_ttl_values(path.join(root, res_arr_filename)))
            ttl_values.append('>= %s' % max_ttl_value)
            assert ttl_values

        users = get_users(path.join(root, users_filename))
        assert users is not None and len(users) != 0
        total_users |= users

        content_name = get_content_name(root, req_arr_filename)

        with open(analysis_filepath_prefix + '_temp.txt', 'a') as fp:

            s = '\t'.join(['%s']*4) + '\n'
            fp.write(s % (content_name,
                          req_arr_num,
                          req_miss_num,
                          str(ttl_values)))

    total_req_arr_num = 0
    total_req_miss_num = 0
    recs = list()

    with open(analysis_filepath_prefix + '_temp.txt') as fp:

        for line in fp:
            args = line.strip().split('\t')

            content_name = args[0]
            req_arr_num = int(args[1])
            req_miss_num = int(args[2])
            ttl_values = args[3]

            total_req_arr_num += req_arr_num
            total_req_miss_num += req_miss_num

            rec = (content_name,
                   req_arr_num,
                   req_miss_num,
                   ttl_values)

            recs.append(rec)

    print 'Total number of req_arr: %s' % total_req_arr_num
    print 'Total number of req_miss: %s' % total_req_miss_num
    print 'Total number of users: %s' % len(total_users)

    final_recs = list()

    for rec in recs:
        content_name, req_arr_num, req_miss_num, ttl_values = rec

        req_arr_ratio = float(req_arr_num)/float(total_req_arr_num)
        req_miss_ratio = float(req_miss_num)/float(total_req_miss_num)
        arrival_rate = float(req_arr_num)/TRACE_DURATION

        final_rec = (content_name,
                     req_arr_num,
                     req_miss_num,
                     req_arr_ratio,
                     req_miss_ratio,
                     arrival_rate,
                     ttl_values)

        final_recs.append(final_rec)

    final_recs = sorted(final_recs, key=itemgetter(1), reverse=True)

    with open(analysis_filepath_prefix + '.txt', 'a') as fp:

        columns = ['content',
                   '#req_arr',
                   '#req_miss',
                   '#req_arr ratio',
                   '#req_miss ratio',
                   '#users',
                   'arrival rate',
                   'ttl(s)']
        fp.write('#%s\n' % '\t'.join(columns))

        for final_rec in final_recs:
            fp.write('%s\t%s\t%s\t%.10f\t%.10f\t%.5f\t%s\n' % final_rec)



if __name__ == '__main__':
    main()
