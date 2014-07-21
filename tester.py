####################################
#   Filename: tester.py            #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import SEPARATOR_1

from os import sys, walk, path



def load_res_cluster(filepath):
    cluster = list()

    with open(filepath) as fp:

        for line in fp:
            args = line.strip().split(SEPARATOR_1)
            secs = args[0]
            ttl = args[1]
            cluster.append((secs, ttl))

    return cluster


def res_arr_cluster_is_valid(res_arr_cluster):
    valid_until = None
    prev_time = None
    prev_ttl = None

    for current_time_str, current_ttl_str in res_arr_cluster:
        current_time = float(current_time_str)
        current_ttl = float(current_ttl_str)

        if valid_until is not None:
            time_diff = current_time - prev_time
            ttl_diff = prev_ttl - current_ttl + 2

            if not current_time > valid_until and time_diff > ttl_diff:
                return False, [current_time_str, time_diff, ttl_diff]

        valid_until = current_time + current_ttl - 1
        prev_time = current_time
        prev_ttl = current_ttl

    return True, None


def res_miss_cluster_is_valid(res_miss_cluster):
    valid_until = None

    for current_time_str, current_ttl_str in res_miss_cluster:
        current_time = float(current_time_str)
        current_ttl = float(current_ttl_str)

        if valid_until is not None and not current_time > valid_until:
            return False, [current_time_str, current_time, valid_until]

        valid_until = current_time + current_ttl - 1

    return True, None



def main():

    if len(sys.argv) < 2:
        raise Exception('No content directory specified!')

    content_dirs = sys.argv[1:]

    for content_dir in content_dirs:

        for root, _, filenames in walk(content_dir):

            for filename in filenames:
                if not 'res_' in filename or 'for_tests' in root:
                    continue

                is_res_arr = True if 'res_arr' in filename else False
                cluster = load_res_cluster(path.join(root, filename))

                if is_res_arr:
                    success, invalid_rec = res_arr_cluster_is_valid(cluster)
                else:
                    success, invalid_rec = res_miss_cluster_is_valid(cluster)

                if success:
                    print 'Success!'
                else:
                    print '%s: FAILURE!\t(%s)' % (filename, str(invalid_rec))



if __name__ == '__main__':
    main()
