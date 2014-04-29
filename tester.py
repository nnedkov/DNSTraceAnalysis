####################################
#   Filename: tester.py            #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from os import sys, walk



def res_arr_cluster_is_valid(res_arr_cluster):
    prev_secs = None
    prev_ttl = None

    for secs_str, ttl_str in res_arr_cluster:
        secs = float(secs_str)
        ttl = float(ttl_str)

        if prev_secs is not None:
            secs_diff = secs - prev_secs
            ttl_diff = prev_ttl - ttl + 2
            if not secs > prev_secs + prev_ttl - 1 and secs_diff > ttl_diff:
                return False, [secs_str, secs_diff, ttl_diff]

        prev_secs = secs
        prev_ttl = ttl

    return True, None


def res_miss_cluster_is_valid(res_miss_cluster):
    prev_secs = None

    for secs_str, ttl_str in res_miss_cluster:
        secs = float(secs_str)
        ttl = float(ttl_str)

        if prev_secs is not None and prev_secs > secs + 1:
            return False, [secs_str, prev_secs, secs]

        prev_secs = secs + ttl

    return True, None


def main(separator='\t'):

    def load_res_cluster(filename):
        cluster = list()

        with open(filename) as fp:

            for line in fp:
                args = line.strip().split(separator)
                secs = args[0]
                ttl = args[1]
                cluster.append((secs, ttl))

        return cluster


    if len(sys.argv) < 2:
        raise Exception('No content directory specified!')

    content_dir = sys.argv[1]

    for dirpath, _, filenames in walk(content_dir):

        for filename in filenames:
            if 'for_tests' in dirpath or not 'res_' in filename:
                continue

            is_res_arr = True if 'res_arr' in filename else False
            cluster = load_res_cluster(dirpath + '/' + filename)

            if is_res_arr:
                success, invalid_rec = res_arr_cluster_is_valid(cluster)
            else:
                success, invalid_rec = res_miss_cluster_is_valid(cluster)

            if success:
                print 'Success!'
            else:
                print '%s : FAILURE!\t\t%s' % (filename, str(invalid_rec))


if __name__ == '__main__':
    main()
