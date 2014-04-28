####################################
#   Filename: tester.py            #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from os import sys



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
    filename = sys.argv[1]
    flag = bool(int(sys.argv[2]))
    cluster = list()

    with open(filename) as fp:

        for line in fp:
            args = line.strip().split(separator)
            secs = args[0]
            ttl = args[1]
            cluster.append((secs, ttl))

    if flag:
        status, message = res_arr_cluster_is_valid(cluster)
        assert status, '%s: %s' % (filename, message)
    else:
        status, message = res_miss_cluster_is_valid(cluster)
        assert status, '%s: %s' % (filename, message)


if __name__ == '__main__':
    main()
