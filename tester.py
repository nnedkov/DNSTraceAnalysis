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

    for secs_since_epoch, ttl in res_arr_cluster:
        secs = float(secs_since_epoch)
        ttl = float(ttl)

        if prev_secs is not None:
            secs_diff = secs - prev_secs
            ttl_diff = prev_ttl - ttl + float(2)
            if not secs > prev_secs + prev_ttl - float(1) and secs_diff > ttl_diff:
                return False, [secs_since_epoch, secs_diff, ttl_diff]

        prev_secs = secs
        prev_ttl = ttl

    return True, None


def res_miss_cluster_is_valid(res_miss_cluster):
    prev_secs = None

    for secs_since_epoch, ttl in res_miss_cluster:
        secs = float(secs_since_epoch)
        ttl = float(ttl)

        if prev_secs is not None and prev_secs > secs + float(1):
            return False, [secs_since_epoch, prev_secs, secs]

        prev_secs = secs + ttl

    return True, None


if __name__ == '__main__':
    filename = sys.argv[1]
    flag = bool(int(sys.argv[2]))
    cluster = list()

    with open(filename) as fp:
        for line in fp:
            args = line.rstrip().split('\t')
            secs_since_epoch = args[0]
            ttl = args[1]
            cluster.append((secs_since_epoch, ttl))

    if flag:
        status, secs = res_arr_cluster_is_valid(cluster)
        assert status, secs
    else:
        status, secs = res_miss_cluster_is_valid(cluster)
        assert status, secs
