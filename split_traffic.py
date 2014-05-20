####################################
#   Filename: split_traffic.py     #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from operator import itemgetter



def main():
    def get_next_turn(clusters_len):
        lens = list()
        for i in range(4):
            try:
                lens.append(clusters_len[i])
            except KeyError:
                return i
        min_len = min(lens)

        for i in range(4):
            if clusters_len[i] == min_len:
                return i

    user_requests = dict()

    with open('./results/214/content_214_v4_0x0001/internal_view/req_arr_214_v4_0x0001.txt') as fp:
        for line in fp:
            user, secs = line.strip().split('\t')
            try:
                user_requests[user].append(secs)
            except KeyError:
                user_requests[user] = [secs]

    user_requests_len = [(user, len(requests)) for user, requests in user_requests.iteritems()]
    user_requests_len = sorted(user_requests_len, key=itemgetter(1), reverse=True)
    user_clusters = dict()
    clusters_len = dict()
    turn = 0

    for user, requests_len in user_requests_len:
        print user, requests_len
        try:
            user_clusters[turn].append(user)
            clusters_len[turn] += requests_len
        except KeyError:
            user_clusters[turn] = [user]
            clusters_len[turn] = requests_len

        turn = get_next_turn(clusters_len)

    for i in range(4):
        requests = list()

        for user in user_clusters[i]:
            requests += user_requests[user]

        print '%s\t%s\t%s' % (i, len(requests), len(user_clusters[i]))

        requests = sorted(requests)

        with open('input_%s.txt' % i, 'w') as fp:
            for request in requests:
                fp.write('%s\n' % request)



if __name__ == '__main__':
    main()
