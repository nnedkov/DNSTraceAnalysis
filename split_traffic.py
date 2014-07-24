#####################################
#   Filename: split_traffic.py      #
#   Nedko Stefanov Nedkov           #
#   nedko.nedkov@inria.fr           #
#   April 2014                      #
#####################################

from config import USER_CLUSTERS_NUMBER, REQ_ARR_FILE_TO_SPLIT, SEPARATOR_1, \
                   VERBOSITY_IS_ON, ANALYSIS_RESULTS_DIR

from operator import itemgetter



def get_next_turn(user_clusters):
    req_nums = list()

    for i in range(USER_CLUSTERS_NUMBER):
        try:
            req_nums.append(user_clusters[i][1])
        except KeyError:
            return i

    min_req_num = min(req_nums)

    for i in range(USER_CLUSTERS_NUMBER):
        if user_clusters[i][1] == min_req_num:
            return i



def main():

    user_requests = dict()

    with open(REQ_ARR_FILE_TO_SPLIT) as fp:

        for line in fp:
            user, request = line.strip().split(SEPARATOR_1)
            try:
                user_requests[user].append(request)
            except KeyError:
                user_requests[user] = [request]

    user_requests_num = [(user, len(requests)) for user, requests in user_requests.iteritems()]
    user_requests_num = sorted(user_requests_num, key=itemgetter(1), reverse=True)
    user_clusters = dict()

    for user, requests_num in user_requests_num:
        if VERBOSITY_IS_ON:
            print 'User %s is issuing %s requests' % (user, requests_num)

        turn = get_next_turn(user_clusters)
        try:
            user_clusters[turn][0].append(user)
            user_clusters[turn][1] += requests_num
        except KeyError:
            user_clusters[turn] = ([user], requests_num)

    for i in range(USER_CLUSTERS_NUMBER):
        users = user_clusters[i][0]
        requests = list()

        for user in users:
            requests += user_requests[user]

        sim_input_filepath = '%s/simulation_input_%s.txt' % (ANALYSIS_RESULTS_DIR, i)

        with open('input_%s.txt' % i, 'w') as fp:

            for request in sorted(requests):
                fp.write('%s\n' % request)

        if VERBOSITY_IS_ON:
            print 'File %s has %s requests from %s different users' % (sim_input_filepath,
                                                                       len(requests),
                                                                       len(users))



if __name__ == '__main__':
    main()
