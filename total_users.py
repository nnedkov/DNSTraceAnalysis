################################
#   Filename: total_users.py   #
#   Nedko Stefanov Nedkov      #
#   nedko.nedkov@inria.fr      #
#   May 2014                   #
################################

from config import ANALYSIS_RESULTS_DIR, CLUST_RESULTS_DIR

from data_dumping import dump_data

from os import walk



def get_users(filepath):
    users = list()

    with open(filepath) as fp:
        users += [line.strip() for line in fp]

    if len(users) != len(set(users)):
        return None

    return set(users)


def main():
    invalid_dirs_log = '%s/invalid_dirs.log' % ANALYSIS_RESULTS_DIR
    total_users = set()

    for dirpath, _, filenames in walk(CLUST_RESULTS_DIR):
        if not filenames or 'for_tests' in dirpath:
            continue

        try:
            req_arr_filenames = [f for f in filenames if 'req_arr' in f]
            if len(req_arr_filenames) > 1:
                dump_data(['error 1: %s' % dirpath], invalid_dirs_log)
                continue
            req_arr_filename = req_arr_filenames[0]
        except IndexError:
            req_arr_filename = None

        try:
            users_filenames = [f for f in filenames if 'users' in f]
            if len(users_filenames) > 1:
                dump_data(['error 2: %s' % dirpath], invalid_dirs_log)
                continue
            users_filename = users_filenames[0]
        except IndexError:
            users_filename = None

        if req_arr_filename is None and users_filename is None:
            continue
        elif req_arr_filename is None or users_filename is None:
            dump_data(['error 3: %s' % dirpath], invalid_dirs_log)
            continue

        content_users = get_users(dirpath + '/' + users_filename)
        if content_users is None:
            dump_data(['error 4: %s' % dirpath], invalid_dirs_log)
            continue
        elif not content_users:
            dump_data(['error 5: %s' % dirpath], invalid_dirs_log)
            continue

        total_users |= content_users

    total_users_filename = '%s/total_users.txt' % ANALYSIS_RESULTS_DIR
    dump_data(total_users, total_users_filename)



if __name__ == '__main__':
    main()
