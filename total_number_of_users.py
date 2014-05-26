##########################################
#   Filename: total_number_of_users.py   #
#   Nedko Stefanov Nedkov                #
#   nedko.nedkov@inria.fr                #
#   May 2014                             #
##########################################

from config import RESULTS_DIR

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
    invalid_files_log = './invalid_files.log'
    total_users = set()

    for dirpath, _, filenames in walk(RESULTS_DIR):
        if not filenames:
            continue

        try:
            req_arr_filenames = [filename for filename in filenames if 'req_arr' in filename]
            if len(req_arr_filenames) > 2:
                dump_data(['1 - %s' % dirpath], invalid_files_log)
                continue
            req_arr_filename = req_arr_filenames[0]
        except IndexError:
            req_arr_filename = None

        try:
            users_filenames = [filename for filename in filenames if 'users' in filename]
            if len(users_filenames) > 1:
                dump_data(['2 - %s' % dirpath], invalid_files_log)
                continue
            users_filename = users_filenames[0]
        except IndexError:
            users_filename = None

        if req_arr_filename is None and users_filename is None:
            continue

        if req_arr_filename is None or users_filename is None:
            dump_data(['3 - %s' % dirpath], invalid_files_log)
            continue

        content_users = get_users(dirpath + '/' + users_filename)
        if content_users is None:
            dump_data(['4 - %s' % dirpath], invalid_files_log)
            continue
        elif not content_users:
            dump_data(['5 - %s' % dirpath], invalid_files_log)
            continue

        total_users |= content_users

    total_users_filename = './total_users.txt'
    dump_data(total_users, total_users_filename)


if __name__ == '__main__':
    main()
