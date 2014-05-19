####################################
#   Filename: popularity.py        #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import RESULTS_DIR

from os import walk



def get_users(filepath):
    users = list()

    with open(filepath) as fp:
        for line in fp:
            user = line.strip()
            users.append(user)

    assert len(users) == len(set(users))

    return set(users)



def main():
    total_users = set()

    for dirpath, _, filenames in walk(RESULTS_DIR):
        if not filenames:
            continue

        try:
            req_arr_filenames = [filename for filename in filenames if 'req_arr' in filename]
            assert len(req_arr_filenames) <= 1
            req_arr_filename = req_arr_filenames[0]
        except IndexError:
            req_arr_filename = None

        try:
            users_filenames = [filename for filename in filenames if 'users' in filename]
            assert len(users_filenames) <= 1
            users_filename = users_filenames[0]
        except IndexError:
            users_filename = None

        if req_arr_filename is None and users_filename is None:
            continue

        assert req_arr_filename is not None and users_filename is not None, [req_arr_filename, users_filename]

        content_users = get_users(dirpath + '/' + users_filename)
        assert content_users, dirpath + '/' + users_filename

        total_users |= content_users

    print len(total_users)


if __name__ == '__main__':
    main()
