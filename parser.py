####################################
#   Filename: parser.py            #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import SEPARATOR_1

import sys, os



def main():

    def write_it(filepath, output):
        dirname = os.path.dirname(filepath)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        with open(filepath, 'w') as fp:
            fp.write('\n'.join(output) + '\n')


    in_filename = sys.argv[1]

    with open(in_filename) as fp:
        last_filepath = None
        output = list()

        for line in fp:
            out_filepath, output_line = line.strip().split(SEPARATOR_1)

            if last_filepath is None:
                last_filepath = out_filepath

            if last_filepath == out_filepath:
                output.append(output_line)
                continue

            write_it(last_filepath, output)
            last_filepath = out_filepath
            output = [output_line]

        if output:
            write_it(last_filepath, output)


if __name__ == '__main__':
    main()
