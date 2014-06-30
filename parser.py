####################################
#   Filename: parser.py            #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   April 2014                     #
####################################

from config import SEPARATOR_2

import os, sys



def main():

    def flush_output(output, filepath):
        dirpath = os.path.dirname(filepath)
        if not os.path.isdir(dirpath):
            os.makedirs(dirpath)

        formatted_output = '%s%s' % ('\n'.join(output), '\n')

        with open(filepath, 'w') as fp:
            fp.write(formatted_output)


    in_filepath = sys.argv[1]

    with open(in_filepath) as fp:

        last_out_filepath = None
        output = list()

        for line in fp:
            out_filepath, out_line = line.strip().split(SEPARATOR_2)

            if last_out_filepath is None:
                last_out_filepath = out_filepath

            if last_out_filepath == out_filepath:
                output.append(out_line)
                continue

            flush_output(output, last_out_filepath)
            last_out_filepath = out_filepath
            output = [out_line]

        if output:
            flush_output(output, last_out_filepath)



if __name__ == '__main__':
    main()
