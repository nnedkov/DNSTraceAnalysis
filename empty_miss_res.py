####################################
#   Filename: empty_miss_res.py    #
#   Nedko Stefanov Nedkov          #
#   nedko.nedkov@inria.fr          #
#   June 2014                      #
####################################

from config import SEPARATOR

from trace import Trace



def reducer(reducer_input, contents, separator='\t'):

    def process_content(content, traces):
        traces = sorted(traces, key=lambda trace: trace.get_secs_value())

        issues_req_miss = bool([True for trace in traces if trace.type in [3, 4]])
        max_ttl = max([trace.ttl for trace in traces if trace.ttl != None])

        return issues_req_miss, max_ttl


    last_content = None
    traces = list()
    results = dict()
    i = 0

    for input_line in reducer_input:
        content, trace_str = input_line.split(separator, 1)
        trace_str = separator.join(trace_str.split(SEPARATOR))

        if content not in contents:
            continue

        trace = Trace(trace_str)
        trace.fill_out()

        if last_content is None:
            last_content = content

        if last_content == content:
            traces.append(trace)
            continue

        result = process_content(last_content, traces)
        results[content] = result
        print '%s/%s' % (i, len(contents))
        i += 1

        last_content = content
        traces = [trace]

    if traces:
        result = process_content(last_content, traces)
        results[content] = result

    return results


def main(separator='\t'):
    print 'Loading trace into memory...'

    reducer_input_filename = './reducer_input.txt'
    reducer_input = list()

    with open(reducer_input_filename) as fp:
        for line in fp:
            reducer_input.append(line.strip())

    print 'Processing the popularity file...'

    popularity_filename = './popularity.txt'
    new_popularity_filename = './new_popularity.txt'
    content_arguments = dict()

    with open(popularity_filename) as fpi:
        for line in fpi:
            args = line.strip().split(separator)
            assert len(args) == 8
            req_miss_num = int(args[2])

            if req_miss_num != 0:
                with open(new_popularity_filename, 'a') as fpo:
                    fpo.write(line)

            content_args = args[0].split('_')

            assert content_args[1] in ['v4', 'v6', 'sec']
            if content_args[1] == 'v4':
                content_args[1] = '0x0001'
            elif content_args[1] == 'v6':
                content_args[1] = '0x001c'
            elif content_args[1] == 'sec':
                content_args[1] = '0x000c'
            content = '_'.join([content_args[0], content_args[2], content_args[1]])

            content_arguments[(content, content_args[3])] = args

    contents = set([content for content, _ in content_arguments.keys()])
    results = reducer(reducer_input, contents)
    keyerrors = list()

    for content, view in content_arguments.keys():
        try:
            issues_req_miss, max_ttl = results[content]
        except KeyError:
            keyerrors.append(content)
            continue
        args = content_arguments[(content, view)]
        args[2] = str(issues_req_miss)
        args[7] = "[ >= '%s']" % max_ttl

        with open(new_popularity_filename, 'a') as fpo:
            fpo.write(separator.join(args) + '\n')

    print len(keyerrors), keyerrors


if __name__ == '__main__':
    main()
