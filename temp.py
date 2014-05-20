from sys import stdin

def main():
    prev_req_arr_num = None
    prev_ttl = None
    
    for input_line in stdin:
        args = input_line.strip().split('\t')
        assert len(args) == 5, input_line
        
        req_arr_num = int(args[1])
        ttl = int(args[2])
        
        if prev_req_arr_num is None and prev_ttl is None:
            prev_req_arr_num = req_arr_num
            prev_ttl = ttl
            continue
        
        if abs(req_arr_num - prev_req_arr_num) < 10 and abs(ttl - prev_ttl) > 10:
            print input_line
            
        prev_req_arr_num = req_arr_num
        prev_ttl = ttl


if __name__ == '__main__':
    main()
