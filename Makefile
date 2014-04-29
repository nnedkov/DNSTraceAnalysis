###############################
#   Filename: Makefile        #
#   Nedko Stefanov Nedkov     #
#   nedko.nedkov@inria.fr     #
#   April 2014		      #
###############################

DNS_TRACE_FILES = ~/Inria/DNS_traces/dns2-sop-000 \
		  ~/Inria/DNS_traces/dns2-sop-001 \
		  ~/Inria/DNS_traces/dns2-sop-002 \
		  ~/Inria/DNS_traces/dns2-sop-003 \
		  ~/Inria/DNS_traces/dns2-sop-004 \
		  ~/Inria/DNS_traces/dns2-sop-005 \
		  ~/Inria/DNS_traces/dns2-sop-006 \
		  ~/Inria/DNS_traces/dns2-sop-007 \
		  ~/Inria/DNS_traces/dns2-sop-008

clean:
	rm -rf *.pyc *~

cleanall:	clean
		rm -rf *.pyc results logs

prepare:	cleanall
		mkdir results logs

run:	prepare
	sed -i 's/RUNNING_ON_HADOOP = True/RUNNING_ON_HADOOP = False/g' ./config.py
	python ./data_clustering.py

test:	./results
	./pre-commit.sh 4

mapper_run:
		cat $(DNS_TRACE_FILES) | python mapper.py | sort -k1,1 > reducer_input.txt

reducer_run:	reducer_input.txt
		cat reducer_input.txt | python reducer.py > reducer_output.txt

hadoop_run:   prepare
	      sed -i 's/RUNNING_ON_HADOOP = False/RUNNING_ON_HADOOP = True/g' ./config.py
	      mapper_run
	      reducer_run

