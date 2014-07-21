#############################
#   Filename: Makefile      #
#   Nedko Stefanov Nedkov   #
#   nedko.nedkov@inria.fr   #
#   April 2014              #
#############################

DNS_TRACE_FILES = ~/Inria/DNS_trace/dns2-sop-000 \
                  ~/Inria/DNS_trace/dns2-sop-001 \
                  ~/Inria/DNS_trace/dns2-sop-002 \
                  ~/Inria/DNS_trace/dns2-sop-003 \
                  ~/Inria/DNS_trace/dns2-sop-004 \
                  ~/Inria/DNS_trace/dns2-sop-005 \
                  ~/Inria/DNS_trace/dns2-sop-006 \
                  ~/Inria/DNS_trace/dns2-sop-007 \
                  ~/Inria/DNS_trace/dns2-sop-008

clean:
	rm -f *.pyc *~

cleanall:	clean
		rm -rf clustering_results analysis_results logs

prepare:	cleanall
		mkdir clustering_results analysis_results
		sed -i 's/RUNNING_ON_HADOOP = True/RUNNING_ON_HADOOP = False/g' config.py

runfew:		prepare
		python clustering.py

runall:		reducer_input.txt prepare
		cat reducer_input.txt | python reducer.py

test:		clustering_results
		./pre-commit.sh 4

analysis:	clustering_results
		rm -rf analysis_results
		mkdir analysis_results
		python analysis.py > analysis_results/info.txt

mapper_run:	prepare
		cat $(DNS_TRACE_FILES) | python mapper.py | sort -k1,1 > reducer_input.txt

reducer_run:	reducer_input.txt
		cat reducer_input.txt | python reducer.py > reducer_output.txt

merge_traffic_files:	input_0.txt input_1.txt input_2.txt input_3.txt input_4.txt
			cat input_0.txt | awk '{ print $1, "\t4"}' > input_temp.txt
			cat input_1.txt | awk '{ print $1, "\t5"}' >> input_temp.txt
			cat input_2.txt | awk '{ print $1, "\t6"}' >> input_temp.txt
			cat input_3.txt | awk '{ print $1, "\t7"}' >> input_temp.txt
			cat input_temp.txt | sort -k1,1 > input.txt
			rm input_temp.txt

