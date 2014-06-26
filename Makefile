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
		python data_clustering.py

runall:		reducer_input.txt prepare
		cat reducer_input.txt | python reducer.py

analysis:	clustering_results
		rm -rf analysis_results
		mkdir analysis_results
		python analysis.py > analysis_results/info.txt

test:		clustering_results
		./pre-commit.sh 4



mapper_run:	prepare
		sed -i 's/RUNNING_ON_HADOOP = False/RUNNING_ON_HADOOP = True/g' config.py
		rm -rf reducer_input.txt
		cat $(DNS_TRACE_FILES) | python mapper.py | sort -k1,1 > reducer_input.txt

reducer_run:	reducer_input.txt
		rm -rf reducer_output.txt
		cat reducer_input.txt | python reducer.py > reducer_output.txt

hadoop_run:	mapper_run reducer_run reducer_output.txt
		python parser.py reducer_output.txt

simulation:
		cat input_0.txt | awk '{ print $1, "\t4"}' > input_temp.txt
		cat input_1.txt | awk '{ print $1, "\t5"}' >> input_temp.txt
		cat input_2.txt | awk '{ print $1, "\t6"}' >> input_temp.txt
		cat input_3.txt | awk '{ print $1, "\t7"}' >> input_temp.txt
		cat input_temp.txt | sort -k1,1 > input.txt
