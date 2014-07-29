#######################################
#   Filename: Makefile                #
#   Nedko Stefanov Nedkov             #
#   nedko.stefanov.nedkov@gmail.com   #
#   April 2014                        #
#######################################

CLUSTERING_RESULTS_DIR = ./clustering_results
ANALYSIS_RESULTS_DIR = ./analysis_results
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
		rm -rf $(CLUSTERING_RESULTS_DIR) $(ANALYSIS_RESULTS_DIR) logs

prepare:	cleanall
		mkdir $(CLUSTERING_RESULTS_DIR) $(ANALYSIS_RESULTS_DIR)
		sed -i 's/RUNNING_ON_HADOOP = True/RUNNING_ON_HADOOP = False/g' config.py

runfew:		prepare
		python clustering.py

mapper_run:
		cat $(DNS_TRACE_FILES) | python mapper.py > unsorted_reducer_input.txt
		sort -k1,1 unsorted_reducer_input.txt -o reducer_input.txt

reducer_run:	reducer_input.txt prepare
		python reducer.py

runall:
		mapper_run
		reducer_run

test:		$(CLUSTERING_RESULTS_DIR)
		./pre-commit.sh 4

analysis:	$(CLUSTERING_RESULTS_DIR)
		rm -rf $(ANALYSIS_RESULTS_DIR)
		mkdir $(ANALYSIS_RESULTS_DIR)
		python analysis.py > $(ANALYSIS_RESULTS_DIR)/info.txt

split_traffic:	$(ANALYSIS_RESULTS_DIR)
		python split_traffic.py

