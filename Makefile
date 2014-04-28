###############################
#   Filename: Makefile        #
#   Nedko Stefanov Nedkov     #
#   nedko.nedkov@inria.fr     #
#   April 2014		      #
###############################

clean:
	rm -rf *.pyc *~

cleanall:	clean
		rm -rf *.pyc results logs

prepare:	cleanall
		mkdir results logs

run:	cleanall
	python ./data_clustering.py

test:	./results
	./pre-commit.sh 4

