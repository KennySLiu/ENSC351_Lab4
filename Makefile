# "all" will compile the multithreaded wordcount, using the openMP framework.
all: 
	g++ -std=c++11 -o wordcount.exe -fopenmp kenny_include.h mapreduce_host.h word_count.cpp

singlethread:
	g++ -std=c++11 -o single_threaded_wordcount.exe kenny_include.h single_threaded_wordcount.cpp

clean: 
	rm wordcount.exe *.h.gch 
