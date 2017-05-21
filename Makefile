CC = g++
CFLAGS = -Wall
STD = -std=c++11
FLAG = -c
FLAG2 = -o
FILES = MapReduceClient.cpp MapReduceFramework.h MapReduceFramework.cpp MapReduceClient.h
FILES2 = search.cpp MapReduceFramework.h MapReduceClient.h
CLEAN = MapReduceFramework.a search.o MapReduceFramework.o ex3.tar
TARSRCS = MapReduceFramework.cpp search.cpp README Makefile

#make
all: MapReduceFramework.a 

#object files
MapReduceFramework.o: MapReduceClient.cpp MapReduceFramework.h MapReduceFramework.cpp MapReduceClient.h
	$(CC) $(CFLAGS) $(STD) $(FLAG) $(FILES)

search.o: search.cpp MapReduceFramework.h MapReduceClient.h
	$(CC) $(CFLAGS) $(STD) $(FLAG) $(FILES2)

#Exectubles:
search: search.o MapReduceFramework.h MapReduceClient.h
	$(CC) $(CFLAGS) $(STD) $(FLAG2) search.o MapReduceFramework.h MapReduceClient.h

#Library
MapReduceFramework.a: MapReduceFramework.o
	ar rcs MapReduceFramework.a MapReduceFramework.o

#Tar
tar: MapReduceFramework.a
	tar -cvf ex3.tar $(TARSRCS)

#Valgrind
val: 
		-valgrind --leak-check=full --show-possibly-lost=yes --show-reachable=yes \
	 --undef-value-errors=yes --track-origins=yes ./run_all


clean:
	-rm -f $(CLEAN)

.PHONY:clean ,all, tar
