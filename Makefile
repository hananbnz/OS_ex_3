CC = g++
CFLAGS = -Wall
STD = -std=c++11
FLAG = -c
FILES = uthreads.cpp uthreads.h Thread.cpp Thread.h
FILES2 = Thread.cpp
CLEAN = libuthreads.a uthreads.o Thread.o ex2.tar
TARSRCS = uthreads.cpp Thread.cpp Thread.h Makefile README

#make
all: libuthreads.a 

#object files
uthreads.o: uthreads.cpp uthreads.h Thread.cpp Thread.h
	$(CC) $(CFLAGS) $(STD) $(FLAG) $(FILES)

Thread.o: Thread.cpp Thread.h
	$(CC) $(CFLAGS) $(STD) $(FLAG) $(FILES2)

#Exectubles:

#Library
libuthreads.a: uthreads.o
	ar rcs libuthreads.a uthreads.o

#Tar
tar: libuthreads.a
	tar -cvf ex2.tar $(TARSRCS)

#Valgrind
val: 
		-valgrind --leak-check=full --show-possibly-lost=yes --show-reachable=yes \
	 --undef-value-errors=yes --track-origins=yes ./run_all


clean:
	-rm -f $(CLEAN)

.PHONY:clean ,all, tar
