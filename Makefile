COMPILER=g++
LINKER=g++
make=make

LD=./fl_libs

LIBS=-lpthread -lmysqlclient
LIBDIRS=-L/usr/lib/mysql
INCLUDEDIRS=-I/usr/include/mysql -I./fl_libs -I./
CPPFLAGS=-DLINUX -D_THREAD_SAFE

LINK=$(LINKER)
LOBJ=$(LD)/network_buffer.o $(LD)/bstring.o $(LD)/file.o $(LD)/socket.o $(LD)/accept_thread.o $(LD)/log.o $(LD)/event_queue.o $(LD)/thread.o $(LD)/mutex.o $(LD)/event_thread.o $(LD)/time.o

OPFLAGS = 
CC=$(COMPILER) $(INCLUDEDIRS) -MD -g -Wall -std=c++0x

NOMOS_OBJ=nomos.o config.o nomos_log.o $(LOBJ)

all: $(NOMOS_OBJ)
	$(LINKER) -g $(OPFLAGS) -o nomos $(NOMOS_OBJ) $(LIBDIRS) $(LIBS)

%.o: %.cpp
	$(CC) -c $< -o $@ $(CPPFLAGS) $(OPFLAGS)

clean::
	find . -name "*.core" -exec rm {} \;
	find . -name "*.d" -exec rm {} \;
	find . -name "*.o" -exec rm {} \;

update::
	git status
	git pull origin master
