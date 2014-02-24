COMPILER=g++
LINKER=g++
make=make

LD=$(PRJ_ROOT)/fl_libs

LIBS=-lpthread -lmysqlclient
LIBDIRS=-L/usr/lib/mysql
INCLUDEDIRS=-I/usr/include/mysql -I$(PRJ_ROOT)/fl_libs -I$(PRJ_ROOT)/
CPPFLAGS=-DLINUX -D_THREAD_SAFE

LINK=$(LINKER)
LOBJ=$(LD)/read_write_lock.o $(LD)/dir.o $(LD)/network_buffer.o $(LD)/bstring.o $(LD)/file.o $(LD)/socket.o $(LD)/accept_thread.o $(LD)/log.o $(LD)/event_queue.o $(LD)/thread.o $(LD)/mutex.o $(LD)/event_thread.o $(LD)/time.o

NOMOS_OBJ=$(PRJ_ROOT)/index.o $(PRJ_ROOT)/item.o $(PRJ_ROOT)/config.o $(PRJ_ROOT)/nomos_log.o $(LOBJ)

OPFLAGS = 
CC=$(COMPILER) $(INCLUDEDIRS) -MD -g -Wall -std=c++0x
