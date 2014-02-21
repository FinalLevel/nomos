PRJ_ROOT=..

include ../Makefile.inc

OPFLAGS = 
TEST_OBJ=index_test.o test.o $(NOMOS_OBJ)

TEST_LIBS=-lboost_unit_test_framework-mt -lboost_filesystem-mt

all: test
	./test

test: $(TEST_OBJ)
	$(LINKER) -g $(OPFLAGS) -o test $(TEST_OBJ) $(LIBDIRS) $(LIBS) $(TEST_LIBS) 

%.o: %.cpp
	$(CC) -c $< -o $@ $(CPPFLAGS) $(OPFLAGS)

include $(wildcard *.d)

clean::
	rm -f *.o *.d
	$(make) -C ../ clean
update::
	$(make) -C ../ update


