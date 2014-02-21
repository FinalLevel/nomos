PRJ_ROOT=.

include ./Makefile.inc

all: $(NOMOS_OBJ) nomos.o test
	$(LINKER) -g $(OPFLAGS) -o nomos nomos.o $(NOMOS_OBJ) $(LIBDIRS) $(LIBS)
	
test: 
	cd  tests; make

%.o: %.cpp
	$(CC) -c $< -o $@ $(CPPFLAGS) $(OPFLAGS)

clean::
	find . -name "*.core" -exec rm {} \;
	find . -name "*.d" -exec rm {} \;
	find . -name "*.o" -exec rm {} \;

update::
	git status
	git pull origin master
