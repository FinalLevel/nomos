AUTOMAKE_OPTIONS = subdir-objects
ACLOCAL_AMFLAGS = ${ACLOCAL_FLAGS} -I m4

AM_CPPFLAGS=-I./fl_libs

SUBDIRS = fl_libs
LDADD = fl_libs/libfl.a

NOMOS_FILES = index_replication_thread.cpp index_sync_thread.cpp nomos_event.cpp index.cpp item.cpp config.cpp nomos_log.cpp

bin_PROGRAMS = nomos
nomos_SOURCES = nomos.cpp $(NOMOS_FILES)

check_PROGRAMS = nomos_test
nomos_test_SOURCES = tests/test.cpp tests/index_test.cpp tests/replication_thread_test.cpp $(NOMOS_FILES)
nomos_test_LDFLAGS = $(BOOST_LDFLAGS) $(BOOST_UNIT_TEST_FRAMEWORK_LIB)

TESTS = nomos_test



