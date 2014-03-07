///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Replication threads classes unit tests
///////////////////////////////////////////////////////////////////////////////

#include <boost/test/unit_test.hpp>
#include <boost/test/output_test_stream.hpp> 

#include "test_path.hpp"
#include "time.hpp"
#include "index.hpp"
#include "index_replication_thread.hpp"

using namespace fl::nomos;
using namespace fl::fs;
using fl::chrono::Time;

BOOST_AUTO_TEST_SUITE( nomos )

BOOST_AUTO_TEST_CASE (testReplicationThreads)
{
	TestIndexPath testPath1;
	TestIndexPath binLogPath1;
	
	TestIndexPath testPath2;
	TestIndexPath binLogPath2;
	Time curTime;
	const char TEST_DATA[] = "1234567";	
	Buffer data;
	try
	{				
		Index index1(testPath1.path());
		BOOST_REQUIRE(index1.startReplicationLog(1, 3600, binLogPath1.path()));
		
		Socket listen;
		BOOST_REQUIRE(listen.listen("127.0.0.1", 43000));
		BOOST_REQUIRE(index1.startReplicationListenter(&listen));
		BOOST_REQUIRE(index1.create("testLevel", KEY_INT32, KEY_STRING));
		BOOST_REQUIRE(index1.create("testLevel2", KEY_STRING, KEY_STRING));
		
		TItemSharedPtr item(new Item(TEST_DATA, sizeof(TEST_DATA) - 1, curTime.unix() + 3600, curTime.unix()));
		BOOST_REQUIRE(index1.put("testLevel", "1", "testKey", item));
		BOOST_REQUIRE(index1.sync(curTime.unix()));
		
		Index index2(testPath2.path());
		BOOST_REQUIRE(index2.startReplicationLog(2, 3600, binLogPath2.path()));
		TServerList masters;
		Server master;
		master.ip = Socket::ip2Long("127.0.0.1");
		master.port = 43000;
		masters.push_back(master);
		BOOST_REQUIRE(index2.startReplication(masters));
		
		struct timespec tim;
		tim.tv_sec = 0;
		static const int RECHECK_TIME = 500000000; // 500 ms
		tim.tv_nsec = RECHECK_TIME;
		nanosleep(&tim , NULL);
		
		auto findItem = index2.find("testLevel", "1", "testKey", curTime.unix());
		BOOST_REQUIRE(findItem.get() != NULL);
		std::string getData((char*)findItem.get()->data(), findItem.get()->size());
		BOOST_CHECK(getData == TEST_DATA);
		BOOST_CHECK(findItem->header().liveTo == (uint32_t)(curTime.unix() + 3600));
		
		index2.exitFlush();
		index1.exitFlush();
	}
	catch (...)
	{
		BOOST_CHECK_NO_THROW(throw);
	}
};

BOOST_AUTO_TEST_SUITE_END()