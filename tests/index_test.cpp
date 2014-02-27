///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Index class unit tests
///////////////////////////////////////////////////////////////////////////////

#include <boost/test/unit_test.hpp>
#include <boost/test/output_test_stream.hpp> 

#include "dir.hpp"
#include "bstring.hpp"

#include "index.hpp"
#include "time.hpp"


using namespace fl::nomos;
using namespace fl::fs;
using fl::strings::BString;
using fl::chrono::Time;

class TestIndexPath
{
public:
	TestIndexPath()
	{
		static int call = 0;
		call++;
		BString path;
		path.sprintfSet("/tmp/test_nomos_index_%u_%u", call, rand());
		_path = path.c_str();
		Directory::makeDirRecursive(_path.c_str());
	}
		
	~TestIndexPath()
	{
		Directory::rmDirRecursive(_path.c_str());
	}
	
	const char *path() const
	{
		return _path.c_str();
	}
private:
	std::string _path;
};


BOOST_AUTO_TEST_SUITE( nomos )

BOOST_AUTO_TEST_CASE( CreateIndex )
{
	TestIndexPath testPath;
	BOOST_CHECK_NO_THROW(
		Index index(testPath.path());
		BOOST_CHECK(index.create("testLevel", KEY_INT32, KEY_STRING));
		BOOST_CHECK(index.create("test,Level", KEY_INT32, KEY_STRING) == false);
		BOOST_CHECK(index.create("09-_Level", KEY_INT32, KEY_STRING));
		BOOST_CHECK(index.create("09-_Level", KEY_INT32, KEY_INT64) == false);
		
		Index indexLoad(testPath.path());
		BOOST_CHECK(indexLoad.size() == 2);
	);
}


BOOST_AUTO_TEST_CASE( ClearOld )
{
	TestIndexPath testPath;
	Time curTime;
	
	BOOST_CHECK_NO_THROW(
		Index index(testPath.path());
		TItemSharedPtr item(new Item());
		item->setLiveTo(curTime.unix() + 1, curTime.unix());
		BOOST_CHECK(index.create("testLevel", KEY_INT32, KEY_STRING));
		BOOST_CHECK(index.put("testLevel", "1", "testKey", item));
		BOOST_CHECK(index.put("testLevel", "1", "testKey2", item));
		BOOST_CHECK(index.find("testLevel", "1", "testKey", curTime.unix()).get() == item.get());
		BOOST_CHECK(index.find("testLevel", "1", "testKey2", curTime.unix()).get() == item.get());
		index.clearOld(curTime.unix() + 1);
		BOOST_CHECK(index.find("testLevel", "1", "testKey", curTime.unix()).get() == NULL);
		BOOST_CHECK(index.find("testLevel", "1", "testKey2", curTime.unix()).get() == NULL);
	);
}

BOOST_AUTO_TEST_CASE( AddFindTouchIndex )
{
	TestIndexPath testPath;
	Time curTime;
	BOOST_CHECK_NO_THROW(
		Index index(testPath.path());
		TItemSharedPtr item(new Item());
		item->setLiveTo(curTime.unix(), curTime.unix());
		BOOST_CHECK(index.create("testLevel", KEY_INT32, KEY_STRING));
		BOOST_CHECK(index.put("testLevel", "1", "testKey", item));
		BOOST_CHECK(index.find("testLevel", "1", "testKey", curTime.unix()).get() == NULL);
		item->setLiveTo(curTime.unix() + 1, curTime.unix());
		BOOST_CHECK(index.put("testLevel", "1", "testKey", item));
		BOOST_CHECK(index.find("testLevel", "1", "testKey", curTime.unix()).get() == item.get());
		
		BOOST_CHECK(index.touch("testLevel", "1", "testKey", 1, curTime.unix()));
		BOOST_CHECK(index.find("testLevel", "1", "testKey", curTime.unix() + 1).get() == NULL);
	);
}

BOOST_AUTO_TEST_CASE( AddFindRemoveIndex )
{
	TestIndexPath testPath;
	Time curTime;
	BOOST_CHECK_NO_THROW(
		Index index(testPath.path());

		TItemSharedPtr item(new Item());
		BOOST_CHECK(index.create("testLevel", KEY_INT32, KEY_STRING));
		BOOST_CHECK(index.put("testLevel", "1", "testKey", item));
		auto findItem = index.find("testLevel", "1", "testKey", curTime.unix());
		BOOST_CHECK(findItem.get() != NULL);
		
		TItemSharedPtr item2(new Item());
		BOOST_CHECK(index.put("testLevel", "1", "testKey", item2));
		findItem = index.find("testLevel", "1", "testKey", curTime.unix());
		BOOST_CHECK(findItem.get() == item2.get());

		BOOST_CHECK(index.put("testLevel", "1", "testKey2", item));
		findItem = index.find("testLevel", "1", "testKey2", curTime.unix());
		BOOST_CHECK(findItem.get() == item.get());
		
		BOOST_CHECK(index.find("testLevel", "1", "testKey3", curTime.unix()).get() == NULL);
		
		BOOST_CHECK(index.remove("testLevel", "1", "testKey"));
		findItem = index.find("testLevel", "1", "testKey", curTime.unix());
		BOOST_CHECK(findItem.get() == NULL);
		BOOST_CHECK(index.remove("testLevel", "1", "testKey") == false);
	);
}

BOOST_AUTO_TEST_CASE( testIndexSyncPut )
{
	TestIndexPath testPath;
	Time curTime;
	const char TEST_DATA[] = "1234567";
	const char TEST_OVERWRITE[] = "new data";
	BOOST_CHECK_NO_THROW(
		Index index(testPath.path());
		BOOST_CHECK(index.create("testLevel", KEY_INT32, KEY_STRING));
		
		TItemSharedPtr item(new Item(TEST_DATA, sizeof(TEST_DATA) - 1, 0, curTime.unix() + 1));
		BOOST_CHECK(index.put("testLevel", "1", "testKey", item));
		
		TItemSharedPtr item2(new Item(TEST_DATA, sizeof(TEST_DATA) - 1, curTime.unix(), curTime.unix()));
		BOOST_CHECK(index.put("testLevel", "1", "testKey2", item2));
	
		BOOST_CHECK(index.put("testLevel", "1", "testKey3", item));
		TItemSharedPtr item3(new Item(TEST_OVERWRITE, sizeof(TEST_OVERWRITE) - 1, 0, curTime.unix() + 1));
		BOOST_CHECK(index.put("testLevel", "1", "testKey3", item3));

		BOOST_CHECK(index.sync(curTime.unix()));
	);
	BOOST_CHECK_NO_THROW(
		Index index(testPath.path());
		BOOST_CHECK(index.load(curTime.unix()));
	
		auto findItem = index.find("testLevel", "1", "testKey", curTime.unix());
		BOOST_REQUIRE(findItem.get() != NULL);
		std::string getData((char*)findItem.get()->data(), findItem.get()->size());
		BOOST_CHECK(getData == TEST_DATA);
		
		BOOST_CHECK(index.find("testLevel", "1", "testKey2", curTime.unix()).get() == NULL);
		
		findItem = index.find("testLevel", "1", "testKey3", curTime.unix());
		BOOST_REQUIRE(findItem.get() != NULL);
		getData.assign((char*)findItem.get()->data(), findItem.get()->size());
		BOOST_CHECK(getData == TEST_OVERWRITE);
	);
}

BOOST_AUTO_TEST_CASE( testIndexSyncTouch )
{
	TestIndexPath testPath;
	Time curTime;
	const char TEST_DATA[] = "1234567";	
	const int ADD_TIME = 10;
	BOOST_CHECK_NO_THROW(
		Index index(testPath.path());
		BOOST_CHECK(index.create("testLevel", KEY_INT32, KEY_STRING));
		
		TItemSharedPtr item(new Item(TEST_DATA, sizeof(TEST_DATA) - 1, curTime.unix() + 1, curTime.unix()));
		BOOST_CHECK(index.put("testLevel", "1", "testKey", item));
		BOOST_CHECK(index.sync(curTime.unix()));
		BOOST_CHECK(index.touch("testLevel", "1", "testKey", ADD_TIME + 1, curTime.unix()));

		TItemSharedPtr item2(new Item(TEST_DATA, sizeof(TEST_DATA) - 1, curTime.unix() + 1, curTime.unix()));
		BOOST_CHECK(index.put("testLevel", "1", "testKey2", item2));
		BOOST_CHECK(index.sync(curTime.unix()));
	);
	BOOST_CHECK_NO_THROW(
		Index index(testPath.path());
		BOOST_CHECK(index.load(curTime.unix() + ADD_TIME));
	
		auto findItem = index.find("testLevel", "1", "testKey", curTime.unix() + ADD_TIME);
		BOOST_REQUIRE(findItem.get() != NULL);
		std::string getData((char*)findItem.get()->data(), findItem.get()->size());
		BOOST_CHECK(getData == TEST_DATA);
		
		BOOST_CHECK(index.find("testLevel", "1", "testKey2", curTime.unix() + ADD_TIME).get() == NULL);
	);
}

BOOST_AUTO_TEST_CASE( testIndexSyncRemove )
{
	TestIndexPath testPath;
	Time curTime;
	const char TEST_DATA[] = "1234567";	
	BOOST_CHECK_NO_THROW(
		Index index(testPath.path());
		BOOST_CHECK(index.create("testLevel", KEY_INT32, KEY_STRING));
		
		TItemSharedPtr item(new Item(TEST_DATA, sizeof(TEST_DATA) - 1, curTime.unix() + 1, curTime.unix()));
		BOOST_CHECK(index.put("testLevel", "1", "testKey", item));
		BOOST_CHECK(index.sync(curTime.unix()));
		BOOST_CHECK(index.remove("testLevel", "1", "testKey"));

		TItemSharedPtr item2(new Item(TEST_DATA, sizeof(TEST_DATA) - 1, curTime.unix() + 1, curTime.unix()));
		BOOST_CHECK(index.put("testLevel", "1", "testKey2", item2));
		BOOST_CHECK(index.sync(curTime.unix()));
	);
	BOOST_CHECK_NO_THROW(
		Index index(testPath.path());
		BOOST_CHECK(index.load(curTime.unix()));
	
		auto findItem = index.find("testLevel", "1", "testKey2", curTime.unix());
		BOOST_REQUIRE(findItem.get() != NULL);
		std::string getData((char*)findItem.get()->data(), findItem.get()->size());
		BOOST_CHECK(getData == TEST_DATA);
		
		BOOST_CHECK(index.find("testLevel", "1", "testKey", curTime.unix()).get() == NULL);
	);
}

BOOST_AUTO_TEST_CASE( testIndexPack )
{
	TestIndexPath testPath;
	Time curTime;
	const char TEST_DATA[] = "1234567";	
	BOOST_CHECK_NO_THROW(
		Index index(testPath.path());
		BOOST_CHECK(index.create("testLevel", KEY_INT32, KEY_STRING));
		
		TItemSharedPtr item(new Item(TEST_DATA, sizeof(TEST_DATA) - 1, curTime.unix() + 1, curTime.unix()));
		BOOST_CHECK(index.put("testLevel", "1", "testKey", item));
		BOOST_CHECK(index.sync(curTime.unix()));
		BOOST_CHECK(index.remove("testLevel", "1", "testKey"));

		TItemSharedPtr item2(new Item(TEST_DATA, sizeof(TEST_DATA) - 1, curTime.unix() + 1, curTime.unix()));
		BOOST_CHECK(index.put("testLevel", "1", "testKey2", item2));
		BOOST_CHECK(index.sync(curTime.unix()));
		
		BOOST_CHECK(index.pack(curTime.unix()));
		BOOST_CHECK(index.pack(curTime.unix()));
	);
	BOOST_CHECK_NO_THROW(
		Index index(testPath.path());
		BOOST_CHECK(index.load(curTime.unix()));
	
		auto findItem = index.find("testLevel", "1", "testKey2", curTime.unix());
		BOOST_REQUIRE(findItem.get() != NULL);
		std::string getData((char*)findItem.get()->data(), findItem.get()->size());
		BOOST_CHECK(getData == TEST_DATA);
		
		BOOST_CHECK(index.find("testLevel", "1", "testKey", curTime.unix()).get() == NULL);
	);
}

BOOST_AUTO_TEST_SUITE_END()