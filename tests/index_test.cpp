///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
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


using namespace fl::nomos;
using namespace fl::fs;
using fl::strings::BString;

class TestIndexPath
{
public:
	TestIndexPath()
	{
		BString path;
		path.sprintfSet("/tmp/test_nomos_index_%u", rand());
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
		BOOST_CHECK(index.create("testLevel", KEY_INT8, KEY_STRING));
		BOOST_CHECK(index.create("test,Level", KEY_INT8, KEY_STRING) == false);
		BOOST_CHECK(index.create("09-_Level", KEY_INT8, KEY_STRING));
		BOOST_CHECK(index.create("09-_Level", KEY_INT8, KEY_INT64) == false);
		
		Index indexLoad(testPath.path());
		BOOST_CHECK(indexLoad.size() == 2);
	);
}

BOOST_AUTO_TEST_SUITE_END()
