#pragma once
#ifndef __FL_TEST_PATH_HPP
#define	__FL_TEST_PATH_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: TestPath helper class
///////////////////////////////////////////////////////////////////////////////
#include "dir.hpp"
#include "bstring.hpp"

namespace fl {
	namespace nomos {
		using fl::strings::BString;
		using fl::fs::Directory;
		
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
			const int countFiles(const char *subdir)
			{
				BString path;
				path.sprintfSet("%s/%s", _path.c_str(), subdir);
				int count = 0;
				Directory dir(path.c_str());
				while (dir.next())
				{
					if (dir.isDirectory())
						continue;
					count++;
				}
				return count;
			}
		private:
			std::string _path;
		};
	};
};

#endif	// __FL_TEST_PATH_HPP
