#pragma once
#ifndef __FL_NOMOS_INDEX_HPP
#define	__FL_NOMOS_INDEX_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Index maintenance classes
///////////////////////////////////////////////////////////////////////////////

#include "mutex.hpp"
#include <boost/unordered_map.hpp>

namespace fl {
	namespace nomos {
	
		class TopLevelIndexInterface
		{
		public:
			virtual bool load(const std::string &fileName) = 0;
			virtual class Item *find(const std::string &subLevel, const std::string &key) = 0;
		};
		
		template <typename TSubLevelKey, typename TItemKey>
		class MemmoryTopLevelIndex : public TopLevelIndexInterface
		{
		public:
			
		private:
			typedef boost::unordered_map<TItemKey, class Item> TItemIndex;
			typedef boost::unordered_map<TSubLevelKey, TItemIndex> TSubLevelIndex;
			TSubLevelIndex _subLevelItem;
		};
		
		class TopIndex
		{
		public:
			bool load(const std::string &path);
		private:
			std::string _path;
			
			typedef u_int32_t TTopLevelKey;
			typedef boost::unordered_map<TTopLevelKey, TopLevelIndexInterface*> TTopLevelIndex;
			TTopLevelIndex _index;
		};
	};
};

#endif	// __FL_NOMOS_INDEX_HPP