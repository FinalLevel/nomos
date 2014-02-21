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

#include <boost/unordered_map.hpp>
#include <string>
#include <memory>

#include "mutex.hpp"
#include "item.hpp"
#include "exception.hpp"
#include "bstring.hpp"


namespace fl {
	namespace nomos {
		using fl::threads::Mutex;
		using fl::threads::AutoMutex;
		using fl::strings::BString;
		
		class IndexError : public fl::exceptions::Error
		{
		public:
			IndexError(const char *what)
				: Error(what)
			{
			}
		};
		
		enum EKeyType
		{
			KEY_STRING,
			KEY_INT8,
			KEY_INT16,
			KEY_INT32,
			KEY_INT64,
			KEY_MAX_TYPE = KEY_INT64 // should always be equal max key type
		};

		class TopLevelIndex
		{
		public:
			typedef uint8_t TVersion;
			static const TVersion CURRENT_VERSION = 1;
			struct MetaData
			{
				uint8_t version;
				uint8_t subLevelKeyType;
				uint8_t itemKeyType;
			};
			static TopLevelIndex *createFromDirectory(const std::string &path);
			static TopLevelIndex *create(const std::string &path, const EKeyType subLevelKeyType, const EKeyType itemKeyType);
			virtual bool load(const std::string &path) = 0;
			virtual class Item *find(const std::string &subLevel, const std::string &key) = 0;
		private:
			static void _formMetaFileName(const std::string &path, BString &metaFileName);
			static TopLevelIndex *_create(const std::string &path, MetaData &md);
		};
		
		class Index
		{
		public:
			Index(const std::string &path);
			~Index();
			
			bool create(const std::string &level, const EKeyType subLevelKeyType, const EKeyType itemKeyType);
			
			bool put(const std::string &level, const std::string &subLevel, const std::string &itemKey, Item &&item);
			bool load(const std::string &path);
			
			Index(const Index &) = delete;
			const size_t size() const
			{
				return _index.size();
			}
		private:
			bool _checkLevelName(const std::string &name);
			std::string _path;
			
			typedef std::string TTopLevelKey;
			typedef boost::unordered_map<TTopLevelKey, TopLevelIndex*> TTopLevelIndex;
			TTopLevelIndex _index;
			Mutex _sync;
		};
	};
};

#endif	// __FL_NOMOS_INDEX_HPP