#pragma once
#ifndef __FL_NOMOS_INDEX_HPP
#define	__FL_NOMOS_INDEX_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Index maintenance classes
///////////////////////////////////////////////////////////////////////////////


#ifdef _USE_BOOST
	#include <boost/unordered_map.hpp>
	using boost::unordered_map;
#else
	#include <unordered_map>
	using std::unordered_map;
#endif

#include <string>
#include <memory>

#include "mutex.hpp"
#include "item.hpp"
#include "exception.hpp"
#include "bstring.hpp"
#include "file.hpp"


namespace fl {
	namespace nomos {
		using fl::threads::Mutex;
		using fl::threads::AutoMutex;
		using fl::strings::BString;
		using fl::fs::File;
	
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
			KEY_INT32,
			KEY_INT64,
			KEY_MAX_TYPE = KEY_INT64 // should always be equal max key type
		};
		
		namespace EIndexCMDType
		{
			enum EIndexCMDType : u_int8_t
			{
				UNKNOWN = 0,
				PUT,
				TOUCH,
				DELETE,
			};
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
			virtual TItemSharedPtr find(const std::string &subLevel, const std::string &key, 
				const ItemHeader::TTime curTime) = 0;
			virtual void put(const std::string &subLevel, const std::string &key, TItemSharedPtr &item) = 0;
			virtual bool remove(const std::string &subLevel, const std::string &itemKey) = 0;
			virtual bool touch(const std::string &subLevel, const std::string &itemKey, 
				const ItemHeader::TTime setTime, const ItemHeader::TTime curTime) = 0;
			virtual void clearOld(const ItemHeader::TTime curTime) = 0;
			virtual bool sync(BString &buf, const ItemHeader::TTime curTime) = 0;
		protected:
			static void _formMetaFileName(const std::string &path, BString &metaFileName);
			static bool _createDataFile(const std::string &path, const u_int32_t curTime, const u_int32_t openNumber, 
				const MetaData &md, File &dataFile);
			static TopLevelIndex *_create(const std::string &path, MetaData &md);
		};
		typedef std::shared_ptr<TopLevelIndex> TTopLevelIndexPtr;
		

		class Index
		{
		public:
			Index(const std::string &path);
			~Index();
			
			bool create(const std::string &level, const EKeyType subLevelKeyType, const EKeyType itemKeyType);
			bool load();
			
			bool put(const std::string &level, const std::string &subLevel, const std::string &itemKey, 
				TItemSharedPtr &item);
			TItemSharedPtr find(const std::string &level, const std::string &subLevel, const std::string &itemKey, 
				const ItemHeader::TTime curTime);
			bool touch(const std::string &level, const std::string &subLevel, const std::string &itemKey, 
				const ItemHeader::TTime setTime, const ItemHeader::TTime curTime);
			bool remove(const std::string &level, const std::string &subLevel, const std::string &itemKey);
			
			
			void clearOld(const ItemHeader::TTime curTime);
			
			Index(const Index &) = delete;
			const size_t size() const
			{
				return _index.size();
			}
			bool sync(const ItemHeader::TTime curTime);
		private:
			bool _checkLevelName(const std::string &name);
			std::string _path;
			
			typedef std::string TTopLevelKey;
			typedef unordered_map<TTopLevelKey, TTopLevelIndexPtr> TTopLevelIndex;
			TTopLevelIndex _index;
			Mutex _sync;
		};
	};
};

#endif	// __FL_NOMOS_INDEX_HPP