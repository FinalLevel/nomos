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
	using boost::unordered_multimap;
#else
	#include <unordered_map>
	using std::unordered_map;
	using std::unordered_multimap;
#endif

#include <string>
#include <memory>
#include <vector>

#include "mutex.hpp"
#include "item.hpp"
#include "exception.hpp"
#include "bstring.hpp"
#include "buffer.hpp"
#include "file.hpp"


namespace fl {
	namespace nomos {
		using fl::threads::Mutex;
		using fl::threads::AutoMutex;
		using fl::strings::BString;
		using fl::fs::File;
		using fl::utils::Buffer;
	
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
				REMOVE,
			};
		};

		class TopLevelIndex
		{
		public:
			typedef uint8_t TVersion;
			static const TVersion CURRENT_VERSION = 1;
			static const std::string DATA_FILE_NAME;
			static const std::string HEADER_FILE_NAME;
			struct MetaData
			{
				bool operator !=(const MetaData &md) const
				{
					return (md.subLevelKeyType != subLevelKeyType) || (md.itemKeyType != itemKeyType);
				}
				uint8_t version;
				uint8_t subLevelKeyType;
				uint8_t itemKeyType;
			};
			TopLevelIndex(const std::string &path, const MetaData &md);
			
			static TopLevelIndex *createFromDirectory(const std::string &path);
			static TopLevelIndex *create(const std::string &path, const EKeyType subLevelKeyType, const EKeyType itemKeyType);
			virtual bool load(Buffer &buf, const ItemHeader::TTime curTime) = 0;
			virtual TItemSharedPtr find(const std::string &subLevel, const std::string &key, 
				const ItemHeader::TTime curTime) = 0;
			virtual void put(const std::string &subLevel, const std::string &key, TItemSharedPtr &item) = 0;
			virtual bool remove(const std::string &subLevel, const std::string &itemKey) = 0;
			virtual bool touch(const std::string &subLevel, const std::string &itemKey, 
				const ItemHeader::TTime setTime, const ItemHeader::TTime curTime) = 0;
			virtual void clearOld(const ItemHeader::TTime curTime) = 0;
			virtual bool sync(Buffer &buf, const ItemHeader::TTime curTime) = 0;
			virtual bool pack(Buffer &buf, const ItemHeader::TTime curTime) = 0;
		protected:
			std::string _path;
			MetaData _md;
			
			static void _formMetaFileName(const std::string &path, BString &metaFileName);
			static bool _createDataFile(const std::string &path, const u_int32_t curTime, const u_int32_t openNumber, 
				const MetaData &md, File &dataFile, BString &fileName, const char *prefix = NULL);
			static bool _createHeaderFile(const std::string &path, const u_int32_t curTime, const u_int32_t openNumber, 
				const MetaData &md, File &dataFile);
			static TopLevelIndex *_create(const std::string &path, MetaData &md);
			typedef std::vector<std::string> TPathVector;
			static bool _unlink(const TPathVector &fileList);
			static bool _loadFileList(const std::string &path, TPathVector &headersFileList, TPathVector &dataFileList);
			struct HeaderCMDData
			{
				ItemHeader::TTime liveTo;
				ItemHeader::UTag tag;
			};
			void _syncToFile(const Buffer::TSize startSavePos, const Buffer::TSize curPos, Buffer &buf, 
				Buffer &writeBuffer, File &packedFile, TPathVector &createdFiles, const u_int32_t curTime);
			void _syncToFile(const Buffer::TDataPtr data, const Buffer::TSize needToWrite, 
				File &packedFile, TPathVector &createdFiles, const u_int32_t curTime);
			void _renameTempToWork(TPathVector &fileList, const u_int32_t curTime);
			
			static const ItemHeader::TTime REMOVED_LIVE_TO = 1;
	
		};
		typedef std::shared_ptr<TopLevelIndex> TTopLevelIndexPtr;
		

		class Index
		{
		public:
			Index(const std::string &path);
			~Index();
			
			bool create(const std::string &level, const EKeyType subLevelKeyType, const EKeyType itemKeyType);
			bool load(const ItemHeader::TTime curTime);
			
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
			bool pack(const ItemHeader::TTime curTime);
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