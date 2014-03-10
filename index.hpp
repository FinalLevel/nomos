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

#include "config.h"
#ifdef HAVE_CXX11
	#include <unordered_map>
	using std::unordered_map;
	using std::unordered_multimap;
#else
	#include <boost/unordered_map.hpp>
	using boost::unordered_map;
	using boost::unordered_multimap;
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
#include "types.hpp"
#include "time_thread.hpp"
#include "read_write_lock.hpp"
#include "socket.hpp"


namespace fl {
	namespace nomos {
		using fl::threads::Mutex;
		using fl::threads::AutoMutex;
		using fl::threads::AutoReadWriteLockWrite;
		using fl::threads::AutoReadWriteLockRead;
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
				EKeyType subLevelKeyType;
				EKeyType itemKeyType;
			} __attribute__((packed));
			
			struct ReplicationPacketHeader
			{
				TServerID serverID; 
				MetaData md;
				uint32_t packetSize;
			} __attribute__((packed));

			TopLevelIndex(const std::string &level, class Index *index, const std::string &path, const MetaData &md);
			
			static TopLevelIndex *createFromDirectory(const std::string &level, class Index *index, const std::string &path);
			static TopLevelIndex *create(const std::string &level, Index *index, const std::string &path, 
				const EKeyType subLevelKeyType, const EKeyType itemKeyType);
			virtual bool load(Buffer &buf, const ItemHeader::TTime curTime) = 0;
			virtual TItemSharedPtr find(const std::string &subLevel, const std::string &key, 
				const ItemHeader::TTime curTime, const ItemHeader::TTime lifeTime, TTopLevelIndexPtr &selfPointer) = 0;
			virtual void put(const std::string &subLevel, const std::string &key, TItemSharedPtr &item, 
				bool checkBeforeReplace) = 0;
			virtual bool remove(const std::string &subLevel, const std::string &itemKey) = 0;
			virtual bool touch(const std::string &subLevel, const std::string &itemKey, 
				const ItemHeader::TTime setTime, const ItemHeader::TTime curTime) = 0;
			virtual void clearOld(const ItemHeader::TTime curTime) = 0;
			virtual bool sync(Buffer &buf, const ItemHeader::TTime curTime, bool force) = 0;
			virtual bool pack(Buffer &buf, const ItemHeader::TTime curTime) = 0;
			
			const MetaData &md() const
			{
				return _md;
			}
			virtual bool addFromAnotherServer(const TServerID serverID, Buffer &data, const Buffer::TSize endPacketPos, 
				const ItemHeader::TTime curTime, Buffer &buffer) = 0;
		protected:
			std::string _level;
			class Index *_index;
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

		class Index
		{
		public:
			Index(const std::string &path);
			~Index();
			void setAutoCreate(const bool ison, const EKeyType defaultSublevelType, const EKeyType defaultItemKeyType);
			void startThreads(const uint32_t syncThreadCount);
			bool hour(fl::chrono::ETime &curTime);
			
			bool create(const std::string &level, const EKeyType subLevelKeyType, const EKeyType itemKeyType);
			bool load(const ItemHeader::TTime curTime);
			
			static const bool CHECK_EXISTS = true;
			static const bool NOT_CHECK_EXISTS = false;
			bool put(const std::string &level, const std::string &subLevel, const std::string &itemKey, 
				TItemSharedPtr &item, bool checkBeforeReplace = NOT_CHECK_EXISTS);
			TItemSharedPtr find(const std::string &level, const std::string &subLevel, const std::string &itemKey, 
				const ItemHeader::TTime curTime, const ItemHeader::TTime lifeTime = 0);
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
			
			static EKeyType stringToType(const std::string &type);
			class ConvertError : public fl::exceptions::Error 
			{
			public:
				ConvertError(const char *what);
				ConvertError(ConvertError &&ce);
				virtual ~ConvertError() throw() {};
			private:
				BString _buf;
			};
			
			void addToSync(TTopLevelIndexPtr &topLevel);
			
			const TServerID serverID() const
			{
				return _serverID;
			}
			bool startReplicationLog(const TServerID serverID, const u_int32_t replicationLogKeepTime, 
				const std::string &replicationLogPath);
			bool startReplicationListenter(fl::network::Socket *listen);
			bool startReplication(TServerList &masters);
			
			void addToReplicationLog(Buffer &buffer);
			bool isReplicating()
			{
				return _replicationLogKeepTime > 0;
			}
			void exitFlush();
			bool getFromReplicationLog(const TServerID serverID, Buffer &data, Buffer &buffer, 
				TReplicationLogNumber &startNumber, uint32_t &seek); 
			bool addFromAnotherServer(const TServerID serverID, Buffer &data, const ItemHeader::TTime curTime, 
				Buffer &buffer);
			const std::string &replicationLogPath() const
			{
				return _replicationLogPath;
			}
			bool deleteOldReplicationLog(const time_t curTime);
			static const std::string REPLICATION_FILE_PREFIX;
			
			const size_t replicationLogFilesSize() const
			{
				return _replicationLogFiles.size();
			}
		private:
			Mutex _hourlySync;
			bool _checkLevelName(const std::string &name);
			TServerID _serverID;
			std::string _path;
			
			
			uint32_t _replicationLogKeepTime;
			std::string _replicationLogPath;
			class ReplicationLog
			{
			public:
				static const uint8_t CURRENT_VERSION = 1;
				
				ReplicationLog(const TReplicationLogNumber number, const char *fileName);
				bool openForRead();
				bool openForWrite();
				TReplicationLogNumber number() const
				{
					return _number;
				}
				const std::string &fileName() const
				{
					return _fileName;
				}
				uint32_t fileSize() const
				{
					return _fileSize;
				}
				const bool haveData(const uint32_t seek) const
				{
					if (seek < _fileSize)
						return true;
					else
						return false;
				}
				bool read(const TServerID serverID, Buffer &data, Buffer &buffer, uint32_t &seek);
				const bool canFit(const uint32_t size);
				void save(Buffer &buffer);
				
			private:
				bool _checkHeader(File &fd);
				TReplicationLogNumber _number;
				u_int8_t _version;
				std::string _fileName;
				File _readFd;
				uint32_t _fileSize;
				File _writeFd;
				fl::threads::ReadWriteLock _sync;
			};
			typedef std::shared_ptr<ReplicationLog>  TReplicationLogPtr;
			TReplicationLogPtr _currentReplicationLog;
			static bool _sortNumber(const TReplicationLogPtr &a, const TReplicationLogPtr &b);

			typedef std::vector<TReplicationLogPtr> TReplicationLogVector;
			TReplicationLogVector _replicationLogFiles;
			bool _openReplicationFiles();
			bool _openCurrentReplicationLog();
			Mutex _replicationSync;
			
			
			typedef uint8_t TStatus;
			TStatus _status;
			static const TStatus ST_AUTO_CREATE = 0x1;
			EKeyType _subLevelKeyType;
			EKeyType _itemKeyType;
			
			typedef std::string TTopLevelKey;
			typedef unordered_map<TTopLevelKey, TTopLevelIndexPtr> TTopLevelIndex;
			TTopLevelIndex _index;
			Mutex _sync;
			
			fl::threads::TimeThread *_timeThread;
			
			typedef std::vector<class IndexSyncThread*> TSyncThreadVector;
			TSyncThreadVector _syncThreads;
			
			class ReplicationAcceptThread *_replicationAcceptThread;
			typedef std::vector<class ReplicationActiveThread *> TReplicationActiveThreadVector;
			TReplicationActiveThreadVector _replicationThreads;
			
			void _stopThreads();
		};
	};
};

#endif	// __FL_NOMOS_INDEX_HPP
