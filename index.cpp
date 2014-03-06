///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Index maintenance classes
///////////////////////////////////////////////////////////////////////////////

#include "index.hpp"
#include "dir.hpp"
#include "nomos_log.hpp"
#include "file.hpp"
#include "util.hpp"
#include "index_sync_thread.hpp"


using namespace fl::nomos;
using fl::fs::Directory;
using fl::fs::File;
using namespace fl::utils;

TopLevelIndex::TopLevelIndex(const std::string &level, Index *index, const std::string &path, const MetaData &md)
	: _level(level), _index(index), _path(path), _md(md)
{
}

void TopLevelIndex::_formMetaFileName(const std::string &path, BString &metaFileName)
{
	metaFileName.sprintfSet("%s/.meta", path.c_str());
}

const std::string TopLevelIndex::DATA_FILE_NAME("data");


bool TopLevelIndex::_createDataFile(const std::string &path, const u_int32_t curTime, const u_int32_t openNumber, 
	const MetaData &md, File &file, BString &fileName, const char *prefix)
{
	fileName.sprintfSet("%s/%s%s_%u_%u", path.c_str(), prefix ? prefix : "", DATA_FILE_NAME.c_str(), curTime, openNumber);
	if (fileExists(fileName.c_str())) {
		log::Fatal::L("TopLevelIndex data file already exists %s\n", fileName.c_str());
		return false;
	}
	if (!file.open(fileName.c_str(), O_CREAT | O_WRONLY))
		return false;
	if (file.write(&md, sizeof(md)) == sizeof(md))
		return true;
	else
		return false;
}

const std::string TopLevelIndex::HEADER_FILE_NAME("header");

bool TopLevelIndex::_createHeaderFile(const std::string &path, const u_int32_t curTime, const u_int32_t openNumber, 
	const MetaData &md, File &file)
{
	BString fileName;
	fileName.sprintfSet("%s/%s_%u_%u", path.c_str(), HEADER_FILE_NAME.c_str(), curTime, openNumber);
	if (fileExists(fileName.c_str())) {
		log::Fatal::L("TopLevelIndex header file already exists %s\n", fileName.c_str());
		return false;
	}
	if (!file.open(fileName.c_str(), O_CREAT | O_WRONLY))
		return false;
	if (file.write(&md, sizeof(md)) == sizeof(md))
		return true;
	else
		return false;
}

void TopLevelIndex::_renameTempToWork(TPathVector &fileList, const u_int32_t curTime)
{
	BString toFileName;
	static u_int32_t opNumber = 0;
	for (auto file = fileList.begin(); file != fileList.end(); file++) {
		toFileName.sprintfSet("%s/%s_%u_%u_pack", _path.c_str(), DATA_FILE_NAME.c_str(), curTime, 
			__sync_add_and_fetch(&opNumber, 1));
		if (fileExists(toFileName.c_str())) {
			log::Fatal::L("TopLevelIndex packed data file already exists %s\n", toFileName.c_str());
			throw std::exception();
		}
		if (rename(file->c_str(), toFileName.c_str()))
		{
			log::Fatal::L("Can't rename packed data file from %s to %s\n", file->c_str(), toFileName.c_str());
			throw std::exception();
		}
	}
}

bool TopLevelIndex::_unlink(const TPathVector &fileList)
{
	bool res = true;
	for (auto file = fileList.begin(); file != fileList.end(); file++) {
		if (unlink(file->c_str()))
			res = false;
	}
	return res;
}

bool TopLevelIndex::_loadFileList(const std::string &path, TPathVector &headersFileList, TPathVector &dataFileList)
{
	try
	{
		Directory dir(path.c_str());
		BString fileName;
		while (dir.next()) {
			if (dir.name()[0] == '.') // skip not data files
				continue;
			fileName.sprintfSet("%s/%s", path.c_str(), dir.name());
			if (!strncmp(dir.name(), HEADER_FILE_NAME.c_str(), HEADER_FILE_NAME.size()))
				headersFileList.push_back(fileName.c_str());
			else if (!strncmp(dir.name(), DATA_FILE_NAME.c_str(), DATA_FILE_NAME.size()))
				dataFileList.push_back(fileName.c_str());
		}
		return true;
	}
	catch (Directory::Error &er)
	{
		log::Fatal::L("Can't open level %s\n", path.c_str());
		return false;		
	}
}

void TopLevelIndex::_syncToFile(const Buffer::TDataPtr data, const Buffer::TSize needToWrite, 
	File &packedFile, TPathVector &createdFiles, const u_int32_t curTime)
{
	if (packedFile.descr())
	{
		if ((packedFile.fileSize() + needToWrite) > MAX_FILE_SIZE)
			packedFile.close();
	}
	if (!packedFile.descr())
	{
		BString fileName;
		if (!_createDataFile(_path, curTime, createdFiles.size(), _md, packedFile, fileName, "."))
		{
			log::Error::L("Can't open file to sync %s\n", fileName.c_str());
			throw std::exception();
		}
		createdFiles.push_back(fileName.c_str());
	}
	if (packedFile.write(data, needToWrite) != (ssize_t)needToWrite)
	{
		log::Error::L("Can't write to file for sync %s\n", _path.c_str());
		throw std::exception();
	}
}

void TopLevelIndex::_syncToFile(const Buffer::TSize startSavePos, const Buffer::TSize curPos, Buffer &buf, 
	Buffer &writeBuffer, File &packedFile, TPathVector &createdFiles, const u_int32_t curTime)
{
	Buffer::TSize needToWrite = curPos - startSavePos;
	if (needToWrite == 0)
		return;
	if (needToWrite > (MAX_BUF_SIZE / 2)) { // write directly to file
		_syncToFile(buf.begin() + startSavePos, needToWrite, packedFile, createdFiles, curTime);
	} else { // simply add data to buffer
		writeBuffer.add(buf.begin() + startSavePos, needToWrite);
	}	
}



template <typename TSubLevelKey, typename TItemKey>
class MemmoryTopLevelIndex : public TopLevelIndex
{
public:
	typedef u_int16_t TSliceCount;
	static const TSliceCount ITEM_DEFAULT_SLICES_COUNT = 10;
	MemmoryTopLevelIndex(const std::string &level, Index *index, const std::string &path, const MetaData &md)
		: TopLevelIndex(level, index, path, md), _slicesCount(ITEM_DEFAULT_SLICES_COUNT)
	{

	}

	virtual ~MemmoryTopLevelIndex() {}

	virtual bool remove(const std::string &subLevelKeyStr, const std::string &key)
	{
		HeaderPacket headerPacket(_index->serverID());
		headerPacket.cmd = EIndexCMDType::REMOVE;
		headerPacket.subLevelKey = convertStdStringTo<TSubLevelKey>(subLevelKeyStr.c_str());
		headerPacket.itemKey = convertStdStringTo<TItemKey>(key.c_str());
		
		AutoMutex autoSync(&_sync);
		auto subLevel = _subLevelItem.find(headerPacket.subLevelKey);
		if (subLevel == _subLevelItem.end())
			return false;
		
		auto sliceID = _findSlice(headerPacket.itemKey);
		auto item = subLevel->second[sliceID].find(headerPacket.itemKey);
		if (item == subLevel->second[sliceID].end())
			return false;
		else {
			item->second->setDeleted();
			headerPacket.itemHeader = item->second->header();
			subLevel->second[sliceID].erase(item);	
			
			autoSync.unLock();
			_packetSync.lock();
			_headerPackets.push_back(headerPacket);
			_packetSync.unLock();
			return true;
		}
	}
	
	virtual TItemSharedPtr find(const std::string &subLevelKeyStr, const std::string &key, 
		const ItemHeader::TTime curTime, const ItemHeader::TTime lifeTime, TTopLevelIndexPtr &selfPointer) 
	{
		HeaderPacket headerPacket(_index->serverID());
		headerPacket.cmd = EIndexCMDType::TOUCH;
		headerPacket.subLevelKey = convertStdStringTo<TSubLevelKey>(subLevelKeyStr.c_str());
		headerPacket.itemKey = convertStdStringTo<TItemKey>(key.c_str());
		
		AutoMutex autoSync(&_sync);
		auto subLevel = _subLevelItem.find(headerPacket.subLevelKey);
		if (subLevel == _subLevelItem.end())
			return TItemSharedPtr();
		auto sliceID = _findSlice(headerPacket.itemKey);
		auto item = subLevel->second[sliceID].find(headerPacket.itemKey);
		if (item == subLevel->second[sliceID].end())
			return TItemSharedPtr();
		else if (item->second->isValid(curTime))
		{
			if (lifeTime) 
			{
				_touch(headerPacket, item->second, lifeTime, curTime);
				_index->addToSync(selfPointer);
			}
			return item->second;
		}
		else {
			subLevel->second[sliceID].erase(item);
			return TItemSharedPtr();
		}
	}
	
	virtual bool touch(const std::string &subLevelKeyStr, const std::string &key, 
		const ItemHeader::TTime setTime, const ItemHeader::TTime curTime)
	{
		HeaderPacket headerPacket(_index->serverID());
		headerPacket.cmd = EIndexCMDType::TOUCH;
		headerPacket.subLevelKey = convertStdStringTo<TSubLevelKey>(subLevelKeyStr.c_str());
		headerPacket.itemKey = convertStdStringTo<TItemKey>(key.c_str());
		
		
		AutoMutex autoSync(&_sync);
		auto subLevel = _subLevelItem.find(headerPacket.subLevelKey);
		if (subLevel == _subLevelItem.end())
			return false;
		auto sliceID = _findSlice(headerPacket.itemKey);
		auto item = subLevel->second[sliceID].find(headerPacket.itemKey);
		if (item == subLevel->second[sliceID].end())
			return false;
		else if (item->second->isValid(curTime)) {
			_touch(headerPacket, item->second, setTime, curTime);
			return true;
		}	else {
			subLevel->second[sliceID].erase(item);
			return false;
		}
	}
	
	virtual void put(const std::string &subLevel, const std::string &key, TItemSharedPtr &item, bool checkBeforeReplace)
	{
		DataPacket dataPacket(_index->serverID());
		dataPacket.subLevelKey = convertStdStringTo<TSubLevelKey>(subLevel);
		dataPacket.itemKey = convertStdStringTo<TItemKey>(key);
		dataPacket.item = item;
		
		TItemSharedPtr oldItem;
		AutoMutex autoSync(&_sync);
		bool changed = _put(dataPacket.subLevelKey, dataPacket.itemKey, item, oldItem, checkBeforeReplace);
		if (changed) {
			autoSync.unLock();
			_packetSync.lock();
			_dataPackets.push_back(dataPacket);
			if (oldItem.get() != NULL) {// mark old item as removed 
				HeaderPacket headerPacket(_index->serverID());
				headerPacket.cmd = EIndexCMDType::REMOVE;
				headerPacket.subLevelKey = dataPacket.subLevelKey;
				headerPacket.itemKey = dataPacket.itemKey;
				headerPacket.itemHeader = oldItem->header();
				_headerPackets.push_back(headerPacket);
			}
			_packetSync.unLock();
		} else {
			auto timeChange = abs(item->header().liveTo - oldItem->header().liveTo);
			if (timeChange > MIN_SYNC_PUT_UPDATE_TIME) {
				oldItem->setHeader(item->header());
				autoSync.unLock();
				
				HeaderPacket headerPacket(_index->serverID());
				headerPacket.cmd = EIndexCMDType::TOUCH;
				headerPacket.subLevelKey = dataPacket.subLevelKey;
				headerPacket.itemKey = dataPacket.itemKey;
				headerPacket.itemHeader = item->header();
				
				_packetSync.lock();
				_headerPackets.push_back(headerPacket);
				_packetSync.unLock();
			}
		}
	}
	
	virtual bool sync(Buffer &buf, const ItemHeader::TTime curTime, bool force)
	{
		AutoMutex autoSync;
		if (force) {
			autoSync.lock(&_diskLock);
		} else {
			if (!autoSync.tryLock(&_diskLock)) // some process already working with this level
				return false;
		}
		
		TDataPacketVector dataPackets;
		THeaderPacketVector headerPackets;
		
		_packetSync.lock();
		std::swap(dataPackets, _dataPackets);
		std::swap(headerPackets, _headerPackets);
		_packetSync.unLock();

		_syncPacketsToDisk(dataPackets, headerPackets, buf, curTime);
		if (force)
			_closeFiles();
		return true;
	}
	typedef unordered_multimap<TItemKey, HeaderCMDData> THeaderCMDDataHash;
	typedef unordered_map<TSubLevelKey, THeaderCMDDataHash> THeaderCMDIndexHash;
	
	virtual bool load(Buffer &buf, const ItemHeader::TTime curTime)
	{
		try
		{
			THeaderCMDIndexHash removeTouchIndex;
			Directory dir(_path.c_str());
			BString path;
			while (dir.next()) {
				if (dir.name()[0] == '.') // skip not data files
					continue;
				path.sprintfSet("%s/%s", _path.c_str(), dir.name());
				if (!strncmp(dir.name(), HEADER_FILE_NAME.c_str(), HEADER_FILE_NAME.size()))
				{
					if (!_loadHeaderData(path.c_str(), buf, curTime, removeTouchIndex))
						return false;
				}
			}
			dir.rewind();
			while (dir.next()) {
				if (dir.name()[0] == '.') // skip not data files
					continue;

				path.sprintfSet("%s/%s", _path.c_str(), dir.name());
				if (!strncmp(dir.name(), DATA_FILE_NAME.c_str(), DATA_FILE_NAME.size()))
				{
					if (!_loadData(path.c_str(), buf, curTime, removeTouchIndex))
						return false;
				}
			}
		}
		catch (Directory::Error &er)
		{
			log::Fatal::L("Can't open level %s\n", _path.c_str());
			return false;
		}
		return true;
	}
	
	
	
	virtual bool pack(Buffer &buf, const ItemHeader::TTime curTime)
	{
		AutoMutex autoSync(&_diskLock);
		_closeFiles();
		
		TPathVector headersFileList;
		TPathVector dataFileList;
		if (!_loadFileList(_path, headersFileList, dataFileList))
			return false;
		_diskLock.unLock();

		THeaderCMDIndexHash removeTouchIndex;
		for (auto headerFile = headersFileList.begin(); headerFile != headersFileList.end(); headerFile++) {
			if (!_loadHeaderData(headerFile->c_str(), buf, curTime, removeTouchIndex))
				return false;
		}
		
		bool result = true;
		TPathVector createdDataFiles;
		TPathVector packedFileList;
		Buffer writeBuffer(MAX_BUF_SIZE * 1.1);
		File packedFile;
		for (auto dataFile = dataFileList.begin(); dataFile != dataFileList.end(); dataFile++) {
			try
			{
				if (!_packDataFile(dataFile->c_str(), buf, writeBuffer, curTime, packedFile, removeTouchIndex,  
					createdDataFiles, packedFileList)) {
					result = false;
					break;
				}
			}
			catch (...)
			{
				log::Error::L("Caught exception while packing level %s\n", _path.c_str());
				result = false;
				break;
			}
		}
		packedFile.close();
		if (result) {
			try
			{
				_renameTempToWork(createdDataFiles, curTime);
			}
			catch (...)
			{
				_unlink(createdDataFiles);
				return false;
			}
			_unlink(headersFileList);
			_unlink(packedFileList);
			return true;
		}	else {
			_unlink(createdDataFiles);
			return false;
		}

	}
	
	virtual void clearOld(const ItemHeader::TTime curTime)
	{
		for (u_int32_t slice = 0; slice < _slicesCount; slice++)
		{
			AutoMutex autoSync(&_sync);
			for (auto subLevel = _subLevelItem.begin(); subLevel != _subLevelItem.end(); subLevel++) {
				for (auto item = subLevel->second[slice].begin(); item != subLevel->second[slice].end(); ) {
					if (item->second->isValid(curTime))
						item++;
					else
					{
						item = subLevel->second[slice].erase(item);
					}
				};
			};
		}
	}
	
	virtual bool addFromAnotherServer(const TServerID serverID, Buffer &data, const Buffer::TSize endPacketPos, 
		const ItemHeader::TTime curTime, Buffer &buffer)
	{
		ItemHeader itemHeader;
		TSubLevelKey subLevelKey;
		TItemKey itemKey;
		
		TDataPacketVector dataPackets;
		THeaderPacketVector headerPackets;
		
		AutoMutex autoSync(&_sync);
		while (data.readPos() < endPacketPos) {
			EIndexCMDType::EIndexCMDType cmd;
			_getEntryHeader(cmd, itemHeader, subLevelKey, itemKey, data);
			if (!itemHeader.liveTo || (itemHeader.liveTo > curTime) || (cmd == EIndexCMDType::REMOVE)) {
				auto subLevel = _subLevelItem.find(subLevelKey);
				if (subLevel != _subLevelItem.end()) {
					auto sliceID = _findSlice(itemKey);
					auto item = subLevel->second[sliceID].find(itemKey);
					if (item != subLevel->second[sliceID].end()) {
						if (cmd == EIndexCMDType::REMOVE) {
							if (itemHeader.timeTag.tag == item->second->header().timeTag.tag) {
									item->second->setDeleted();
									HeaderPacket hp(serverID);
									hp.cmd = EIndexCMDType::REMOVE;
									hp.subLevelKey = subLevelKey;
									hp.itemKey = itemKey;
									hp.itemHeader = item->second->header();
									subLevel->second[sliceID].erase(item);	
									headerPackets.push_back(hp);
							}
						}	else 	if (itemHeader.timeTag.tag > item->second->header().timeTag.tag) {
							if (cmd == EIndexCMDType::TOUCH) {
								item->second->setHeader(itemHeader);
								HeaderPacket hp(serverID);
								hp.cmd = EIndexCMDType::TOUCH;
								hp.subLevelKey = subLevelKey;
								hp.itemKey = itemKey;
								hp.itemHeader = itemHeader;
								hp.item = item->second;
								headerPackets.push_back(hp);
							} else if (cmd == EIndexCMDType::PUT) {
								HeaderPacket hp(serverID);
								hp.cmd = EIndexCMDType::REMOVE;
								hp.subLevelKey = subLevelKey;
								hp.itemKey = itemKey;
								hp.itemHeader = item->second->header();
								headerPackets.push_back(hp);

								item->second.reset(new Item((char*)data.mapBuffer(itemHeader.size), itemHeader));
								// remove old item
								
								DataPacket dataPacket(serverID);
								dataPacket.subLevelKey = subLevelKey;
								dataPacket.itemKey = itemKey;
								dataPacket.item = item->second;
								dataPackets.push_back(dataPacket);
								continue;
							}
							else
							{
								log::Fatal::L("Receive unknown cmd from server %u\n", serverID);
								return false;
							}
						}
						data.skip(itemHeader.size);
						continue;
					}
				}
				// item not found or should be updated
				switch (cmd)
				{
					case EIndexCMDType::REMOVE:
					break;
					case EIndexCMDType::TOUCH:
					case EIndexCMDType::PUT: 
					{
						TItemSharedPtr oldItem;
						TItemSharedPtr item(new Item((char*)data.mapBuffer(itemHeader.size), itemHeader));
						_put(subLevelKey, itemKey, item, oldItem, false);
						DataPacket dataPacket(serverID);
						dataPacket.subLevelKey = subLevelKey;
						dataPacket.itemKey = itemKey;
						dataPacket.item = item;
						dataPackets.push_back(dataPacket);
						continue;
					}
					case EIndexCMDType::UNKNOWN:
					{
						log::Fatal::L("Receive unknown cmd from server %u\n", serverID);
						return false;
					}
				}
			}
			data.skip(itemHeader.size);
		}
		autoSync.unLock();
		AutoMutex autoDiskLock(&_diskLock);
		_syncPacketsToDisk(dataPackets, headerPackets, buffer, curTime);
		return true;
	};
	
private:
	TSliceCount _findSlice(const TItemKey &itemKey)
	{
		return getCheckSum32Tmpl<TItemKey>(itemKey) % _slicesCount;
	}
	
	bool _put(const TSubLevelKey &subLevelKey, const TItemKey &itemKey, TItemSharedPtr &item, 
		TItemSharedPtr &oldItem, const bool checkBeforeReplace)
	{
		static TItemIndexVector startIndexSlices(_slicesCount);
		auto res = _subLevelItem.emplace(subLevelKey, startIndexSlices);
		auto sliceID = _findSlice(itemKey);
		auto itemRes = res.first->second[sliceID].emplace(itemKey, item);
		
		if (!itemRes.second) {
			oldItem = itemRes.first->second;
			if (checkBeforeReplace && itemRes.first->second->equal(item.get()))	{
				return false;
			}
			itemRes.first->second = item;
		}
		return true;
	}
	
	void _put(const TSubLevelKey &subLevelKey, const TItemKey &itemKey, const ItemHeader &itemHeader, const char *data)
	{
		static TItemIndexVector startIndexSlices(_slicesCount);
		static TItemSharedPtr empty;
		auto res = _subLevelItem.emplace(subLevelKey, startIndexSlices);
		auto sliceID = _findSlice(itemKey);
		auto itemRes = res.first->second[sliceID].emplace(itemKey, empty);
		if (!itemRes.second) {
			if (itemRes.first->second->header().timeTag.tag >= itemHeader.timeTag.tag) // skip old data
				return;
		}
		itemRes.first->second.reset(new Item(data, itemHeader));
	}

	struct DataPacket
	{
		DataPacket(const TServerID serverID)
			: serverID(serverID)
		{
		}
		TServerID serverID;
		TSubLevelKey subLevelKey;
		TItemKey itemKey;
		TItemSharedPtr item;
	};
	
	Mutex _packetSync;
	typedef std::vector<DataPacket> TDataPacketVector;
	TDataPacketVector _dataPackets;
	
	struct HeaderPacket
	{
		HeaderPacket(const TServerID serverID)
			: serverID(serverID)
		{
		}

		TServerID serverID;
		EIndexCMDType::EIndexCMDType cmd;
		TSubLevelKey subLevelKey;
		TItemKey itemKey;
		ItemHeader itemHeader;
		TItemSharedPtr item;
	};
	
	typedef std::vector<HeaderPacket> THeaderPacketVector;
	THeaderPacketVector _headerPackets;
	
	void _syncPacketsToDisk(TDataPacketVector &dataPackets, THeaderPacketVector &headerPackets, Buffer &buf, 
		const ItemHeader::TTime curTime)
	{
		if (!dataPackets.empty() || !headerPackets.empty()) {
			if (!_openFiles(curTime)) {
				log::Fatal::L("Can't open files for %s synchronization\n", _path.c_str());
				throw std::exception();
			}
			_syncDataPackets(dataPackets, buf, curTime);
			_syncHeaderPackets(headerPackets, buf, curTime);
		}
	}

	
	void _touch(HeaderPacket &headerPacket, TItemSharedPtr &item, const ItemHeader::TTime setTime, 
		const ItemHeader::TTime curTime)
	{
		ItemHeader::TTime liveTo = setTime;
		if (liveTo)
			liveTo += curTime;
		
		const ItemHeader &itemHeader = item->header();
		if (abs(liveTo - itemHeader.liveTo) > (setTime * MIN_SYNC_TOUCH_TIME_PERCENT))
		{
			item->setLiveTo(liveTo, curTime);
			headerPacket.itemHeader = itemHeader;
			if (_index->isReplicating())
				headerPacket.item = item;
			_packetSync.lock();
			_headerPackets.push_back(headerPacket);
			_packetSync.unLock();
		}
	}
	
	
	Mutex _diskLock;
	File _dataFile;
	File _headerFile;
	bool _openFiles(const u_int32_t curTime)
	{
		if (!_dataFile.descr()) // not open
		{
			static u_int32_t openNumber = 0;
			openNumber++;
			BString fileName;
			if (!_createDataFile(_path, curTime, openNumber, _md, _dataFile, fileName))
				return false;
		}
		if (!_headerFile.descr())
		{
			static u_int32_t openNumber = 0;
			openNumber++;
			if (!_createHeaderFile(_path, curTime, openNumber, _md, _headerFile))
				return false;
		}
		return true;
	}
	void _closeFiles()
	{
		_dataFile.close();
		_headerFile.close();
	}

	void _addReplicationPacketHeader(Buffer &buf)
	{
		buf.addSpace(sizeof(ReplicationPacketHeader));
		buf.add(_level);
	}
	
	void _saveReplicationPacket(Buffer &buf, const TServerID curServerID)
	{
		ReplicationPacketHeader &rph = (*(ReplicationPacketHeader*)buf.mapBuffer(sizeof(ReplicationPacketHeader)));
		rph.md = _md;
		rph.packetSize = buf.writtenSize() - sizeof(ReplicationPacketHeader);
		rph.serverID = curServerID;
		_index->addToReplicationLog(buf);
	}
	
	
	void _syncHeaderPackets(THeaderPacketVector &headerPackets, Buffer &buf, const ItemHeader::TTime curTime)
	{
		buf.clear();
		for (auto packet = headerPackets.begin(); packet != headerPackets.end(); ) {
			_addEntryHeader(packet->cmd, packet->itemHeader, packet->subLevelKey, packet->itemKey, buf);
			
			packet++;
			if ((buf.writtenSize() > MAX_BUF_SIZE) || (packet == headerPackets.end())) {
				if (!buf.empty()) {
					ssize_t needWrite = buf.writtenSize();
					if (_headerFile.write(buf.begin(), needWrite) != needWrite) { // skip 
						log::Fatal::L("Can't sync %s\n", _path.c_str());
						throw std::exception();
					};
					buf.clear();
				}
			}
		}
		
		if (_index->isReplicating()) {
			buf.clear();
			TServerID curServerID = 0;
			for (auto packet = headerPackets.begin(); packet != headerPackets.end(); ) {
				if (buf.empty()) {
					_addReplicationPacketHeader(buf);
					curServerID = packet->serverID;
				}
				if (packet->cmd == EIndexCMDType::REMOVE)
					packet->itemHeader.size = 0;
				
				_addEntryHeader(packet->cmd, packet->itemHeader, packet->subLevelKey, packet->itemKey, buf);
				if (packet->itemHeader.size > 0) // full data need
					buf.add(packet->item->data(), packet->itemHeader.size);
				
				packet++;
				if ((buf.writtenSize() > MAX_BUF_SIZE) || (packet == headerPackets.end()) ||
					(curServerID && (curServerID != packet->serverID))
				) {
					if (!buf.empty()) {
						_saveReplicationPacket(buf, curServerID);
						buf.clear();
					}
				}
			}
		}
		if (_headerFile.seek(0, SEEK_CUR) > static_cast<off_t>(MAX_FILE_SIZE))
			_headerFile.close();
	}
	
	bool _loadHeaderData(const char *path, Buffer &buf, const ItemHeader::TTime curTime, 
		THeaderCMDIndexHash &removeTouchIndex)
	{
		File fd;
		if (!fd.open(path, O_RDONLY))	{
			log::Error::L("Can't open level data file %s\n", path);
			return false;
		}
		buf.clear();
		ssize_t fileSize = fd.fileSize();
		if (fd.read(buf.reserveBuffer(fileSize), fileSize) != fileSize)	{
			log::Error::L("Can't read data file %s\n", path);
			return false;
		}
		
		try
		{
			MetaData md;
			buf.get(&md, sizeof(md));
			if ((md.itemKeyType != _md.itemKeyType) || (md.subLevelKeyType != _md.subLevelKeyType)) {
				log::Error::L("Sublevel/KeyType mismatch %s (%u/%u | %u/%u)\n", path, md.subLevelKeyType, _md.subLevelKeyType,
					md.subLevelKeyType, _md.subLevelKeyType);
				return false;

			}
			EIndexCMDType::EIndexCMDType cmd;
			ItemHeader itemHeader;
			TSubLevelKey subLevelKey;
			TItemKey itemKey;
			HeaderCMDData headerCMDData;
			static THeaderCMDDataHash emptySubLevelIndex;
			while (!buf.isEnded()) {
				_getEntryHeader(cmd, itemHeader, subLevelKey, itemKey, buf);
				if ((cmd != EIndexCMDType::TOUCH) && (cmd != EIndexCMDType::REMOVE)) {
					log::Error::L("Bad cmd type %u in header\n", cmd);
					return false;
				}
				if (cmd == EIndexCMDType::REMOVE)
					headerCMDData.liveTo = REMOVED_LIVE_TO;
				else
					headerCMDData.liveTo = itemHeader.liveTo;
				headerCMDData.tag = itemHeader.timeTag;
				auto subLevelRes = removeTouchIndex.emplace(subLevelKey, emptySubLevelIndex);
				auto itemRes = subLevelRes.first->second.equal_range(itemKey);
				
				bool needAdd = true;
				if (cmd == EIndexCMDType::REMOVE)	{
					for (auto it = itemRes.first; it != itemRes.second; ++it) {
						if (it->second.liveTo == REMOVED_LIVE_TO) // skipDelete
							continue;
						if (it->second.tag.tag <= headerCMDData.tag.tag) {
							it->second = headerCMDData;
							needAdd = false;
						}
					}
				}
				else
				{
					for (auto it = itemRes.first; it != itemRes.second; ++it) {
						if (it->second.liveTo == REMOVED_LIVE_TO) // check if a deleted is older than current TOUCH skip touch
						{
							if (it->second.tag.tag > headerCMDData.tag.tag)
							{
								needAdd = false;
								break;
							}
							continue;
						}
						if (it->second.tag.tag <= headerCMDData.tag.tag) {
							it->second = headerCMDData;
							needAdd = false;
						}
					}
				}
				if (needAdd)
					subLevelRes.first->second.emplace(itemKey, headerCMDData);
			}

		}
		catch (Buffer::Error &er)
		{
			log::Fatal::L("Catch error in data file %s\n", path);
			return false;
		}
		return true;
	}

	// two mirror functions
	void _addEntryHeader(EIndexCMDType::EIndexCMDType cmd, const ItemHeader &itemHeader, const TSubLevelKey &subLevelKey, 
		const TItemKey &itemKey, Buffer &buf)
	{
		buf.add(cmd);
		buf.add(&itemHeader, sizeof(itemHeader));
		buf.add(subLevelKey);
		buf.add(itemKey);
	}
	
	void _getEntryHeader(EIndexCMDType::EIndexCMDType &cmd, ItemHeader &itemHeader, TSubLevelKey &subLevelKey, 
		TItemKey &itemKey, Buffer &buf)
	{
		buf.get(cmd);
		buf.get(&itemHeader, sizeof(itemHeader));
		buf.get(subLevelKey);
		buf.get(itemKey);
	}

	bool _loadDataEntryHeader(Buffer &buf, ItemHeader &itemHeader, TSubLevelKey &subLevelKey, TItemKey &itemKey, 
		THeaderCMDIndexHash &removeTouchIndex)
	{
		EIndexCMDType::EIndexCMDType cmd;
		_getEntryHeader(cmd, itemHeader, subLevelKey, itemKey, buf);
		if (cmd != EIndexCMDType::PUT) {
			log::Error::L("Bad cmd type %u in data\n", cmd);
			throw std::exception();
		}
		auto f = removeTouchIndex.find(subLevelKey);
		if (f != removeTouchIndex.end()) {
			auto itemRes = f->second.equal_range(itemKey);
			for (auto it = itemRes.first; it != itemRes.second; ++it)
			{			
				if (it->second.tag.tag >= itemHeader.timeTag.tag) {
					itemHeader.liveTo = it->second.liveTo;
					itemHeader.timeTag.tag = it->second.tag.tag;
					return true; // changed
				}
			}
		}
		return false;  // not changed
	}
	
	void _syncDataPackets(TDataPacketVector &workPackets, Buffer &buf, const ItemHeader::TTime curTime)
	{
		Buffer::TSize replicationHeaderEnd = 0;
		TServerID curServerID = 0;
		buf.clear();
		for (auto dataPacket = workPackets.begin(); dataPacket != workPackets.end(); ) {
			if (dataPacket->item->isValid(curTime)) {
				if (_index->isReplicating()) {
					if (buf.empty())	{
						_addReplicationPacketHeader(buf);
						replicationHeaderEnd = buf.writtenSize();
						curServerID = dataPacket->serverID;
					}
				}
				const ItemHeader &itemHeader = dataPacket->item->header();
				_addEntryHeader(EIndexCMDType::PUT, itemHeader, dataPacket->subLevelKey, dataPacket->itemKey, buf);
				buf.add(dataPacket->item->data(), itemHeader.size);
			}
			dataPacket++;
			if ((buf.writtenSize() > MAX_BUF_SIZE) || (dataPacket == workPackets.end()) ||
				(curServerID && (curServerID != dataPacket->serverID))
			) {
				if (!buf.empty()) {
					ssize_t needWrite = buf.writtenSize() - replicationHeaderEnd;
					if (_dataFile.write(buf.begin() + replicationHeaderEnd,  needWrite) != needWrite) { // skip 
						log::Fatal::L("Can't sync %s\n", _path.c_str());
						throw std::exception();
					};
					if (replicationHeaderEnd > 0)
						_saveReplicationPacket(buf, curServerID);
					buf.clear();
				}
			}
		}
		if (_dataFile.seek(0, SEEK_CUR) > static_cast<off_t>(MAX_FILE_SIZE))
			_dataFile.close();
	}
	
	bool _loadData(const char *path, Buffer &buf, const ItemHeader::TTime curTime, THeaderCMDIndexHash &removeTouchIndex)
	{
		File fd;
		if (!fd.open(path, O_RDONLY))	{
			log::Error::L("Can't open level data file %s\n", path);
			return false;
		}
		buf.clear();
		ssize_t fileSize = fd.fileSize();
		if (fd.read(buf.reserveBuffer(fileSize), fileSize) != fileSize)	{
			log::Error::L("Can't read data file %s\n", path);
			return false;
		}
		
		try
		{
			MetaData md;
			buf.get(&md, sizeof(md));
			if (md != _md) {
				log::Error::L("Sublevel/KeyType mismatch %s (%u/%u | %u/%u)\n", path, md.subLevelKeyType, _md.subLevelKeyType,
					md.subLevelKeyType, _md.subLevelKeyType);
				return false;
			}
			ItemHeader itemHeader;
			TSubLevelKey subLevelKey;
			TItemKey itemKey;
			while (!buf.isEnded()) {
				_loadDataEntryHeader(buf, itemHeader, subLevelKey, itemKey, removeTouchIndex);
				if (!itemHeader.liveTo || (itemHeader.liveTo > curTime))
				{
					_put(subLevelKey, itemKey, itemHeader, (char*)buf.mapBuffer(itemHeader.size));
				}
				else
					buf.skip(itemHeader.size);
			}

		}
		catch (Buffer::Error &er)
		{
			log::Fatal::L("Catch error in data file %s\n", path);
			return false;
		}
		catch (std::exception &er)
		{
			log::Fatal::L("Catch std::exception in data file %s\n", path);
			return false;
		}
		return true;
	}

	bool _packDataFile(const char *path, Buffer &buf, Buffer &writeBuffer, const ItemHeader::TTime curTime,
		File &packedFile, THeaderCMDIndexHash &removeTouchIndex, TPathVector &createdFiles, TPathVector &packedFileList)
	{
		File fd;
		if (!fd.open(path, O_RDONLY))	{
			log::Error::L("Can't open level data file %s\n", path);
			return false;
		}
		MetaData md;
		if (fd.read(&md, sizeof(md)) != sizeof(md))
			return false;
		if (_md != md) {
			log::Error::L("Sublevel/KeyType mismatch %s (%u/%u | %u/%u)\n", path, md.subLevelKeyType, _md.subLevelKeyType,
				md.subLevelKeyType, _md.subLevelKeyType);
		}
			
		buf.clear();
		writeBuffer.clear();
		ssize_t fileSize = fd.fileSize() - sizeof(md);
		buf.add<u_int8_t>(1); // add fake byte to move buf start to 1 from zero
		if (fd.read(buf.reserveBuffer(fileSize), fileSize) != fileSize)	{
			log::Error::L("Can't read data file %s\n", path);
			return false;
		}
		buf.skip(sizeof(u_int8_t));

		try
		{
			ItemHeader itemHeader;
			TSubLevelKey subLevelKey;
			TItemKey itemKey;
			Buffer::TSize startSavePos = 0;
			Buffer::TSize curPos = 0;
			while (!buf.isEnded()) {
				curPos = buf.readPos();
				bool changed = _loadDataEntryHeader(buf, itemHeader, subLevelKey, itemKey, removeTouchIndex);
				if (!itemHeader.liveTo || (itemHeader.liveTo > curTime)) {
					if (changed) { // need to rewrite
						_addEntryHeader(EIndexCMDType::PUT, itemHeader, subLevelKey, itemKey, writeBuffer);
						writeBuffer.add(buf.mapBuffer(itemHeader.size), itemHeader.size);
						if (startSavePos)
						{
							_syncToFile(startSavePos, curPos, buf, writeBuffer, packedFile, createdFiles, curTime);
							startSavePos = 0;
						}
					} else 
					{
						if (!startSavePos)
							startSavePos = curPos;
						buf.skip(itemHeader.size);
					}
				}
				else
				{
					if (startSavePos)
					{
						_syncToFile(startSavePos, curPos, buf, writeBuffer, packedFile, createdFiles, curTime);
						startSavePos = 0;
					}
					buf.skip(itemHeader.size);
				}
				if (writeBuffer.writtenSize() > MAX_BUF_SIZE)
				{
					_syncToFile(writeBuffer.begin(), writeBuffer.writtenSize(), packedFile, createdFiles, curTime);
					writeBuffer.clear();
				}
			}
			if (startSavePos == 1) // the whole file is not changed
				return true;
			else if (startSavePos)
				_syncToFile(startSavePos, buf.readPos(), buf, writeBuffer, packedFile, createdFiles, curTime);
			
			if (!writeBuffer.empty())
				_syncToFile(writeBuffer.begin(), writeBuffer.writtenSize(), packedFile, createdFiles, curTime);
			
			packedFileList.push_back(path);
			return true;
		}
		catch (Buffer::Error &er)
		{
			log::Fatal::L("Catch error in data file %s\n", path);
			return false;
		}
		return false;
	}
	
	typedef unordered_map<TItemKey, TItemSharedPtr> TItemIndex;
	typedef std::vector<TItemIndex> TItemIndexVector;
	typedef unordered_map<TSubLevelKey, TItemIndexVector> TSubLevelIndex;
	TSubLevelIndex _subLevelItem;
	Mutex _sync;
	TSliceCount _slicesCount;
};

template <EKeyType type>
struct TKeyType  {	};

static_assert(KEY_MAX_TYPE == KEY_INT64, "New key type should be added below");

template <>
struct TKeyType<KEY_STRING>  
{	
	typedef std::string type;
};
template <>
struct TKeyType<KEY_INT32>  
{	
	typedef u_int32_t type;
};
template <>
struct TKeyType<KEY_INT64>  
{	
	typedef u_int64_t type;
};

template <template<typename TSubLevelKey, typename TItemKey> class TIndexClass, typename TSubLevelKey>
TopLevelIndex *createTopLevelIndex(const EKeyType itemKeyType, const std::string &level, Index *index, 
	const std::string &path, const TopLevelIndex::MetaData &md
)
{
	switch (itemKeyType) 
	{
	case KEY_STRING:
		return new TIndexClass<TSubLevelKey, TKeyType<KEY_STRING>::type>(level, index, path, md);
	case KEY_INT32:
		return new TIndexClass<TSubLevelKey, TKeyType<KEY_INT32>::type>(level, index, path, md);
	case KEY_INT64:
		return new TIndexClass<TSubLevelKey, TKeyType<KEY_INT64>::type>(level, index, path, md);
	};
	return  NULL;
}
template <template<typename TSubLevelKey, typename TItemKey> class TIndexClass>
TopLevelIndex *createTopLevelIndex(
	const EKeyType subLevelType, 
	const EKeyType itemKeyType, 
	const std::string &level,
	Index *index,
	const std::string &path, 
	const TopLevelIndex::MetaData &md
)
{
	switch (subLevelType)
	{
		case KEY_STRING:
			return createTopLevelIndex<TIndexClass, TKeyType<KEY_STRING>::type>(itemKeyType, level, index, path, md);
		case KEY_INT32:
			return createTopLevelIndex<TIndexClass, TKeyType<KEY_INT32>::type>(itemKeyType, level, index, path, md);
		case KEY_INT64:
			return createTopLevelIndex<TIndexClass, TKeyType<KEY_INT64>::type>(itemKeyType, level, index, path, md);
	};
	return  NULL;
}

TopLevelIndex *TopLevelIndex::createFromDirectory(const std::string &level, Index *index, const std::string &path)
{
	BString metFileName;
	_formMetaFileName(path, metFileName);
	File fd;
	if (!fd.open(metFileName.c_str(), O_RDONLY)) {
		log::Error::L("Cannot open metadata file %s\n", metFileName.c_str());
		return NULL;
	}
	MetaData md;
	if (fd.read(&md, sizeof(md)) != sizeof(md))	{
		log::Error::L("Cannot read from metadata file %s\n", metFileName.c_str());
		return NULL;
	}
	return createTopLevelIndex<MemmoryTopLevelIndex>(static_cast<EKeyType>(md.subLevelKeyType), \
		static_cast<EKeyType>(md.itemKeyType), level, index, path, md);
}

TopLevelIndex *TopLevelIndex::create(
	const std::string &level,
	Index *index,
	const std::string &path, 
	const EKeyType subLevelKeyType, 
	const EKeyType itemKeyType
)
{
	if (!Directory::makeDirRecursive(path.c_str()))
		return NULL;
	MetaData md;
	bzero(&md, sizeof(md));
	md.version = CURRENT_VERSION;
	md.subLevelKeyType = subLevelKeyType;
	md.itemKeyType = itemKeyType;

	BString metFileName;
	_formMetaFileName(path, metFileName);
	File fd;
	if (!fd.open(metFileName.c_str(), O_WRONLY | O_CREAT)) {
		log::Error::L("Cannot create metadata file %s\n", metFileName.c_str());
		return NULL;
	}
	if (fd.write(&md, sizeof(md)) != sizeof(md)) {
		log::Error::L("Cannot write to metadata file %s\n", metFileName.c_str());
		return NULL;
	}
	
	return createTopLevelIndex<MemmoryTopLevelIndex>(subLevelKeyType, itemKeyType, level, index, path, md);
}

Index::Index(const std::string &path)
	: _serverID(0), _path(path), _replicationLogKeepTime(0), _status(0),
	_subLevelKeyType(KEY_INT32), _itemKeyType(KEY_INT64), _timeThread(5 * 60) // minutes tic time
{
	Directory dir(path.c_str());
	BString topLevelPath;
	while (dir.next()) {
		if (!dir.isDirectory()) // skip files
			continue;
		if (dir.name()[0] == '.' && (dir.name()[1] == '.' || dir.name()[1] == 0)) // skip
			continue;
		std::string levelName(dir.name());
		topLevelPath.sprintfSet("%s/%s", path.c_str(), dir.name());
		TopLevelIndex *topLevelIndex = TopLevelIndex::createFromDirectory(levelName, this, topLevelPath.c_str());
		if (!topLevelIndex) {
			log::Error::L("Cannot load TopLevelIndex %s\n", topLevelPath.c_str());
			continue;
		}
		_index.emplace(levelName, TTopLevelIndexPtr(topLevelIndex));
	}
	log::Info::L("Loaded %u top indexes\n", _index.size());
}

Index::~Index()
{
}

void Index::setAutoCreate(const bool ison, const EKeyType defaultSublevelType, const EKeyType defaultItemKeyType)
{
	if (ison)
		_status |= ST_AUTO_CREATE;
	else
		_status &= (~ST_AUTO_CREATE);
	_subLevelKeyType = defaultSublevelType;
	_itemKeyType = defaultItemKeyType;
}

bool Index::_checkLevelName(const std::string &name)
{
	if (name.size() > MAX_TOP_LEVEL_NAME_LENGTH)
		return false;
	
	for (std::string::const_iterator c = name.begin(); c != name.end(); c++) {
		auto ch = (*c);
		if (!isxdigit(ch) && (ch != '_') && (ch != '-') && (ch != '.')) {
			if (((ch >= 'a') && (ch <= 'z')) || ((ch >= 'A') && (ch <= 'Z')))
				continue;
			else
				return false;
		}
	}
	return true;
}

bool Index::pack(const ItemHeader::TTime curTime)
{
	AutoMutex autoSync(&_sync);
	auto indexCopy = _index;
	_sync.unLock();
	
	Buffer buf;
	for (auto topLevel = indexCopy.begin(); topLevel != indexCopy.end(); topLevel++) {
		if (!topLevel->second->pack(buf, curTime))
			return false;
	}
	return true;
}

bool Index::sync(const ItemHeader::TTime curTime)
{
	AutoMutex autoSync(&_sync);
	auto indexCopy = _index;
	_sync.unLock();
	
	Buffer buf(MAX_BUF_SIZE * 1.1); // to prevent resizing
	for (auto topLevel = indexCopy.begin(); topLevel != indexCopy.end(); topLevel++) {
		if (!topLevel->second->sync(buf, curTime, false))
			return false;
	}
	return true;
}

void Index::clearOld(const ItemHeader::TTime curTime)
{
	AutoMutex autoSync(&_sync);
	auto indexCopy = _index;
	_sync.unLock();
	for (auto topLevel = indexCopy.begin(); topLevel != indexCopy.end(); topLevel++)
		topLevel->second->clearOld(curTime);
}

bool Index::put(const std::string &level, const std::string &subLevel, const std::string &itemKey, 
	TItemSharedPtr &item, bool checkBeforeReplace)
{
	AutoMutex autoSync(&_sync);
	auto f = _index.find(level);
	if (f == _index.end()) {
		if (_status & ST_AUTO_CREATE)	{
			autoSync.unLock();
			if (create(level, _subLevelKeyType, _itemKeyType)) {
				return put(level, subLevel, itemKey, item, checkBeforeReplace);
			}
			else
				return false;
		}
		else
			return false;
	}
	auto topLevel = f->second;
	autoSync.unLock();
	topLevel->put(subLevel, itemKey, item, checkBeforeReplace);
	addToSync(topLevel);
	return true;
}

bool Index::remove(const std::string &level, const std::string &subLevel, const std::string &itemKey)
{
	AutoMutex autoSync(&_sync);
	auto f = _index.find(level);
	if (f == _index.end())
		return false;
	auto topLevel = f->second;
	autoSync.unLock();
	if (topLevel->remove(subLevel, itemKey))
	{
		addToSync(topLevel);
		return true;
	}
	else
		return false;
}

TItemSharedPtr Index::find(const std::string &level, const std::string &subLevel, const std::string &itemKey, 
	const ItemHeader::TTime curTime, const ItemHeader::TTime lifeTime)
{
	AutoMutex autoSync(&_sync);
	auto f = _index.find(level);
	if (f == _index.end())
		return TItemSharedPtr();
	auto topLevel = f->second;
	autoSync.unLock();
	return topLevel->find(subLevel, itemKey, curTime, lifeTime, topLevel);
}

bool Index::touch(const std::string &level, const std::string &subLevel, const std::string &itemKey, 
	const ItemHeader::TTime setTime, const ItemHeader::TTime curTime)
{
	AutoMutex autoSync(&_sync);
	auto f = _index.find(level);
	if (f == _index.end())
		return false;
	auto topLevel = f->second;
	autoSync.unLock();
	if (topLevel->touch(subLevel, itemKey, setTime, curTime))
	{
		addToSync(topLevel);
		return true;
	}
	else
		return false;
}

bool Index::load(const ItemHeader::TTime curTime)
{
	Buffer buf;
	for (auto topLevel = _index.begin(); topLevel != _index.end(); topLevel++)
	{
		if (!topLevel->second->load(buf, curTime))
			return false;
	}
	return true;
}

Index::ConvertError::ConvertError(const char *what)
	: fl::exceptions::Error(_buf.c_str())
{
	_buf.sprintfSet("Unknown index type %s\n", what);
	_what = _buf.c_str();
}

Index::ConvertError::ConvertError(ConvertError &&ce)
	: Error(NULL), _buf(std::move(ce._buf))
{
	_what = _buf.c_str();
}


EKeyType Index::stringToType(const std::string &type)
{
	static const char * KEY_TYPES[KEY_MAX_TYPE + 1] = {
		"STRING",
		"INT32",
		"INT64",
	};
	for (int i = 0; i <= KEY_MAX_TYPE; i++) {
		if (!strcasecmp(type.c_str(), KEY_TYPES[i])) {
			return (EKeyType)i;
		}
	}
	log::Error::L("Cannot find type %s\n", type.c_str());
	throw ConvertError(type.c_str());
}

bool Index::create(const std::string &level, const EKeyType subLevelKeyType, const EKeyType itemKeyType)
{
	if (!_checkLevelName(level)) {
		log::Error::L("Cannot create new top level %s (it must contain only A-Za-z0-9_-. chars)\n", level.c_str());
		return false;
	}

	AutoMutex autoSync(&_sync);
	if (_index.find(level) != _index.end())	{
		log::Error::L("Cannot create new top level %s already exists \n", level.c_str());
		return false;
	}
	BString path;
	path.sprintfSet("%s/%s", _path.c_str(), level.c_str());
	if (!Directory::makeDirRecursive(path.c_str()))	{
		log::Error::L("Cannot create directory for a new top level %s\n", path.c_str());
		return false;
	}
	TopLevelIndex *topLevelIndex = TopLevelIndex::create(level, this, path.c_str(), subLevelKeyType, itemKeyType);
	if (!topLevelIndex)	{
		log::Error::L("Cannot create new TopLevelIndex %s\n", path.c_str());
		return false;
	}
	_index.emplace(level, TTopLevelIndexPtr(topLevelIndex));
	return true;
}

Mutex Index::_hourlySync;

bool Index::hour(fl::chrono::ETime &curTime)
{
	AutoMutex autoSync(&_hourlySync);
	log::Info::L("Hourly routines started\n");
	clearOld(curTime.unix());
	pack(curTime.unix());
	log::Info::L("Hourly routines ended\n");
	return true;
}

void Index::startThreads(const uint32_t syncThreadCount)
{
	_timeThread.addEveryHour(new fl::threads::TimeTask<Index>(this, &Index::hour));
	if (!_timeThread.create())
	{
		log::Fatal::L("Can't create a time thread\n");
		throw std::exception();
	}
	
	for (uint32_t i = 0; i < syncThreadCount; i++)
	{
		_syncThreads.push_back(new IndexSyncThread());
	}
	log::Info::L("%u sync threads have been started\n", _syncThreads.size());
}

void Index::addToSync(TTopLevelIndexPtr &topLevel)
{
	if (_syncThreads.empty())
		return;
	
	static int num = 0;
	num++;
	_syncThreads[num % _syncThreads.size()]->add(topLevel);
}

bool Index::startReplicationLog(const TServerID serverID, const u_int32_t replicationLogKeepTime, 
	const std::string &replicationLogPath)
{
	if (!replicationLogKeepTime) // replication is turned off
		return true;
	
	_serverID = serverID;
	_replicationLogPath = replicationLogPath;
	_replicationLogKeepTime = replicationLogKeepTime;
	
	if (!_openReplicationFiles())
		return false;
	return _openCurrentReplicationLog();
}

const std::string Index::REPLICATION_FILE_PREFIX = "nomos_bin_";

Index::ReplicationLog::ReplicationLog(const TReplicationLogNumber number, const char *fileName)
	: _number(number), _version(CURRENT_VERSION), _fileName(fileName), _fileSize(0)
{
}

bool Index::_sortNumber(const TReplicationLogPtr &a, const TReplicationLogPtr &b)
{
	return a->number() < b->number();
}

bool Index::ReplicationLog::openForRead()
{
	if (!_readFd.open(_fileName.c_str(), O_RDONLY))
		return false;
	if (!_checkHeader(_readFd))
		return false;
	_fileSize = _readFd.fileSize();
	return true;
}

bool Index::ReplicationLog::_checkHeader(File &fd)
{
	if (!fd.read(&_version, sizeof(_version)))
	{
		log::Error::L("Can't read replication log version %s\n", _fileName.c_str());
		return false;
	}
	TReplicationLogNumber number;
	if (!fd.read(&number, sizeof(number)))
	{
		log::Error::L("Can't read replication log number %s\n", _fileName.c_str());
		return false;
	}
	if (number != _number)
	{
		log::Error::L("Can't read replication log number %x is different than %x (%s)\n", number, _number, 
			_fileName.c_str());
		return false;
	}
	return true;
}


bool Index::ReplicationLog::openForWrite()
{
	if (!_writeFd.open(_fileName.c_str(), O_RDWR | O_CREAT))
		return false;
	if (_writeFd.fileSize()) {
		if (!_checkHeader(_writeFd))
			return false;
	} else { // need write header
		if (!_writeFd.write(&_version, sizeof(_version))) {
			log::Error::L("Can't write replication log version %s\n", _fileName.c_str());
			return false;
		}
		if (!_writeFd.write(&_number, sizeof(_number))) {
			log::Error::L("Can't write replication log number %s\n", _fileName.c_str());
			return false;
		}
	}
	_fileSize = _writeFd.seek(0, SEEK_END);
	return true;
}

const bool Index::ReplicationLog::canFit(const uint32_t size)
{
	if (_fileSize + size > MAX_REPLICATION_FILE_SIZE)
		return false;
	else
		return true;
}

void Index::ReplicationLog::save(Buffer &buffer)
{
	AutoReadWriteLockWrite autoWriteLock(&_sync);
	
	if (_writeFd.write(buffer.begin(), buffer.writtenSize()) != (ssize_t)buffer.writtenSize()) {
		log::Error::L("Can't write data to replication log %s\n", _fileName.c_str());
		throw std::exception();
	}
	_fileSize += buffer.writtenSize();
}

bool Index::ReplicationLog::read(const TServerID serverID, BString &data, Buffer &buffer, uint32_t &seek)
{
	if (seek == 0)
		seek += (sizeof(_version) + sizeof(_number));
	buffer.clear();
	
	AutoReadWriteLockRead autoReadLock(&_sync);
	ssize_t leftRead = _fileSize - seek;
	while (leftRead > 0)
	{
		ssize_t readChunk = MAX_REPLICATION_BUFFER - 1;
		if (readChunk > leftRead)
			readChunk = leftRead;
		if (_readFd.pread(buffer.reserveBuffer(readChunk), readChunk, seek) != readChunk)
			return false;
		seek += readChunk;
		leftRead -= readChunk;
		
		size_t lastOkBlock = 0;
		try
		{
			while (buffer.readPos() < buffer.writtenSize())
			{
				lastOkBlock = buffer.readPos();
				TopLevelIndex::ReplicationPacketHeader &rph = 
					*(TopLevelIndex::ReplicationPacketHeader*)buffer.mapBuffer(sizeof(TopLevelIndex::ReplicationPacketHeader));
				buffer.skip(rph.packetSize); // check if the packet has read fully 
				if (rph.serverID == serverID)
					continue;
				else {
					buffer.seekReadPos(lastOkBlock);
					auto addData = sizeof(TopLevelIndex::ReplicationPacketHeader) + rph.packetSize;
					data.add((char*)buffer.mapBuffer(addData), addData);
					if (data.size() > (BString::TSize)MAX_BUF_SIZE)
					{
						seek += buffer.readPos();
						return true;
					}
				}
			}
			seek += buffer.readPos();
			leftRead -= buffer.readPos();
		}
		catch (Buffer::Error &er)
		{
			if (leftRead == 0) {// all read and receive error
				log::Error::L("Catch Buffer exception %s lastSeek %u\n", _fileName.c_str(), seek);
				return false;
			}
			if (lastOkBlock == 0) {
				log::Error::L("Catch Buffer exception in first block %s lastSeek %u\n", _fileName.c_str(), seek);
				return false;
			}
			seek += lastOkBlock;
			leftRead -= lastOkBlock;
		}
		buffer.clear();
	}
	return true;
}

bool Index::_openCurrentReplicationLog()
{
	if (_currentReplicationLog.get())
	{
		if (!_currentReplicationLog->openForWrite())
			return false;
		log::Info::L("Open bin log %s for writing, current position is %u\n", _currentReplicationLog->fileName().c_str(), 
			_currentReplicationLog->fileSize());
		return true;
	}
	TReplicationLogNumber number = 1;
	if (!_replicationLogFiles.empty())
		number = _replicationLogFiles.back()->number() + 1;
	
	BString fileName;
	fileName.sprintfSet("%s/%s%u_%+08x", _replicationLogPath.c_str(), REPLICATION_FILE_PREFIX.c_str(), _serverID, number);
	TReplicationLogPtr rl(new ReplicationLog(number, fileName.c_str()));
	if (!rl->openForWrite())
		return false;
	if (!rl->openForRead())
		return false;
	log::Info::L("Create new bin log %s\n", fileName.c_str());
	_currentReplicationLog = rl;
	_replicationLogFiles.push_back(rl);
	return true;
}

bool Index::_openReplicationFiles()
{
	try
	{
		BString fileName;
		Directory dir(_replicationLogPath.c_str());
		while (dir.next())
		{
			if (strncmp(dir.name(), REPLICATION_FILE_PREFIX.c_str(), REPLICATION_FILE_PREFIX.size()))
				continue;
			char *serverIDend;
			TServerID serverID = strtoul(dir.name() + REPLICATION_FILE_PREFIX.size(), &serverIDend, 10);
			if (serverID != _serverID)
				continue;
			if (*serverIDend != '_')
				continue;
			TReplicationLogNumber number = strtoul(serverIDend + 1, NULL, 16);
			
			fileName.sprintfSet("%s/%s", _replicationLogPath.c_str(), dir.name());
			TReplicationLogPtr rl(new ReplicationLog(number, fileName.c_str()));
			if (!rl->openForRead())
				return false;
			_replicationLogFiles.push_back(rl);
		}
		std::sort(_replicationLogFiles.begin(), _replicationLogFiles.end(), &_sortNumber);
		if (!_replicationLogFiles.empty())
			_currentReplicationLog = _replicationLogFiles.back();
		log::Info::L("Found %u bin logs at %s\n", _replicationLogFiles.size(), _replicationLogPath.c_str());
		return true;
	}
	catch (Directory::Error &er)
	{
		log::Error::L("Can't start replication from %s\n", _replicationLogPath.c_str());
		return false;
	}
}

void Index::addToReplicationLog(Buffer &buffer)
{
	if (!_replicationLogKeepTime)
		return;
	AutoMutex autoSync(&_replicationSync);
	if (!_currentReplicationLog->canFit(buffer.writtenSize()))
	{
		_currentReplicationLog.reset();
		if (!_openCurrentReplicationLog())
			throw std::exception();
	}
	auto rl = _currentReplicationLog;
	autoSync.unLock();
	rl->save(buffer);
}

bool Index::getFromReplicationLog(const TServerID serverID, BString &data, Buffer &buffer, 
	TReplicationLogNumber &startNumber, uint32_t &seek)
{
	AutoMutex autoSync(&_replicationSync);
	auto repl = _replicationLogFiles.rbegin();
	for (;  repl != _replicationLogFiles.rend(); repl++) {
		if ((*repl)->number() == startNumber)
			break;
	}
	if (repl == _replicationLogFiles.rend())
	{
		log::Error::L("[%s] Can't find log %u seek %u for server %u\n", _replicationLogPath.c_str(), startNumber, seek);
		return false;
	}
	while (true)
	{
		if ((*repl)->haveData(seek))
			break;
		if (repl == _replicationLogFiles.rbegin())
			return true;
		repl++;
		seek = 0;
		startNumber = (*repl)->number();
	}
	auto rl = (*repl);
	autoSync.unLock();
	return 	rl->read(serverID, data, buffer, seek);
}

bool Index::addFromAnotherServer(const TServerID serverID, Buffer &data, const ItemHeader::TTime curTime, 
	Buffer &buffer)
{
	try
	{
		std::string topLevelName;
		while (data.readPos() < data.writtenSize())
		{
			TopLevelIndex::ReplicationPacketHeader &rph = 
				*(TopLevelIndex::ReplicationPacketHeader*)data.mapBuffer(sizeof(TopLevelIndex::ReplicationPacketHeader));
			if (rph.serverID == _serverID)
			{
				log::Warning::L("Server %u has received its packet from %u\n", _serverID, serverID);
				data.skip(rph.packetSize);
				continue;
			}
			Buffer::TSize curReadPos = data.readPos();
			data.get(topLevelName);
			AutoMutex autoSync(&_sync);
			
			auto f = _index.find(topLevelName);
			if (f == _index.end()) {
				autoSync.unLock();
				if (!create(topLevelName, rph.md.subLevelKeyType, rph.md.itemKeyType))
					return false;
				autoSync.lock(&_sync);
				f = _index.find(topLevelName);
			} else{
				if (f->second->md() != rph.md)
				{
					log::Fatal::L("Level's meta data mismatch %s/%s\n", _path.c_str(), topLevelName.c_str());
					return false;
				}
			}
			auto topLevel = f->second;
			autoSync.unLock();
			
			Buffer::TSize endPacketPos = curReadPos + rph.packetSize;
			if (!topLevel->addFromAnotherServer(serverID, data, endPacketPos, curTime, buffer))
				return false;
		}
		return true;
	}
	catch (Buffer::Error &er)
	{
		log::Error::L("Catch Buffer exception in packet from server %u\n", serverID);
		return false;
	}
	return false;
}


void Index::exitFlush()
{
	log::Error::L("Index %s flushing\n", _path.c_str());
	sleep(1); // wait for finishing commands
	
	_hourlySync.lock();
	_sync.lock();
	fl::chrono::Time curTime;
	Buffer buf(MAX_BUF_SIZE + 1);
	for (auto topLevel = _index.begin(); topLevel != _index.end(); topLevel++) {
		curTime.update();
		topLevel->second->sync(buf, curTime.unix(), true);
	}
}
