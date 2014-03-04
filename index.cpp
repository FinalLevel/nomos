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
#include "cond_mutex.hpp"


using namespace fl::nomos;
using fl::fs::Directory;
using fl::fs::File;
using namespace fl::utils;

TopLevelIndex::TopLevelIndex(Index *index, const std::string &path, const MetaData &md)
	: _index(index), _path(path), _md(md)
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
	MemmoryTopLevelIndex(Index *index, const std::string &path, const MetaData &md)
		: TopLevelIndex(index, path, md), _slicesCount(ITEM_DEFAULT_SLICES_COUNT)
	{

	}

	virtual ~MemmoryTopLevelIndex() {}

	virtual bool remove(const std::string &subLevelKeyStr, const std::string &key)
	{
		HeaderPacket headerPacket;
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
		HeaderPacket headerPacket;
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
				_touch(headerPacket, item->second.get(), lifeTime, curTime);
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
		HeaderPacket headerPacket;
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
			_touch(headerPacket, item->second.get(), setTime, curTime);
			return true;
		}	else {
			subLevel->second[sliceID].erase(item);
			return false;
		}
	}
	
	virtual void put(const std::string &subLevel, const std::string &key, TItemSharedPtr &item, bool checkBeforeReplace)
	{
		DataPacket dataPacket;
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
				HeaderPacket headerPacket;
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
				
				HeaderPacket headerPacket;
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
	
	virtual bool sync(Buffer &buf, const ItemHeader::TTime curTime)
	{
		AutoMutex autoSync;
		if (!autoSync.tryLock(&_diskLock)) // some process already working with this level
			return false;
		
		TDataPacketVector dataPackets;
		THeaderPacketVector headerPackets;
		
		_packetSync.lock();
		std::swap(dataPackets, _dataPackets);
		std::swap(headerPackets, _headerPackets);
		_packetSync.unLock();

		if (dataPackets.empty() && headerPackets.empty()) // work ended
			return true;
		
		if (!_openFiles(curTime))
		{
			log::Fatal::L("Can't open files for %s synchronization\n", _path.c_str());
			throw std::exception();
		}
		_syncDataPackets(dataPackets, buf, curTime);
		_syncHeaderPackets(headerPackets, buf, curTime);

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
		_dataFile.close();
		_headerFile.close();
		
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
		TSubLevelKey subLevelKey;
		TItemKey itemKey;
		TItemSharedPtr item;
	};
	
	Mutex _packetSync;
	typedef std::vector<DataPacket> TDataPacketVector;
	TDataPacketVector _dataPackets;
	
	struct HeaderPacket
	{
		EIndexCMDType::EIndexCMDType cmd;
		TSubLevelKey subLevelKey;
		TItemKey itemKey;
		ItemHeader itemHeader;
	};
	
	typedef std::vector<HeaderPacket> THeaderPacketVector;
	THeaderPacketVector _headerPackets;
	
	void _touch(HeaderPacket &headerPacket, Item *item, const ItemHeader::TTime setTime, 
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
	
	void _syncHeaderPackets(THeaderPacketVector &headerPackets, Buffer &buf, const ItemHeader::TTime curTime)
	{
		buf.clear();
		for (auto packet = headerPackets.begin(); packet != headerPackets.end(); packet++) {
			buf.add(packet->cmd);
			buf.add(&packet->itemHeader, sizeof(packet->itemHeader));
			buf.add(packet->subLevelKey);
			buf.add(packet->itemKey);
			if ((buf.writtenSize() > MAX_BUF_SIZE) || ((packet + 1) == headerPackets.end())) {
				if (!buf.empty()) {
					if (_headerFile.write(buf.begin(), buf.writtenSize()) != static_cast<ssize_t>(buf.writtenSize())) { // skip 
						log::Fatal::L("Can't sync %s\n", _path.c_str());
						throw std::exception();
					};
					buf.clear();
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
				buf.get(cmd);
				if ((cmd != EIndexCMDType::TOUCH) && (cmd != EIndexCMDType::REMOVE)) {
					log::Error::L("Bad cmd type %u in data\n", cmd);
					return false;
				}
				buf.get(&itemHeader, sizeof(itemHeader));
				buf.get(subLevelKey);
				buf.get(itemKey);
				
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
	void _syncDataEntryHeader(Buffer &buf, const ItemHeader &itemHeader, const TSubLevelKey &subLevelKey, 
		const TItemKey &itemKey)
	{
		buf.add(&itemHeader, sizeof(itemHeader));
		buf.add(subLevelKey);
		buf.add(itemKey);
	}

	bool _loadDataEntryHeader(Buffer &buf, ItemHeader &itemHeader, TSubLevelKey &subLevelKey, TItemKey &itemKey, 
		THeaderCMDIndexHash &removeTouchIndex)
	{
		EIndexCMDType::EIndexCMDType cmd;
		buf.get(cmd);
		if (cmd != EIndexCMDType::PUT) {
			log::Error::L("Bad cmd type %u in data\n", cmd);
			throw std::exception();
		}
		buf.get(&itemHeader, sizeof(itemHeader));
		buf.get(subLevelKey);
		buf.get(itemKey);
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
		EIndexCMDType::EIndexCMDType cmd = EIndexCMDType::PUT;
		buf.clear();
		for (auto dataPacket = workPackets.begin(); dataPacket != workPackets.end(); dataPacket++) {
			if (dataPacket->item->isValid(curTime)) {
				buf.add(cmd);
				const ItemHeader &itemHeader = dataPacket->item->header();
				_syncDataEntryHeader(buf, itemHeader, dataPacket->subLevelKey, dataPacket->itemKey);
				buf.add(dataPacket->item->data(), itemHeader.size);
			}
			if ((buf.writtenSize() > MAX_BUF_SIZE) || ((dataPacket + 1) == workPackets.end())) {
				if (!buf.empty()) {
					if (_dataFile.write(buf.begin(), buf.writtenSize()) != static_cast<ssize_t>(buf.writtenSize())) { // skip 
						log::Fatal::L("Can't sync %s\n", _path.c_str());
						throw std::exception();
					};
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
						_syncDataEntryHeader(writeBuffer, itemHeader, subLevelKey, itemKey);
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
TopLevelIndex *createTopLevelIndex(
	const EKeyType itemKeyType, Index *index, const std::string &path, const TopLevelIndex::MetaData &md
)
{
	switch (itemKeyType) 
	{
	case KEY_STRING:
		return new TIndexClass<TSubLevelKey, TKeyType<KEY_STRING>::type>(index, path, md);
	case KEY_INT32:
		return new TIndexClass<TSubLevelKey, TKeyType<KEY_INT32>::type>(index, path, md);
	case KEY_INT64:
		return new TIndexClass<TSubLevelKey, TKeyType<KEY_INT64>::type>(index, path, md);
	};
	return  NULL;
}
template <template<typename TSubLevelKey, typename TItemKey> class TIndexClass>
TopLevelIndex *createTopLevelIndex(
	const EKeyType subLevelType, 
	const EKeyType itemKeyType, 
	Index *index,
	const std::string &path, 
	const TopLevelIndex::MetaData &md
)
{
	switch (subLevelType)
	{
		case KEY_STRING:
			return createTopLevelIndex<TIndexClass, TKeyType<KEY_STRING>::type>(itemKeyType, index, path, md);
		case KEY_INT32:
			return createTopLevelIndex<TIndexClass, TKeyType<KEY_INT32>::type>(itemKeyType, index, path, md);
		case KEY_INT64:
			return createTopLevelIndex<TIndexClass, TKeyType<KEY_INT64>::type>(itemKeyType, index, path, md);
	};
	return  NULL;
}

TopLevelIndex *TopLevelIndex::createFromDirectory(Index *index, const std::string &path)
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
	return createTopLevelIndex<MemmoryTopLevelIndex>(
					static_cast<EKeyType>(md.subLevelKeyType), static_cast<EKeyType>(md.itemKeyType), index, path, md);
}

TopLevelIndex *TopLevelIndex::create(
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
	
	return createTopLevelIndex<MemmoryTopLevelIndex>(subLevelKeyType, itemKeyType, index, path, md);
}

Index::Index(const std::string &path)
	: _path(path), _status(0), _subLevelKeyType(KEY_INT32), _itemKeyType(KEY_INT64), 
	_timeThread(5 * 60) // minutes tic time
{
	Directory dir(path.c_str());
	BString topLevelPath;
	while (dir.next()) {
		if (dir.name()[0] == '.') // skip
			continue;
		topLevelPath.sprintfSet("%s/%s", path.c_str(), dir.name());
		TopLevelIndex *topLevelIndex = TopLevelIndex::createFromDirectory(this, topLevelPath.c_str());
		if (!topLevelIndex) {
			log::Error::L("Cannot load TopLevelIndex %s\n", topLevelPath.c_str());
			continue;
		}
		_index.emplace(std::string(dir.name()), TTopLevelIndexPtr(topLevelIndex));
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
		if (!topLevel->second->sync(buf, curTime))
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
	TopLevelIndex *topLevelIndex = TopLevelIndex::create(this, path.c_str(), subLevelKeyType, itemKeyType);
	if (!topLevelIndex)	{
		log::Error::L("Cannot create new TopLevelIndex %s\n", path.c_str());
		return false;
	}
	_index.emplace(level, TTopLevelIndexPtr(topLevelIndex));
	return true;
}

bool Index::hour(fl::chrono::ETime &curTime)
{
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

IndexSyncThread::IndexSyncThread()
{
	static const uint32_t SYNC_THREAD_STACK_SIZE = 100000;
	setStackSize(SYNC_THREAD_STACK_SIZE);
	if (!create())
	{
		log::Fatal::L("Can't create an index thread\n");
		throw std::exception();
	}
}

void IndexSyncThread::add(TTopLevelIndexPtr &topLevel)
{
	_sync.lock();
	_needSyncLevels.push_back(topLevel);
	_sync.unLock();
	_cond.sendSignal();
}

void IndexSyncThread::run()
{
	fl::chrono::Time curTime;
	Buffer buf(MAX_BUF_SIZE + 1);
	while (true)
	{
		TTopLevelVector workItems;
		_sync.lock();
		std::swap(_needSyncLevels, workItems);
		_sync.unLock();
		if (workItems.empty())
		{
			_cond.waitSignal();
			continue;
		}
		curTime.update();
		for (auto level = workItems.begin(); level != workItems.end(); level++) {
			if (!(*level)->sync(buf, curTime.unix()))	{
				_sync.lock();
				_needSyncLevels.push_back(*level);
				_sync.unLock();
			}
		}
	}
}
