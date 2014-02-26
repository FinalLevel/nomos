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


using namespace fl::nomos;
using fl::fs::Directory;
using fl::fs::File;
using namespace fl::utils;

void TopLevelIndex::_formMetaFileName(const std::string &path, BString &metaFileName)
{
	metaFileName.sprintfSet("%s/.meta", path.c_str());
}

const std::string TopLevelIndex::DATA_FILE_NAME("data");


bool TopLevelIndex::_createDataFile(const std::string &path, const u_int32_t curTime, const u_int32_t openNumber, 
	const MetaData &md, File &file)
{
	BString fileName;
	fileName.sprintfSet("%s/%s_%u_%u", path.c_str(), DATA_FILE_NAME.c_str(), curTime, openNumber);
	if (fileExists(fileName.c_str()))
	{
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
	if (fileExists(fileName.c_str()))
	{
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


template <typename TSubLevelKey, typename TItemKey>
class MemmoryTopLevelIndex : public TopLevelIndex
{
public:
	typedef u_int16_t TSliceCount;
	static const TSliceCount ITEM_DEFAULT_SLICES_COUNT = 10;
	MemmoryTopLevelIndex(const std::string &path, const MetaData &md)
		: _path(path), _md(md), _slicesCount(ITEM_DEFAULT_SLICES_COUNT)
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
			_headerPacketSync.lock();
			_headerPackets.push_back(headerPacket);
			_headerPacketSync.unLock();
			return true;
		}
	}
	
	virtual TItemSharedPtr find(const std::string &subLevelKeyStr, const std::string &key, 
		const ItemHeader::TTime curTime) 
	{
		const TSubLevelKey subLevelKey = convertStdStringTo<TSubLevelKey>(subLevelKeyStr.c_str());
		const TItemKey itemKey = convertStdStringTo<TItemKey>(key.c_str());
		
		AutoMutex autoSync(&_sync);
		auto subLevel = _subLevelItem.find(subLevelKey);
		if (subLevel == _subLevelItem.end())
			return TItemSharedPtr();
		auto sliceID = _findSlice(itemKey);
		auto item = subLevel->second[sliceID].find(itemKey);
		if (item == subLevel->second[sliceID].end())
			return TItemSharedPtr();
		else if (item->second->isValid(curTime))
			return item->second;
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
			const ItemHeader &itemHeader = item->second->header();
			ItemHeader::TTime lastLiveTo = itemHeader.liveTo;
			if (setTime == 0)
				item->second->setLiveTo(setTime, curTime);
			else
				item->second->setLiveTo(setTime + curTime, curTime);
			
			if (abs(lastLiveTo - itemHeader.liveTo) > (setTime * MIN_SYNC_TOUCH_TIME_PERCENT))
			{
				headerPacket.itemHeader = itemHeader;
				autoSync.unLock();
				_headerPacketSync.lock();
				_headerPackets.push_back(headerPacket);
				_headerPacketSync.unLock();
			}
			return true;
		}	else {
			subLevel->second[sliceID].erase(item);
			return false;
		}
	}
	
	virtual void put(const std::string &subLevel, const std::string &key, TItemSharedPtr &item)
	{
		DataPacket dataPacket;
		dataPacket.subLevelKey = convertStdStringTo<TSubLevelKey>(subLevel);
		dataPacket.itemKey = convertStdStringTo<TItemKey>(key);
		dataPacket.item = item;
		
		AutoMutex autoSync(&_sync);
		_put(dataPacket.subLevelKey, dataPacket.itemKey, item);
		_sync.unLock();
		
		_dataPacketSync.lock();
		_dataPackets.push_back(dataPacket);
		_dataPacketSync.unLock();
	}
	
	virtual bool sync(Buffer &buf, const ItemHeader::TTime curTime)
	{
		AutoMutex autoSync;
		if (!autoSync.tryLock(&_diskLock)) // some process already working with this level
			return false;
		
		TDataPacketVector dataPackets;
		_dataPacketSync.lock();
		std::swap(dataPackets, _dataPackets);
		_dataPacketSync.unLock();

		THeaderPacketVector headerPackets;
		_headerPacketSync.lock();
		std::swap(headerPackets, _headerPackets);
		_headerPacketSync.unLock();

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
	
	struct HeaderCMDData
	{
		ItemHeader::TTime liveTo;
		ItemHeader::UTag tag;
	};
	typedef unordered_map<TItemKey, HeaderCMDData> THeaderCMDDataHash;
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
						item = subLevel->second[slice].erase(item);
				};
			};
		}
	}
private:
	TSliceCount _findSlice(const TItemKey &itemKey)
	{
		return getCheckSum32Tmpl<TItemKey>(itemKey) % _slicesCount;
	}
	void _put(const TSubLevelKey &subLevelKey, const TItemKey &itemKey, TItemSharedPtr &item)
	{
		static TItemIndexVector startIndexSlices(_slicesCount);
		auto res = _subLevelItem.emplace(subLevelKey, startIndexSlices);
		auto sliceID = _findSlice(itemKey);
		auto itemRes = res.first->second[sliceID].emplace(itemKey, item);
		if (!itemRes.second)
			itemRes.first->second = item;
	}
	
	void _put(const TSubLevelKey &subLevelKey, const TItemKey &itemKey, const ItemHeader &itemHeader, const char *data)
	{
		static TItemIndexVector startIndexSlices(_slicesCount);
		static TItemSharedPtr empty;
		auto res = _subLevelItem.emplace(subLevelKey, startIndexSlices);
		auto sliceID = _findSlice(itemKey);
		auto itemRes = res.first->second[sliceID].emplace(itemKey, empty);
		if (!itemRes.second)
		{
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
	
	Mutex _dataPacketSync;
	typedef std::vector<DataPacket> TDataPacketVector;
	TDataPacketVector _dataPackets;
	
	struct HeaderPacket
	{
		EIndexCMDType::EIndexCMDType cmd;
		TSubLevelKey subLevelKey;
		TItemKey itemKey;
		ItemHeader itemHeader;
	};
	Mutex _headerPacketSync;
	typedef std::vector<HeaderPacket> THeaderPacketVector;
	THeaderPacketVector _headerPackets;

	
	Mutex _diskLock;
	File _dataFile;
	File _headerFile;
	bool _openFiles(const u_int32_t curTime)
	{
		if (!_dataFile.descr()) // not open
		{
			static u_int32_t openNumber = 0;
			openNumber++;
			if (!_createDataFile(_path, curTime, openNumber, _md, _dataFile))
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
	
	bool _loadHeaderData(const char *path, Buffer &buf, const ItemHeader::TTime curTime, THeaderCMDIndexHash &removeTouchIndex)
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
					headerCMDData.liveTo = 1;
				else
					headerCMDData.liveTo = itemHeader.liveTo;
				headerCMDData.tag = itemHeader.timeTag;
				auto res = removeTouchIndex.emplace(subLevelKey, emptySubLevelIndex).first->second.emplace(itemKey, headerCMDData);
				if (!res.second && (res.first->second.tag.tag < headerCMDData.tag.tag))
					res.first->second = headerCMDData;
			}

		}
		catch (Buffer::Error &er)
		{
			log::Fatal::L("Catch error in data file %s\n", path);
			return false;
		}
		return true;
	}
	
	void _syncDataPackets(TDataPacketVector &workPackets, Buffer &buf, const ItemHeader::TTime curTime)
	{
		EIndexCMDType::EIndexCMDType cmd = EIndexCMDType::PUT;
		buf.clear();
		for (auto dataPacket = workPackets.begin(); dataPacket != workPackets.end(); dataPacket++) {
			if (dataPacket->item->isValid(curTime)) {
				buf.add(cmd);
				const ItemHeader &itemHeader = dataPacket->item->header();
				buf.add(&itemHeader, sizeof(itemHeader));
				buf.add(dataPacket->subLevelKey);
				buf.add(dataPacket->itemKey);
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
			if ((md.itemKeyType != _md.itemKeyType) || (md.subLevelKeyType != _md.subLevelKeyType)) {
				log::Error::L("Sublevel/KeyType mismatch %s (%u/%u | %u/%u)\n", path, md.subLevelKeyType, _md.subLevelKeyType,
					md.subLevelKeyType, _md.subLevelKeyType);
				return false;

			}
			EIndexCMDType::EIndexCMDType cmd;
			ItemHeader itemHeader;
			TSubLevelKey subLevelKey;
			TItemKey itemKey;
			while (!buf.isEnded()) {
				buf.get(cmd);
				if (cmd != EIndexCMDType::PUT)
				{
					log::Error::L("Bad cmd type %u in data\n", cmd);
					return false;
				}
				buf.get(&itemHeader, sizeof(itemHeader));
				buf.get(subLevelKey);
				buf.get(itemKey);
				auto f = removeTouchIndex.find(subLevelKey);
				if (f != removeTouchIndex.end())
				{
					auto fHeader = f->second.find(itemKey);
					if (fHeader != f->second.end())
					{
						if (fHeader->second.tag.tag >= itemHeader.timeTag.tag)
						{
							itemHeader.liveTo = fHeader->second.liveTo;
							itemHeader.timeTag.tag = fHeader->second.tag.tag;
						}
					}
				}
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
	
	std::string _path;
	MetaData _md;
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
	const EKeyType itemKeyType, const std::string &path, const TopLevelIndex::MetaData &md
)
{
	switch (itemKeyType) 
	{
	case KEY_STRING:
		return new TIndexClass<TSubLevelKey, TKeyType<KEY_STRING>::type>(path, md);
	case KEY_INT32:
		return new TIndexClass<TSubLevelKey, TKeyType<KEY_INT32>::type>(path, md);
	case KEY_INT64:
		return new TIndexClass<TSubLevelKey, TKeyType<KEY_INT64>::type>(path, md);
	};
	return  NULL;
}
template <template<typename TSubLevelKey, typename TItemKey> class TIndexClass>
TopLevelIndex *createTopLevelIndex(
	const EKeyType subLevelType, 
	const EKeyType itemKeyType, 
	const std::string &path, 
	const TopLevelIndex::MetaData &md
)
{
	switch (subLevelType)
	{
		case KEY_STRING:
			return createTopLevelIndex<TIndexClass, TKeyType<KEY_STRING>::type>(itemKeyType, path, md);
		case KEY_INT32:
			return createTopLevelIndex<TIndexClass, TKeyType<KEY_INT32>::type>(itemKeyType, path, md);
		case KEY_INT64:
			return createTopLevelIndex<TIndexClass, TKeyType<KEY_INT64>::type>(itemKeyType, path, md);
	};
	return  NULL;
}

TopLevelIndex *TopLevelIndex::createFromDirectory(const std::string &path)
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
					static_cast<EKeyType>(md.subLevelKeyType), static_cast<EKeyType>(md.itemKeyType), path, md);
}

TopLevelIndex *TopLevelIndex::create(
	const std::string &path, 
	const EKeyType subLevelKeyType, 
	const EKeyType itemKeyType
)
{
	if (!Directory::makeDirRecursive(path.c_str()))
		return NULL;
	MetaData md;
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
	
	return createTopLevelIndex<MemmoryTopLevelIndex>(subLevelKeyType, itemKeyType, path, md);
}

Index::Index(const std::string &path)
	: _path(path)
{
	Directory dir(path.c_str());
	BString topLevelPath;
	while (dir.next()) {
		if (dir.name()[0] == '.') // skip
			continue;
		topLevelPath.sprintfSet("%s/%s", path.c_str(), dir.name());
		TopLevelIndex *topLevelIndex = TopLevelIndex::createFromDirectory(topLevelPath.c_str());
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
	TItemSharedPtr &item)
{
	AutoMutex autoSync(&_sync);
	auto f = _index.find(level);
	if (f == _index.end())
		return false;
	auto topLevel = f->second;
	autoSync.unLock();
	topLevel->put(subLevel, itemKey, item);
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
	return topLevel->remove(subLevel, itemKey);
}

TItemSharedPtr Index::find(const std::string &level, const std::string &subLevel, const std::string &itemKey, 
	const ItemHeader::TTime curTime)
{
	AutoMutex autoSync(&_sync);
	auto f = _index.find(level);
	if (f == _index.end())
		return TItemSharedPtr();
	auto topLevel = f->second;
	autoSync.unLock();
	return topLevel->find(subLevel, itemKey, curTime);
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
	return topLevel->touch(subLevel, itemKey, setTime, curTime);
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
	TopLevelIndex *topLevelIndex = TopLevelIndex::create(path.c_str(), subLevelKeyType, itemKeyType);
	if (!topLevelIndex)	{
		log::Error::L("Cannot create new TopLevelIndex %s\n", path.c_str());
		return false;
	}
	_index.emplace(level, TTopLevelIndexPtr(topLevelIndex));
	return true;
}
