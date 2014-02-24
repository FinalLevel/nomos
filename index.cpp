///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Index maintenance classes
///////////////////////////////////////////////////////////////////////////////

#include <type_traits>
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

	virtual bool load(const std::string &path)
	{
		return false;
	}
	virtual bool remove(const std::string &subLevelKeyStr, const std::string &key)
	{
		const TSubLevelKey subLevelKey = convertStdStringTo<TSubLevelKey>(subLevelKeyStr.c_str());
		const TItemKey itemKey = convertStdStringTo<TItemKey>(key.c_str());
		
		AutoMutex autoSync(&_sync);
		auto subLevel = _subLevelItem.find(subLevelKey);
		if (subLevel == _subLevelItem.end())
			return false;
		
		auto sliceID = _findSlice(itemKey);
		auto item = subLevel->second[sliceID].find(itemKey);
		if (item == subLevel->second[sliceID].end())
			return false;
		else {
			subLevel->second[sliceID].erase(item);	
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
		const TSubLevelKey subLevelKey = convertStdStringTo<TSubLevelKey>(subLevelKeyStr.c_str());
		const TItemKey itemKey = convertStdStringTo<TItemKey>(key.c_str());
		
		AutoMutex autoSync(&_sync);
		auto subLevel = _subLevelItem.find(subLevelKey);
		if (subLevel == _subLevelItem.end())
			return false;
		auto sliceID = _findSlice(itemKey);
		auto item = subLevel->second[sliceID].find(itemKey);
		if (item == subLevel->second[sliceID].end())
			return false;
		else if (item->second->isValid(curTime)) {
			item->second->setLiveTo(setTime, curTime);
			return true;
		}	else {
			subLevel->second[sliceID].erase(item);
			return false;
		}
	}
	
	virtual void put(const std::string &subLevel, const std::string &key, TItemSharedPtr &item)
	{
		const TSubLevelKey subLevelKey = convertStdStringTo<TSubLevelKey>(subLevel);
		const TItemKey itemKey = convertStdStringTo<TItemKey>(key);
		AutoMutex autoSync(&_sync);
		_put(subLevelKey, itemKey, item);
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
	
	std::string _path;
	MetaData _md;
	typedef boost::unordered_map<TItemKey, TItemSharedPtr> TItemIndex;
	typedef std::vector<TItemIndex> TItemIndexVector;
	typedef boost::unordered_map<TSubLevelKey, TItemIndexVector> TSubLevelIndex;
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
struct TKeyType<KEY_INT8>  
{	
	typedef u_int8_t type;
};
template <>
struct TKeyType<KEY_INT16>  
{	
	typedef u_int16_t type;
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
	case KEY_INT8:
		return new TIndexClass<TSubLevelKey, TKeyType<KEY_INT8>::type>(path, md);
	case KEY_INT16:
		return new TIndexClass<TSubLevelKey, TKeyType<KEY_INT16>::type>(path, md);
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
		case KEY_INT8:
			return createTopLevelIndex<TIndexClass, TKeyType<KEY_INT8>::type>(itemKeyType, path, md);
		case KEY_INT16:
			return createTopLevelIndex<TIndexClass, TKeyType<KEY_INT16>::type>(itemKeyType, path, md);
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
		_index.emplace(std::string(dir.name()), topLevelIndex);
	}
	log::Info::L("Loaded %u top indexes\n", _index.size());
}

Index::~Index()
{
}

bool Index::_checkLevelName(const std::string &name)
{
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

void Index::clearOld(const ItemHeader::TTime curTime)
{
	AutoMutex autoSync(&_sync);
	auto indexCopy = _index;
	_sync.unLock();
	for (auto topLevel = indexCopy.begin(); topLevel != indexCopy.begin(); topLevel++)
		topLevel->second->clearOld(curTime);
}
bool Index::put(const std::string &level, const std::string &subLevel, const std::string &itemKey, TItemSharedPtr &item)
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
		return TItemSharedPtr();
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
		return TItemSharedPtr();
	auto topLevel = f->second;
	autoSync.unLock();
	return topLevel->touch(subLevel, itemKey, setTime, curTime);
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
