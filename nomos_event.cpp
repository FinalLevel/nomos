///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Nomos event system classes
///////////////////////////////////////////////////////////////////////////////

#include "nomos_event.hpp"
#include "nomos_log.hpp"
#include "index.hpp"

using namespace fl::nomos;

Config *NomosEvent::_config = NULL;

void NomosEvent::setConfig(Config *config)
{
	_config = config;
}

Index *NomosEvent::_index = NULL;

bool NomosEvent::_isReady = false;
void NomosEvent::setInited(class Index *index)
{
	_index = index;
	_isReady = true;
}

void NomosEvent::exitFlush()
{
	_isReady = false;
	if (_index)
		_index->exitFlush();
}

NomosEvent::NomosEvent(const TEventDescriptor descr, const time_t timeOutTime)
	: WorkEvent(descr, timeOutTime), _networkBuffer(NULL), _curState(ST_WAIT_QUERY), _querySize(0), _dataQuery(NULL)
{
	_setWaitRead();
}

void NomosEvent::_setWaitRead()
{
	_events = E_INPUT | E_ERROR | E_HUP;
}

void NomosEvent::_setWaitSend()
{
	_events = E_OUTPUT | E_ERROR | E_HUP;
}

bool NomosEvent::_reset()
{
	_curState = ST_WAIT_QUERY;
	_networkBuffer->clear();
	_querySize = 0;
	_setWaitRead();
	if (!_thread->ctrl(this))
		return false;
	_timeOutTime = EPollWorkerGroup::curTime.unix() + _config->cmdTimeout();
	return true;
}

NomosEvent::~NomosEvent()
{
	_endWork();
}

void NomosEvent::_endWork()
{
	_curState = ST_FINISHED;
	if (_descr != 0) {
		close(_descr);
		_descr = 0;
	}
	if (_networkBuffer)
	{
		auto threadSpecData = static_cast<NomosThreadSpecificData*>(_thread->threadSpecificData());
		threadSpecData->bufferPool.free(_networkBuffer);
		_networkBuffer = NULL;
	}
	delete _dataQuery;
	_dataQuery = NULL;
}

inline bool _readString(std::string &str, NetworkBuffer::TDataPtr &query, const char ch)
{
	char *pEnd = strchr(query, ch);
	if (!pEnd)
		return false;
	auto len = pEnd - query;
	if (len < 1)
		return false;
	str.assign(query, len);
	query = pEnd + 1;
	return true;
}

bool NomosEvent::_parseCreateQuery(NetworkBuffer::TDataPtr &query)
{
	std::string level;
	if (!_readString(level, query, ','))
		return false;

	std::string subLevelType;
	if (!_readString(subLevelType, query, ','))
		return false;
	try
	{
		auto subLevelTypeID = Index::stringToType(subLevelType);
		std::string itemType;
		if (!_readString(itemType, query, 0))
			return false;
		auto itemTypeID = Index::stringToType(itemType);
		if (_index->create(level, subLevelTypeID, itemTypeID)) {
			_formOkAnswer(0);
			return true;
		} else {
			_curState = ER_CRITICAL;
			return false;
		}
	}
	catch (...)
	{
		return false;
	}
	return false;
}

bool NomosEvent::_parsePutQuery(NetworkBuffer::TDataPtr &query)
{
	if (!_dataQuery)
		_dataQuery = new DataQuery();
	
	if (!_readString(_dataQuery->level, query, ','))
		return false;
	if (!_readString(_dataQuery->subLevel, query, ','))
		return false;
	if (!_readString(_dataQuery->itemKey, query, ','))
		return false;
	char *endQ;
	_dataQuery->lifeTime = strtoul(query, &endQ, 10);
	if (*endQ != ',')
		return false;
	query = endQ + 1;
	_dataQuery->itemSize = strtoul(query, &endQ, 10);
	if (!_dataQuery->itemSize)
		return false;
	uint32_t readBodyLen = _networkBuffer->size() - _querySize;
	if (readBodyLen >= _dataQuery->itemSize)
		return _formPutAnswer(); // check exists only on UPDATE cmd 
	else {
		_curState = ST_WAIT_DATA;
		return true;
	}
}

bool NomosEvent::_formPutAnswer()
{
	TItemSharedPtr item(new Item(_networkBuffer->c_str() + _querySize, _dataQuery->itemSize, 
		EPollWorkerGroup::curTime.unix() + _dataQuery->lifeTime, EPollWorkerGroup::curTime.unix()));
	auto res = _index->put(_dataQuery->level, _dataQuery->subLevel, _dataQuery->itemKey, item, _cmd == CMD_UPDATE);
	if (res) {
		_formOkAnswer(0);
		return true;
	}
	else
	{
		_curState = ER_PUT;
		return false;
	}
}

bool NomosEvent::_parseGetQuery(NetworkBuffer::TDataPtr &query)
{
	std::string level;
	if (!_readString(level, query, ','))
		return false;
	std::string subLevel;
	if (!_readString(subLevel, query, ','))
		return false;
	std::string itemKey;
	if (!_readString(itemKey, query, ','))
		return false;
	time_t lifeTime = strtoul(query, NULL, 10);
	
	auto item = _index->find(level, subLevel, itemKey, EPollWorkerGroup::curTime.unix(), lifeTime);
	if (item.get() == NULL) {
		_curState = ER_NOT_FOUND;
		return false;
	}
	else
	{
		_formOkAnswer(item->size());
		_networkBuffer->add(static_cast<NetworkBuffer::TDataPtr>(item->data()), item->size());
		return true;
	}
}

bool NomosEvent::_parseTouchQuery(NetworkBuffer::TDataPtr &query)
{
	std::string level;
	if (!_readString(level, query, ','))
		return false;
	std::string subLevel;
	if (!_readString(subLevel, query, ','))
		return false;
	std::string itemKey;
	if (!_readString(itemKey, query, ','))
		return false;
	time_t lifeTime = strtoul(query, NULL, 10);

	if (_index->touch(level, subLevel, itemKey, lifeTime, EPollWorkerGroup::curTime.unix())) {
		_formOkAnswer(0);
		return true;
	} else {
		_curState = ER_NOT_FOUND;
		return false;
	}
}

bool NomosEvent::_parseRemoveQuery(NetworkBuffer::TDataPtr &query)
{
	std::string level;
	if (!_readString(level, query, ','))
		return false;
	std::string subLevel;
	if (!_readString(subLevel, query, ','))
		return false;
	std::string itemKey;
	if (!_readString(itemKey, query, 0))
		return false;

	if (_index->remove(level, subLevel, itemKey)) {
		_formOkAnswer(0);
		return true;
	} else {
		_curState = ER_NOT_FOUND;
		return false;
	}
}

bool NomosEvent::_parseQuery()
{
	if (!_isReady)
	{
		_curState = ER_NOT_READY;
		return false;
	}
	_curState = ER_PARSE;
	NetworkBuffer::TDataPtr query = _networkBuffer->c_str();
	
	if (memcmp(query, "V01,", 4))
		return false;
	query += 4;
	_cmd = static_cast<ENomosCMD>(*query);
	query++;
	if (*query != ',')
		return false;
	query++;
	switch (_cmd)
	{
	case CMD_GET:
		return _parseGetQuery(query);
	case CMD_PUT:
	case CMD_UPDATE:
		return _parsePutQuery(query);
	case CMD_TOUCH:
		return _parseTouchQuery(query);
	case CMD_REMOVE:
		return _parseRemoveQuery(query);
	case CMD_CREATE:
		return _parseCreateQuery(query);
	default:
		return false;
	};
}

bool NomosEvent::_readQuery()
{
	if (!_networkBuffer) {
		auto threadSpecData = static_cast<NomosThreadSpecificData*>(_thread->threadSpecificData());
		_networkBuffer = threadSpecData->bufferPool.get();
	}
	auto lastChecked = _networkBuffer->size();
	auto res = _networkBuffer->read(_descr);
	if ((res == NetworkBuffer::ERROR) || (res == NetworkBuffer::CONNECTION_CLOSE))
		return false;
	else if (res == NetworkBuffer::IN_PROGRESS)
		return true;
	const char *endQuery = strchr(_networkBuffer->c_str() +  lastChecked, '\n');
	if (endQuery) { // query finished
		_querySize =  (endQuery - _networkBuffer->c_str()) + 1;
		static const NetworkBuffer::TSize MIN_QUERY_SIZE = 10;
		if (_querySize < MIN_QUERY_SIZE)
		{
			_curState = ER_PARSE;
			return false;
		}
		if (*(endQuery - 1) == '\r')
			*const_cast<char*>(endQuery - 1) = 0;
		else
			*const_cast<char*>(endQuery) = 0;	
		if (_parseQuery())
			return true;
		else
			return false;
	}
	else
		return true;
}

bool NomosEvent::_readQueryData()
{
	auto res = _networkBuffer->read(_descr);
	if ((res == NetworkBuffer::ERROR) || (res == NetworkBuffer::CONNECTION_CLOSE))
		return false;
	else if (res == NetworkBuffer::IN_PROGRESS)
		return true;
	uint32_t readBodyLen = _networkBuffer->size() - _querySize;
	if (readBodyLen >= _dataQuery->itemSize)
		return _formPutAnswer();
	return true;
}

inline void NomosEvent::_formOkAnswer(const uint32_t size)
{
	_curState = ST_SEND;
	_networkBuffer->clear();
	_networkBuffer->sprintfSet("OK%+08x\n", size);
}

NomosEvent::ECallResult NomosEvent::_sendError()
{
	int erorrNum = _curState;
	if (erorrNum > ER_UNKNOWN)
		erorrNum = ER_UNKNOWN;
	_networkBuffer->clear();
	if (erorrNum <= ER_CRITICAL)	{
		_networkBuffer->sprintfSet("ERR_CR%+04x\n", erorrNum);
		_curState = ST_SEND_AND_CLOSE;
	} else {
		_networkBuffer->sprintfSet("ERR%+07x\n", erorrNum);
		_curState = ST_SEND;
	}
	return _sendAnswer();
}

NomosEvent::ECallResult NomosEvent::_sendAnswer()
{
	auto res = _networkBuffer->send(_descr);
	if (res == NetworkBuffer::IN_PROGRESS) {
		_setWaitSend();
		if (_thread->ctrl(this)) {
			return SKIP;
		}
		else
			return FINISHED;
	} else if (res == NetworkBuffer::OK) {
		if (_curState != ST_SEND_AND_CLOSE) {
			if (_reset())
				return CHANGE;
		}
	}
	return FINISHED;	
}

const NomosEvent::ECallResult NomosEvent::call(const TEvents events)
{
	if (_curState == ST_FINISHED)
		return FINISHED;
	
	if (((events & E_HUP) == E_HUP) || ((events & E_ERROR) == E_ERROR)) {
		_endWork();
		return FINISHED;
	}
	
	if (events & E_INPUT) {
		if (_curState == ST_WAIT_QUERY)	{
			if (!_readQuery())
				return _sendError();
		}
		if (_curState == ST_WAIT_DATA) {
			if (!_readQueryData())
				return _sendError();
		}
		if (_curState == ST_SEND)
			return _sendAnswer();
		else
			return SKIP;
	}
	if (events & E_OUTPUT)
	{
		if ((_curState == ST_SEND) ||  (_curState == ST_SEND_AND_CLOSE)) {
			return _sendAnswer();
		} else {
			log::Error::L("Output event is in error state (%u/%u)\n", _events, _curState);
		}
	}
	return SKIP;
}

NomosThreadSpecificData::NomosThreadSpecificData(Config *config)
	: bufferPool(config->bufferSize(), config->maxFreeBuffers())
{
	
}

NomosThreadSpecificDataFactory::NomosThreadSpecificDataFactory(Config *config)
	: _config(config)
{
}

ThreadSpecificData *NomosThreadSpecificDataFactory::create()
{
	return new NomosThreadSpecificData(_config);
}

NomosEventFactory::NomosEventFactory(Config *config)
	: _config(config)
{
	NomosEvent::setConfig(config);
}

WorkEvent *NomosEventFactory::create(const TEventDescriptor descr, const TIPv4 ip, 
	const time_t timeOutTime, Socket* acceptSocket)
{
	return new NomosEvent(descr, EPollWorkerGroup::curTime.unix() + _config->cmdTimeout());
}
