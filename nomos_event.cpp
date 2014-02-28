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

using namespace fl::nomos;

bool NomosEvent::_inited = false;
Config *NomosEvent::_config = NULL;

void NomosEvent::setConfig(Config *config)
{
	_config = config;
}

void NomosEvent::setInited(const bool inited)
{
	_inited = inited;
}

NomosEvent::NomosEvent(const TEventDescriptor descr, const time_t timeOutTime)
	: WorkEvent(descr, timeOutTime), _networkBuffer(NULL), _curState(ST_WAIT_QUERY), _querySize(0)
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
	if (_descr != 0)
		close(_descr);
	if (_networkBuffer)
	{
		auto threadSpecData = static_cast<NomosThreadSpecificData*>(_thread->threadSpecificData());
		threadSpecData->bufferPool.free(_networkBuffer);
		_networkBuffer = NULL;
	}
}

inline bool _readString(std::string &str, NetworkBuffer::TDataPtr &query, const char ch)
{
	char *pEnd = strchr(query, ',');
	if (!pEnd)
		return false;
	auto len = pEnd - query;
	if (len < 1)
		return false;
	str.assign(query, len);
	query = pEnd + 1;
	return true;
}

bool NomosEvent::_parsePutQuery(NetworkBuffer::TDataPtr &query)
{
	if (!_readString(_level, query, ','))
		return false;
	if (!_readString(_subLevel, query, ','))
		return false;
	if (!_readString(_itemKey, query, ','))
		return false;
	char *endQ;
	_lifeTime = strtoul(query, &endQ, 10);
	if (*endQ != ',')
		return false;
	query = endQ + 1;
	_itemSize = strtoul(query, &endQ, 10);
	if (!_itemSize)
		return false;
	uint32_t readBodyLen = _networkBuffer->size() - _querySize;
	if (readBodyLen >= _itemSize)
		_curState = ST_FORM_ANSWER;
	else
		_curState = ST_WAIT_DATA;
	return true;
}

bool NomosEvent::_parseQuery()
{
	NetworkBuffer::TDataPtr query = _networkBuffer->c_str();
	
	if (memcmp(query, "V01,", 4))
		return false;
	query += 4;
	_cmd = static_cast<ENomosCMD>(*query);
	switch (_cmd)
	{
	case CMD_PUT:
		return _parsePutQuery(query);
		break;
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
			return false;
		if (*(endQuery - 1) == '\r')
			*const_cast<char*>(endQuery - 1) = 0;
		else
			*const_cast<char*>(endQuery) = 0;	
		if (_parseQuery())
			return true;
		else
		{
			_curState = ER_PARSE;
			return false;
		}
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
	if (readBodyLen >= _itemSize)
		_curState = ST_FORM_ANSWER;
	return true;
}

bool NomosEvent::_formAnswer()
{
	if (!_inited)
	{
		_curState = ER_NOT_READY;
		return false;
	}
	_networkBuffer->clear();
	_networkBuffer->sprintfSet("OK\n");
	return true;
}

NomosEvent::ECallResult NomosEvent::_sendError()
{
	int erorrNum = _curState;
	if (erorrNum > ER_UNKNOWN)
		erorrNum = ER_UNKNOWN;
	_networkBuffer->clear();
	
	_networkBuffer->sprintfSet("ER%8x\n", erorrNum);
	auto res = _networkBuffer->send(_descr);
	if (res == NetworkBuffer::IN_PROGRESS) {
		_setWaitSend();
		if (_thread->ctrl(this))
		{
			_curState = ST_SEND_AND_CLOSE;
			return SKIP;
		}
		else
			return FINISHED;
	}
	else
		return FINISHED;
}

NomosEvent::ECallResult NomosEvent::_sendAnswer()
{
	auto res = _networkBuffer->send(_descr);
	if (res == NetworkBuffer::IN_PROGRESS) {
		_setWaitSend();
		if (_thread->ctrl(this)) {
			_curState = ST_SEND_AND_CLOSE;
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
		if (_curState == ST_FORM_ANSWER) {
			if (_formAnswer()) {
				return _sendAnswer();
			}
			else
				return _sendError();
		}
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

WorkEvent *NomosEventFactory::create(const TEventDescriptor descr, const time_t timeOutTime, Socket* acceptSocket)
{
	return new NomosEvent(descr, EPollWorkerGroup::curTime.unix() + _config->cmdTimeout());
}
