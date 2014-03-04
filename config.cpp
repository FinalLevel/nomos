///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Nomos server's config class
///////////////////////////////////////////////////////////////////////////////

#include <boost/property_tree/ini_parser.hpp>

#include "config.hpp"
#include "log.hpp"
#include "nomos_log.hpp"
#include "index.hpp"

using namespace fl::nomos;
using namespace boost::property_tree::ini_parser;

Config::Config(int argc, char *argv[])
	: _status(0), _logLevel(FL_LOG_LEVEL), _port(0), _cmdTimeout(0), _workerQueueLength(0), _workers(0),
	_bufferSize(0), _maxFreeBuffers(0),
	_defaultSublevelKeyType(KEY_INT32), _defaultItemKeyType(KEY_INT64),
	_syncThreadsCount(1)
{
	std::string configFileName(DEFAULT_CONFIG);
	char ch;
	while ((ch = getopt(argc, argv, "c:")) != -1) {
		switch (ch) {
			case 'c':
				configFileName = optarg;
			break;
		}
	}
	
	boost::property_tree::ptree pt;
	try
	{
		read_ini(configFileName.c_str(), pt);
		_logPath = pt.get<decltype(_logPath)>("nomos-server.log", _logPath);
		_logLevel = pt.get<decltype(_logLevel)>("nomos-server.logLevel", _logLevel);
		if (pt.get<std::string>("nomos-server.logStdout", "on") == "on")
			_status |= ST_LOG_STDOUT;
		_dataPath = pt.get<decltype(_dataPath)>("nomos-server.dataPath", "");
		if (_dataPath.empty())
		{
			printf("nomos-server.dataPath is not set\n");
			throw std::exception();
		}
		_parseNetworkParams(pt);
		_parseIndexParams(pt);
		_cmdTimeout =  pt.get<decltype(_cmdTimeout)>("nomos-server.cmdTimeout", DEFAULT_SOCKET_TIMEOUT);
		_workerQueueLength = pt.get<decltype(_workerQueueLength)>("nomos-server.socketQueueLength", 
			DEFAULT_SOCKET_QUEUE_LENGTH);
		_workers = pt.get<decltype(_workers)>("nomos-server.workers", DEFAULT_WORKERS_COUNT);
		
		_bufferSize = pt.get<decltype(_bufferSize)>("nomos-server.bufferSize", DEFAULT_BUFFER_SIZE);
		_maxFreeBuffers = pt.get<decltype(_maxFreeBuffers)>("nomos-server.maxFreeBuffers", DEFAULT_MAX_FREE_BUFFERS);
	}
	catch (ini_parser_error &err)
	{
		printf("Caught error %s when parse %s at line %lu\n", err.message().c_str(), err.filename().c_str(), err.line());
		throw err;
	}
	catch(...)
	{
		printf("Caught unknown exception while parsing ini file %s\n", configFileName.c_str());
		throw;
	}
};

void Config::_parseNetworkParams(boost::property_tree::ptree &pt)
{
	_listenIp = pt.get<decltype(_listenIp)>("nomos-server.listen", "");
	if (_listenIp.empty())
	{
		printf("nomos-server.listenIP is not set\n");
		throw std::exception();
	}
	_port = pt.get<decltype(_port)>("nomos-server.port", DEFAULT_CMD_PORT);
}

void Config::_parseIndexParams(boost::property_tree::ptree &pt)
{
	if (pt.get<std::string>("nomos-server.autoCreateTopIndex", "on") == "on")
		_status |= ST_AUTO_CREATE_TOP_LEVEL;
	try
	{
		_defaultSublevelKeyType = Index::stringToType(pt.get<std::string>("nomos-server.defaultSublevelKeyType", "INT32"));
		_defaultItemKeyType = Index::stringToType(pt.get<std::string>("nomos-server.defaultItemKeyType", "INT64"));
		
		_syncThreadsCount = pt.get<decltype(_syncThreadsCount)>("nomos-server.syncThreadsCount", 1);
	}
	catch (Index::ConvertError &e)
	{
		printf("%s\n", e.what());
		throw;
	}
}

bool Config::initNetwork()
{
	if (!_listenSocket.listen(_listenIp.c_str(), _port))
	{
		log::Error::L("Can't listen to %s:%u\n", _listenIp.c_str(), _port);
		return false;
	}
	log::Warning::L("Listen to %s:%u\n", _listenIp.c_str(), _port);
	return true;
}