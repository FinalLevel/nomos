///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Nomos server's config class
///////////////////////////////////////////////////////////////////////////////

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>

#include "config.hpp"
#include "log.hpp"

using namespace fl::nomos;
using namespace boost::property_tree::ini_parser;

Config::Config(int argc, char *argv[])
	: _status(0), _logLevel(FL_LOG_LEVEL), _cmdTimeout(0), _workerQueueLength(0), _workers(0)
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
		if (pt.get<std::string>("nomos-server.logStdout", "") == "on")
			_status |= ST_LOG_STDOUT;
		_dataPath = pt.get<decltype(_dataPath)>("nomos-server.dataPath", "");
		if (_dataPath.empty())
		{
			printf("Nomos dataPath is not set\n");
			throw std::exception();
		}
		_cmdTimeout =  pt.get<decltype(_cmdTimeout)>("nomos-server.cmdTimeout", DEFAULT_SOCKET_TIMEOUT);
		_workerQueueLength = pt.get<decltype(_workerQueueLength)>("nomos-server.socketQueueLength", 
			DEFAULT_SOCKET_QUEUE_LENGTH);
		_workers = pt.get<decltype(_workers)>("nomos-server.workers", DEFAULT_WORKERS_COUNT);
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