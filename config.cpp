///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Nomos server's configuration class
///////////////////////////////////////////////////////////////////////////////

#include <boost/property_tree/ini_parser.hpp>
#include <grp.h>
#include <pwd.h>

#include "config.hpp"
#include "log.hpp"
#include "nomos_log.hpp"
#include "index.hpp"

using namespace fl::nomos;
using namespace boost::property_tree::ini_parser;

Config::Config(int argc, char *argv[])
	: _uid(0), _gid(0), _status(0), _logLevel(FL_LOG_LEVEL), _port(0), _cmdTimeout(0), _workerQueueLength(0), _workers(0),
	_bufferSize(0), _maxFreeBuffers(0),
	_defaultSublevelKeyType(KEY_INT32), _defaultItemKeyType(KEY_INT64),
	_syncThreadsCount(1), _serverID(0), _replicationLogKeepTime(0), _replicationPort(0)
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
		if (_dataPath.empty()) {
			printf("nomos-server.dataPath is not set\n");
			throw std::exception();
		}
		_parseUserGroupParams(pt);
		_parseNetworkParams(pt);
		_parseIndexParams(pt);
		_parseReplicationParams(pt);
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

void Config::_parseUserGroupParams(boost::property_tree::ptree &pt)
{
	_userName = pt.get<decltype(_userName)>("nomos-server.user", "nobody");
	auto passwd = getpwnam(_userName.c_str());
	if (passwd) {
		_uid = passwd->pw_uid;
	} else {
		printf("User %s has not been found\n", _userName.c_str());
		throw std::exception();
	}

	_groupName = pt.get<decltype(_groupName)>("nomos-server.group", "nobody");
	auto groupData = getgrnam(_groupName.c_str());
	if (groupData) {
		_gid = groupData->gr_gid;
	} else {
		printf("Group %s has not been found\n", _groupName.c_str());
		throw std::exception();
	}
}

void Config::setProcessUserAndGroup()
{
	if (setgid(_gid) || setuid(_uid)) {
		log::Error::L("Cannot set the process user %s (%d) and gid %s (%d)\n", _userName.c_str(), _uid, _groupName.c_str(),
			_gid);
	} else {
		log::Error::L("Set the process user %s (%d) and gid %s (%d)\n", _userName.c_str(), _uid, _groupName.c_str(), _gid);
	}
}

void Config::_parseNetworkParams(boost::property_tree::ptree &pt)
{
	_listenIp = pt.get<decltype(_listenIp)>("nomos-server.listen", "");
	if (_listenIp.empty()) {
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

void Config::_parseReplicationParams(boost::property_tree::ptree &pt)
{
	auto replicationLogKeepTimeStr = pt.get<std::string>("nomos-server.replicationLogKeepTime", "0");
	char *last;
	u_int32_t replicationLogKeepTime = strtoul(replicationLogKeepTimeStr.c_str(), &last, 10);
	if (!replicationLogKeepTime) // turn replication off
		return;
	if (tolower(*last) == 'h')
		_replicationLogKeepTime = replicationLogKeepTime * 3600;
	else 
		_replicationLogKeepTime = replicationLogKeepTime * 3600 * 24;
	
	_serverID = pt.get<decltype(_serverID)>("nomos-server.serverID", 0);
	if (!_serverID) {
		printf("nomos-server.serverID can't be zero\n");
		throw std::exception();
	}
	_replicationLogPath = pt.get<decltype(_replicationLogPath)>("nomos-server.replicationLogPath", "");
	if (_replicationLogPath.empty())
	{
		printf("nomos-server.replicationLogPath can't be empty\n");
		throw std::exception();
	}
	_replicationPort = pt.get<decltype(_replicationPort)>("nomos-server.replicationPort", 0);
	if (!_replicationPort) {
		printf("nomos-server.replicationPort can't be zero\n");
		throw std::exception();
	}
	
	auto masters = pt.get<std::string>("nomos-server.masters", "");
	const char *pbegin = masters.c_str();
	const char *pend = masters.c_str() + masters.size();
	Server server;
	while (pbegin < pend) {
		const char *p = strchr(pbegin, ':');
		if (p == NULL) { // can't find port
			printf("Can't find port at nomos-server.masters\n");
			throw std::exception();
		}
		int len = p - pbegin;
		static const int MIN_IP_LENGTH = 7;
		if (len < MIN_IP_LENGTH) {
			printf("IP format is mismatch at nomos-server.masters\n");
			throw std::exception();
		}
		std::string listenIP(pbegin, len);
		server.ip = Socket::ip2Long(listenIP.c_str());
		pbegin = p + 1;
		char *endPort;
		server.port = strtoul(pbegin, &endPort, 10);
		if (server.port == 0) {
			printf("Port can't be zero at nomos-server.masters\n");
			throw std::exception();
		}
		_masters.push_back(server);
		pbegin = endPort + 1;
	}
}

bool Config::initNetwork()
{
	if (!_listenSocket.listen(_listenIp.c_str(), _port))	{
		log::Error::L("Can't listen to %s:%u\n", _listenIp.c_str(), _port);
		return false;
	}
	log::Warning::L("Listen to %s:%u\n", _listenIp.c_str(), _port);
	
	if (_replicationPort > 0)	{
		if (!_replicationSocket.listen(_listenIp.c_str(), _replicationPort)) {
			log::Error::L("Can't listen to %s:%u for replication\n", _listenIp.c_str(), _replicationPort);
			return false;
		}
		log::Warning::L("Listen to %s:%u for replication\n", _listenIp.c_str(), _replicationPort);
	}
	return true;
}