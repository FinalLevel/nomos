#pragma once
#ifndef __FL_NOMOS_CONFIG_HPP
#define	__FL_NOMOS_CONFIG_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Nomos server's config class
///////////////////////////////////////////////////////////////////////////////

#include <boost/property_tree/ptree.hpp>

#include <string>
#include <vector>
#include "socket.hpp"
#include "types.hpp"

namespace fl {
	namespace nomos {
		using fl::network::Socket;
		
		const char * const DEFAULT_CONFIG = "./etc/nomos.cnf";
		const size_t MAX_BUF_SIZE = 300000;
		const size_t MAX_ITEM_SIZE = 300000;
		const size_t MAX_REPLICATION_BUFFER = MAX_BUF_SIZE + (MAX_ITEM_SIZE * 2);
		const size_t MAX_TOP_LEVEL_NAME_LENGTH = 16;
		const size_t MAX_FILE_SIZE = 64 * 1024 * 1024; // 100Mb
		
		const double MIN_SYNC_TOUCH_TIME_PERCENT = 0.1; // 10 percent
		const int MIN_SYNC_PUT_UPDATE_TIME = 5 * 60; // 5 minutes
		const int DEFAULT_SOCKET_TIMEOUT = 60;
		const uint32_t DEFAULT_CMD_PORT = 7007;
		const size_t DEFAULT_SOCKET_QUEUE_LENGTH = 10000;
		const size_t EPOLL_WORKER_STACK_SIZE = 100000;
		const size_t DEFAULT_WORKERS_COUNT = 2;
		
		const size_t DEFAULT_BUFFER_SIZE = 32000;
		const size_t DEFAULT_MAX_FREE_BUFFERS = 500;
		
		const uint32_t MAX_REPLICATION_FILE_SIZE = 1000000000; // 1GB
		
		class Config
		{
		public:
			Config(int argc, char *argv[]);
			const std::string &logPath() const
			{
				return _logPath;
			}
			int logLevel() const
			{
				return _logLevel;
			}
			typedef uint32_t TStatus;
			static const TStatus ST_LOG_STDOUT = 0x1;
			static const TStatus ST_AUTO_CREATE_TOP_LEVEL = 0x2;
			const bool isLogStdout() const
			{
				return _status & ST_LOG_STDOUT;
			}
			const bool isAutoCreate() const
			{
				return _status & ST_AUTO_CREATE_TOP_LEVEL;
			}
			const std::string &dataPath() const
			{
				return _dataPath;
			}
			const int cmdTimeout() const
			{
				return _cmdTimeout;
			}
			Socket &listenSocket()
			{
				return _listenSocket;
			}
			const size_t workerQueueLength() const
			{
				return _workerQueueLength;
			}
			const size_t workers() const
			{
				return _workers;
			}
			const size_t bufferSize() const
			{
				return _bufferSize;
			}
			const size_t maxFreeBuffers() const
			{
				return _maxFreeBuffers;
			}
			bool initNetwork();
			EKeyType defaultSublevelKeyType() const
			{
				return _defaultSublevelKeyType;
			}
			EKeyType defaultItemKeyType() const
			{
				return _defaultItemKeyType;
			}
			uint32_t syncThreadsCount() const
			{
				return _syncThreadsCount;
			}
			TServerID serverID() const
			{
				return _serverID;
			}
			uint32_t replicationLogKeepTime() const
			{
				return _replicationLogKeepTime;
			}
			const std::string &replicationLogPath() const
			{
				return _replicationLogPath;
			}
			
		private:
			void _parseNetworkParams(boost::property_tree::ptree &pt);
			void _parseIndexParams(boost::property_tree::ptree &pt);
			void _parseReplicationParams(boost::property_tree::ptree &pt);
			TStatus _status;
			std::string _logPath;
			int _logLevel;
			std::string _dataPath;
			std::string _listenIp;
			uint32_t _port;
			int _cmdTimeout;
			Socket _listenSocket;
			size_t _workerQueueLength;
			size_t _workers;
			
			size_t _bufferSize;
			size_t _maxFreeBuffers;
			
			EKeyType _defaultSublevelKeyType;
			EKeyType _defaultItemKeyType;
			
			uint32_t _syncThreadsCount;
			TServerID _serverID;
			std::string _replicationLogPath;
			uint32_t _replicationLogKeepTime;
			uint32_t _replicationPort;
			Socket _replicationSocket;
			TServerList _masters;
		};
	};
};

#endif	// __FL_NOMOS_CONFIG_HPP
