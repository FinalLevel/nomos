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


#include <string>
#include "socket.hpp"

namespace fl {
	namespace nomos {
		using fl::network::Socket;
		
		const char * const DEFAULT_CONFIG = "./etc/nomos.cnf";
		const size_t MAX_BUF_SIZE = 300000;
		const size_t MAX_TOP_LEVEL_NAME_LENGTH = 16;
		const size_t MAX_FILE_SIZE = 64 * 1024 * 1024; // 100Mb
		
		const double MIN_SYNC_TOUCH_TIME_PERCENT = 0.1; // 10 percent
		const int DEFAULT_SOCKET_TIMEOUT = 60;
		const size_t DEFAULT_SOCKET_QUEUE_LENGTH = 10000;
		const size_t EPOLL_WORKER_STACK_SIZE = 100000;
		const size_t DEFAULT_WORKERS_COUNT = 2;
		
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
			const bool isLogStdout() const
			{
				return _status & ST_LOG_STDOUT;
			}
			const std::string &dataPath() const
			{
				return _dataPath;
			}
			const int cmdTimeout() const
			{
				return _cmdTimeout;
			}
			Socket &cmdListenSocket()
			{
				return _cmdListenSocket;
			}
			const size_t workerQueueLength() const
			{
				return _workerQueueLength;
			}
			const size_t workers() const
			{
				return _workers;
			}
		private:
			TStatus _status;
			std::string _logPath;
			int _logLevel;
			std::string _dataPath;
			int _cmdTimeout;
			Socket _cmdListenSocket;
			size_t _workerQueueLength;
			size_t _workers;
		};
	};
};

#endif	// __FL_NOMOS_CONFIG_HPP
