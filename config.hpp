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

namespace fl {
	namespace nomos {
		
		const char * const DEFAULT_CONFIG = "./etc/nomos.cnf";
		
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
		private:
			TStatus _status;
			std::string _logPath;
			int _logLevel;
		};
	};
};

#endif	// __FL_NOMOS_CONFIG_HPP
