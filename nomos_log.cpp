///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Nomos log system customization class
///////////////////////////////////////////////////////////////////////////////

#include "nomos_log.hpp"
using namespace fl::nomos::log;

NomosLogSystem NomosLogSystem::_logSystem;

NomosLogSystem::NomosLogSystem()
	: LogSystem("nomos"), _logLevel(MAX_LOG_LEVEL)
{
	_logSystem.addTarget(new fl::log::ScreenTarget());
}

bool NomosLogSystem::log(
	const size_t target, 
	const int level, 
	const time_t curTime, 
	struct tm *ct, 
	const char *fmt, 
	va_list args
)
{
	if (level <= _logSystem._logLevel)
		return _logSystem._log(target, level, curTime, ct, fmt, args);
	else
		return false;
}

bool NomosLogSystem::init(fl::nomos::Config *config)
{
	_logSystem.clearTargets();
	_logSystem._logLevel = config->logLevel();
	if (!config->logPath().empty())
		_logSystem.addTarget(new fl::log::FileTarget(config->logPath().c_str()));
	if (config->isLogStdout())
		_logSystem.addTarget(new fl::log::ScreenTarget());
	return true;
}
