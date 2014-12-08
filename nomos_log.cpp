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


int NomosLogSystem::_logLevel = 0;

bool NomosLogSystem::log(
	const size_t target, 
	const int level, 
	const time_t curTime, 
	struct tm *ct, 
	const char *fmt, 
	va_list args
)
{
	if (level <= _logLevel)
		return LogSystem::defaultLog().log(target, level, "nomos", curTime, ct, fmt, args);
	else
		return false;
}

bool NomosLogSystem::init(fl::nomos::Config *config)
{
	LogSystem::defaultLog().clearTargets();
	_logLevel = config->logLevel();
	if (!config->logPath().empty())
		LogSystem::defaultLog().addTarget(new fl::log::FileTarget(config->logPath().c_str()));
	if (config->isLogStdout())
		LogSystem::defaultLog().addTarget(new fl::log::ScreenTarget());
	return true;
}
