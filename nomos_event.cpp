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

using namespace fl::nomos;

NomosEvent::NomosEvent(const TEventDescriptor descr, const time_t timeOutTime)
	: WorkEvent(descr, timeOutTime)
{
	
}

const NomosEvent::ECallResult NomosEvent::call(const TEvents events)
{
	return FINISHED;
}

ThreadSpecificData *NomosThreadSpecificDataFactory::create()
{
	return new NomosThreadSpecificData();
}

NomosEventFactory::NomosEventFactory(Config *config)
	: _config(config)
{
}

WorkEvent *NomosEventFactory::create(const TEventDescriptor descr, const time_t timeOutTime, Socket* acceptSocket)
{
	return new NomosEvent(descr, _config->cmdTimeout());
}
