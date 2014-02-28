#pragma once
#ifndef __FL_NOMOS_EVENT_HPP
#define	__FL_NOMOS_EVENT_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Nomos event system classes
///////////////////////////////////////////////////////////////////////////////

#include "event_thread.hpp"
#include "config.hpp"

namespace fl {
	namespace nomos {
		using namespace fl::events;
		
		class NomosEvent : public WorkEvent
		{
		public:
			NomosEvent(const TEventDescriptor descr, const time_t timeOutTime);
			virtual const ECallResult call(const TEvents events);
		private:
		};
		
		
		class NomosThreadSpecificData : public ThreadSpecificData
		{
		public:
		private:
		};
		
		class NomosThreadSpecificDataFactory : public ThreadSpecificDataFactory
		{
		public:
			virtual ThreadSpecificData *create();
			virtual ~NomosThreadSpecificDataFactory() {};
		};

		class NomosEventFactory : public WorkEventFactory 
		{
		public:
			NomosEventFactory(Config *config);
			virtual WorkEvent *create(const TEventDescriptor descr, const time_t timeOutTime, Socket *acceptSocket);
			virtual ~NomosEventFactory() {};
		private:
			Config *_config;
		};
		
	};
};

#endif	// __FL_NOMOS_EVENT_HPP