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
#include "fl_libs/network_buffer.hpp"

namespace fl {
	namespace nomos {
		using namespace fl::events;
		
		class NomosEvent : public WorkEvent
		{
		public:
			enum ENomosState : u_int8_t
			{
				ER_PARSE = 1,
				ER_NOT_READY,			
				ER_UNKNOWN,
				ST_WAIT_QUERY,
				ST_WAIT_DATA,
				ST_FORM_ANSWER,
				ST_SEND,
				ST_SEND_AND_CLOSE,
				ST_FINISHED
			};
			enum ENomosCMD : char
			{
				CMD_GET = 'G',
				CMD_PUT = 'P',
				CMD_TOUCH = 'T',
			};

			NomosEvent(const TEventDescriptor descr, const time_t timeOutTime);
			virtual ~NomosEvent();
			virtual const ECallResult call(const TEvents events);
			static void setInited(const bool inited = true);
			static void setConfig(Config *config);
		private:
			static Config *_config;
			void _endWork();
			bool _reset();
			bool _readQuery();
			bool _readQueryData();
			bool _formAnswer();
			bool _parseQuery();
			bool _parsePutQuery(NetworkBuffer::TDataPtr &query);
			void _setWaitRead();
			void _setWaitSend();
			
			ECallResult _sendError();
			ECallResult _sendAnswer();
			static bool _inited;
			NetworkBuffer *_networkBuffer;
			ENomosState _curState;
			ENomosCMD _cmd;
			uint16_t _querySize;
			std::string _level;
			std::string _subLevel;
			std::string _itemKey;
			time_t _lifeTime;
			uint32_t _itemSize;
		};
		
		
		class NomosThreadSpecificData : public ThreadSpecificData
		{
		public:
			NomosThreadSpecificData(Config *config);
			virtual ~NomosThreadSpecificData() {}
			NetworkBufferPool bufferPool;
		};
		
		class NomosThreadSpecificDataFactory : public ThreadSpecificDataFactory
		{
		public:
			NomosThreadSpecificDataFactory(Config *config);
			virtual ThreadSpecificData *create();
			virtual ~NomosThreadSpecificDataFactory() {};
		private:
			Config *_config;
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