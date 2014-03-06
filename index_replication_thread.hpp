#pragma once
#ifndef __FL_NOMOS_INDEX_REPLICATION_THREAD_HPP
#define	__FL_NOMOS_INDEX_REPLICATION_THREAD_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Index's replication threads implementation
///////////////////////////////////////////////////////////////////////////////

#include "thread.hpp"
#include "socket.hpp"
#include "mutex.hpp"
#include "types.hpp"
#include <vector>

namespace fl {
	namespace nomos {
		using fl::network::Socket;
		using fl::network::TDescriptor;
		using fl::network::TIPv4;
		using fl::threads::Mutex;

		class ReplicationReceiverThread : public fl::threads::Thread
		{
		public:
			ReplicationReceiverThread(const TDescriptor descr, const TIPv4 ip,  class Index *index, 
				class ReplicationAcceptThread *acceptThread);
			virtual ~ReplicationReceiverThread();
			void setSocket(const TDescriptor descr, const TIPv4 ip);
		private:
			bool _doHandShake();
			bool _readPacket();
			virtual void run();
			Socket _socket;
			TIPv4 _ip;
			class Index *_index;
			class ReplicationAcceptThread *_acceptThread;
		};
		
		class ReplicationAcceptThread : public fl::threads::Thread
		{
		public:
			ReplicationAcceptThread(Socket *listenTo, class Index *index);
			virtual ~ReplicationAcceptThread() {};
			void stop();
			void addFreeReceiver(ReplicationReceiverThread *thread);
		private:
			virtual void run();
			Socket *_listenTo;
			class Index *_index;
			typedef std::vector<ReplicationReceiverThread*> TReceiverThreadVector;
			Mutex _sync;
			TReceiverThreadVector _freeReceiver;
			TReceiverThreadVector _receivers;
		};
	};
};

#endif	// __FL_NOMOS_INDEX_REPLICATION_THREAD_HPP
