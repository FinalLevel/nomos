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
#include "buffer.hpp"
#include "bstring.hpp"
#include "file.hpp"
#include <vector>

namespace fl {
	namespace nomos {
		using fl::network::Socket;
		using fl::network::TDescriptor;
		using fl::network::TIPv4;
		using fl::threads::Mutex;
		using fl::utils::Buffer;
		using fl::strings::BString;

		struct ReadBinLogRequest
		{
			TReplicationLogNumber number;
			uint32_t seek;
		};
		struct ReadBinLogAnswer
		{
			TReplicationLogNumber number;
			uint32_t seek;
			uint32_t size;
		};
		
		class ReplicationActiveThread : public fl::threads::Thread
		{
		public:
			ReplicationActiveThread(TIPv4 hostIp, const uint32_t hostPort, class Index *index);
			virtual ~ReplicationActiveThread() {};
		private:
			bool _handshake();
			bool _openReplicationInfo();
			bool _saveReplicationInfo();
			bool _getReplicationPacket();
			virtual void run();
			TIPv4 _hostIp; 
			uint32_t _hostPort;
			TServerID _fromServer;
			TReplicationLogNumber _startNumber;
			uint32_t _startSeek;
			ReadBinLogRequest _readBinLogPacket;
			Socket _socket;
			class Index *_index;
			Buffer _buffer;
			Buffer _data;
			BString _infoData;
			fl::fs::File _fd;
		};
		
		class ReplicationReceiverThread : public fl::threads::Thread
		{
		public:
			ReplicationReceiverThread(const TDescriptor descr, const TIPv4 ip,  class Index *index, 
				class ReplicationAcceptThread *acceptThread);
			virtual ~ReplicationReceiverThread();
			void setSocket(const TDescriptor descr, const TIPv4 ip);
		private:
			bool _doHandShake();
			bool _recvPacket();
			bool _getReplicationPacket();
			virtual void run();
			Socket _socket;
			TIPv4 _ip;
			class Index *_index;
			class ReplicationAcceptThread *_acceptThread;
			TServerID _toServer;
			ReadBinLogRequest _readBinLogPacket;
			Buffer _buffer;
			Buffer _data;
		};
		
		class ReplicationAcceptThread : public fl::threads::Thread
		{
		public:
			ReplicationAcceptThread(Socket *listenTo, class Index *index);
			virtual ~ReplicationAcceptThread();
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
