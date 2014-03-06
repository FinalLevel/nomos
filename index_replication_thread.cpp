///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Index's replication threads implementation
///////////////////////////////////////////////////////////////////////////////

#include "index_replication_thread.hpp"
#include "index.hpp"
#include "nomos_log.hpp"

using namespace fl::nomos;
using namespace fl::network;


ReplicationAcceptThread::ReplicationAcceptThread(Socket *listenTo, class Index *index)
	: _listenTo(listenTo), _index(index)
{
	static const uint32_t REPLICATION_THREAD_STACK_SIZE = 100000;
	setStackSize(REPLICATION_THREAD_STACK_SIZE);
	if (!create()) {
		log::Fatal::L("Can't create an index thread\n");
		throw std::exception();
	}
}

void ReplicationAcceptThread::stop()
{
	cancel();
	for (auto thread = _receivers.begin(); thread != _receivers.end(); thread++) {
		(*thread)->cancel();
		(*thread)->waitMe();
	}
}

void ReplicationAcceptThread::addFreeReceiver(ReplicationReceiverThread *thread)
{
	_sync.lock();
	_freeReceiver.push_back(thread);
	_sync.unLock();
}

void ReplicationAcceptThread::run()
{
	while (1) {
		TIPv4 ip;
		auto clientDescr = _listenTo->acceptDescriptor(ip);
		if (clientDescr == INVALID_SOCKET) {
			log::Error::L("Cannection accept error\n");
			continue;
		};
		if (!Socket::setNonBlockIO(clientDescr)) {
			log::Error::L("AcceptThread cannot setNonBlockIO\n");
			close(clientDescr);
			continue;
		}
		AutoMutex autoSync(&_sync);
		if (_freeReceiver.empty())
			_receivers.push_back(new ReplicationReceiverThread(clientDescr, ip, _index, this));
		else {
			_freeReceiver.back()->setSocket(clientDescr, ip);
			_freeReceiver.pop_back();
		}
	}
}

struct ReplicationHandshake
{
	uint8_t version;
	TServerID fromServerID;
};

ReplicationReceiverThread::ReplicationReceiverThread(const TDescriptor descr, const TIPv4 ip, class Index *index,
	class ReplicationAcceptThread *acceptThread)
	: _socket(descr), _ip(ip), _index(index), _acceptThread(acceptThread)
{
	static const uint32_t REPLICATION_THREAD_STACK_SIZE = 100000;
	setStackSize(REPLICATION_THREAD_STACK_SIZE);
	if (!create()) {
		log::Fatal::L("Can't create an index thread\n");
		throw std::exception();
	}
}

void ReplicationReceiverThread::setSocket(const TDescriptor descr, const TIPv4 ip)
{
	_socket.reset(descr);
	_ip = ip;
}

ReplicationReceiverThread::~ReplicationReceiverThread()
{
}

bool ReplicationReceiverThread::_doHandShake()
{
	ReplicationHandshake rh;
	if (!_socket.pollAndRecvAll(&rh, sizeof(rh)))
	{
		log::Fatal::L("Can't receive handshake from %s\n", Socket::ip2String(_ip).c_str());
		return false;
	}
	
	return false;
}

bool ReplicationReceiverThread::_readPacket()
{
	return false;
}

void ReplicationReceiverThread::run()
{
	bool added = false;
	while (true) {
		if (_socket.descr() == INVALID_SOCKET)
		{
			if (!added)
			{
				_acceptThread->addFreeReceiver(this);
				added = true;
			}
			sleep(1);
			continue;
		}
		added = false;
		log::Info::L("Receive connection from %s\n", Socket::ip2String(_ip).c_str());
		if (!_doHandShake())
		{
			_socket.reset(INVALID_SOCKET);
			continue;
		}
		while (_readPacket())
		{
			
		}
		_socket.reset(INVALID_SOCKET);
	}
};


