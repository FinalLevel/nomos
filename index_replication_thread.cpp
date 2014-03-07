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
#include "time.hpp"

using namespace fl::nomos;
using namespace fl::network;
using fl::chrono::Time;


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
			log::Error::L("Connection accept error\n");
			sleep(1);
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

enum ECMDPacket : uint8_t
{
	CMD_READ_BIN_LOG = 1,
};

struct SenderHandshakeRequest
{
	static const uint8_t CURRENT_VERSION = 1;
	SenderHandshakeRequest()
		: version(CURRENT_VERSION)
	{
	}
	uint8_t version;
	ECMDPacket cmd;
	TServerID fromServerID;
};

bool ReplicationReceiverThread::_doHandShake()
{
	SenderHandshakeRequest shr;
	if (!_socket.pollAndRecvAll(&shr, sizeof(shr)))
	{
		log::Fatal::L("Can't receive handshake from %s\n", Socket::ip2String(_ip).c_str());
		return false;
	}
	_toServer = shr.fromServerID;
	TServerID serverID = _index->serverID();
	if (serverID == _toServer)
	{
		log::Fatal::L("Self connection detected %u\n", serverID);
		return false;
	}
	if (!_socket.pollAndSendAll(&serverID, sizeof(serverID)))
	{
		log::Fatal::L("Can't send handshake from %u to %s\n", _index->serverID(), Socket::ip2String(_ip).c_str());
		return false;		
	}
	return true;
}

bool ReplicationReceiverThread::_recvPacket()
{
	if (!_socket.pollAndRecvAll(&_readBinLogPacket, sizeof(_readBinLogPacket)))
	{
		log::Fatal::L("Can't receive packet from %s\n", Socket::ip2String(_ip).c_str());
		return false;
	}
	return true;
}

bool ReplicationReceiverThread::_getReplicationPacket()
{
	_data.clear();
	ReadBinLogAnswer &ra = *(ReadBinLogAnswer*)_data.reserveBuffer(sizeof(ra));

	static const int MAX_WAIT_TRIES = 10; // tries 50ms each
	for (int tries = 0; tries < MAX_WAIT_TRIES; tries++)
	{
		if (!_index->getFromReplicationLog(_toServer, _data, _buffer, _readBinLogPacket.number, _readBinLogPacket.seek))
		{
			log::Info::L("Can't read bin log for %u (%u-%u)\n", _toServer, 
				_readBinLogPacket.number, _readBinLogPacket.seek);
			return false;
		}
		if (_data.writtenSize() > sizeof(ra))
			break;
		else
		{
			struct timespec tim;
			tim.tv_sec = 0;
			static const int RECHECK_TIME = 50000000; // 50 ms
			tim.tv_nsec = RECHECK_TIME;
			nanosleep(&tim , NULL);
		}
	}
	ra.number = _readBinLogPacket.number;
	ra.seek = _readBinLogPacket.seek;
	ra.size = _data.writtenSize() - sizeof(ra);
	if (!_socket.pollAndSendAll(_data.begin(), _data.writtenSize()))
	{
		log::Fatal::L("Can't send replication packet from %u to %u\n", _index->serverID(), _toServer);
		return false;
	}
	return true;
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
		while (_recvPacket())
		{
			if (!_getReplicationPacket())
				break;
		}
		_socket.reset(INVALID_SOCKET);
	}
};


ReplicationActiveThread::ReplicationActiveThread(TIPv4 hostIp, const uint32_t hostPort, Index* index)
	: _hostIp(hostIp), _hostPort(hostPort), _fromServer(0), _index(index)
{
	static const uint32_t REPLICATION_THREAD_STACK_SIZE = 100000;
	setStackSize(REPLICATION_THREAD_STACK_SIZE);
	if (!create()) {
		log::Fatal::L("Can't create an index thread\n");
		throw std::exception();
	}
}

bool ReplicationActiveThread::_saveReplicationInfo()
{
	_infoData.sprintfSet("%u-%u\n", _readBinLogPacket.number, _readBinLogPacket.seek);
	_fd.seek(0, SEEK_SET);
	if (!_fd.write(_infoData.c_str(), _infoData.size()) != _infoData.size())
	{
		log::Error::L("Can't write replication info for server %u\n", _fromServer);
		return false;
	}
	return true;
}

bool ReplicationActiveThread::_openReplicationInfo()
{
	if (_fd.descr())
		return true;
	BString fileName;
	fileName.sprintfSet("%s/nomos_repl_info_%u", _index->replicationLogPath().c_str(), _fromServer);
	if (!_fd.open(fileName.c_str(), O_CREAT | O_RDWR))
	{
		log::Error::L("Can't open replication info file %s\n", fileName.c_str());
		return false;
	}
	bzero(&_readBinLogPacket, sizeof(_readBinLogPacket));
	auto fileSize = _fd.fileSize();
	if (fileSize)
	{
		BString data(fileSize + 1);
		if (_fd.read(data.reserveBuffer(fileSize), fileSize) != fileSize)
		{
			log::Error::L("Can't read replication info file %s\n", fileName.c_str());
			return false;
		}
		char *endP;
		_readBinLogPacket.number = strtoul(data.c_str(), &endP, 10);
		if (*endP != '-')
		{
			log::Error::L("Bad replication number delimiter %c in  %s\n", *endP, fileName.c_str());
			return false;
		}
		_readBinLogPacket.seek = strtoul(endP + 1, NULL, 10);
	}
	return true;
}

bool ReplicationActiveThread::_handshake()
{
	SenderHandshakeRequest shr;
	shr.cmd = CMD_READ_BIN_LOG;
	shr.fromServerID = _index->serverID();
	if (!_socket.pollAndSendAll(&shr, sizeof(shr))) {
		log::Error::L("Can't send handshake packet\n");
		return false;
	}
	TServerID fromServer;
	if (!_socket.pollAndRecvAll(&fromServer, sizeof(fromServer))) {
		log::Error::L("Can't send handshake packet\n");
		return false;
	}
	if (fromServer != _fromServer)
		_fd.close();
	if (_openReplicationInfo())
	{
		log::Info::L("Start synchronization from %u/%u from serverID %u\n", _readBinLogPacket.number, 
			_readBinLogPacket.seek);
		return true;
	}
	else {
		_fd.close();
		return false;
	}
}

bool ReplicationActiveThread::_getReplicationPacket()
{
	if (!_socket.pollAndSendAll(&_readBinLogPacket, sizeof(_readBinLogPacket))) {
		log::Error::L("Can't send readBinLogPacket request\n");
		return false;
	}
	ReadBinLogAnswer rba;
	static const int LONG_WAIT_ANSWER = 60 * 1000; // wait 1 minute
	if (!_socket.pollAndRecvAll(&rba, sizeof(rba), LONG_WAIT_ANSWER)) {
		log::Error::L("Can't read ReadBinLogAnswer\n");
		return false;
	}
	if (rba.size > 0)
	{
		_data.clear();
		if (!_socket.pollAndRecvAll(_data.reserveBuffer(rba.size), rba.size)) {
			log::Error::L("Can't read ReadBinLogAnswer data\n");
			return false;
		}
		Time curTime;
		if (!_index->addFromAnotherServer(_fromServer, _data, curTime.unix(), _buffer))
		{
			log::Error::L("Can't add data from %u\n", _fromServer);
			return false;
		}
		_readBinLogPacket.number  = rba.number;
		_readBinLogPacket.seek = rba.seek;
		if (!_saveReplicationInfo())
		{
			log::Error::L("Can't saveReplicationInfo for %u\n", _fromServer);
			return false;
		}
	}
	return true;
}

void ReplicationActiveThread::run()
{
	while (true) {
		if (!_socket.reopen()) {
			log::Error::L("ReplicationActiveThread can't create socket\n");
			sleep(1);
			continue;
		}
		
		static const int MAX_CONNECT_TIMEOUT = 1000 * 2; // 2 seconds
		if (!_socket.connect(_hostIp, _hostPort, MAX_CONNECT_TIMEOUT)) {
			log::Error::L("ReplicationActiveThread can't connect to %s:%u\n", Socket::ip2String(_hostIp).c_str(), _hostPort);
			sleep(1);
			continue;
		}
		if (!_handshake()) {
			log::Error::L("ReplicationActiveThread can't handshake with %s:%u\n", 
				Socket::ip2String(_hostIp).c_str(), _hostPort);
			sleep(1);
			continue;
		}
		while (_getReplicationPacket())
			;
	}
}

