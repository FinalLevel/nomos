///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Nomos Storage is a key-value, persistent and high available server, 
// which is simple but extremely fast 
///////////////////////////////////////////////////////////////////////////////


#include <memory>
#include "socket.hpp"
#include "config.hpp"
#include "nomos_log.hpp"
#include "index.hpp"
#include "time.hpp"
#include "accept_thread.hpp"
#include "nomos_event.hpp"


using fl::network::Socket;
using fl::chrono::Time;
using namespace fl::nomos;
using namespace fl::events;

int main(int argc, char *argv[])
{
	std::unique_ptr<Config> config;
	std::unique_ptr<Index> index;
	std::unique_ptr<EPollWorkerGroup> workerGroup;
	try
	{
		config.reset(new Config(argc, argv));
		if (!log::NomosLogSystem::init(config.get()))
			return -1;
		
		log::Warning::L("Starting Nomos Storage server\n");
		if (!config->initNetwork())
			return -1;

		NomosEventFactory *factory = new NomosEventFactory(config.get());
		NomosThreadSpecificDataFactory *dataFactory = new NomosThreadSpecificDataFactory(config.get());
		workerGroup.reset(new EPollWorkerGroup(dataFactory, config->workers(), config->workerQueueLength(), 
			EPOLL_WORKER_STACK_SIZE));
		AcceptThread cmdThread(workerGroup.get(), &config->listenSocket(), factory);
		
		index.reset(new Index(config->dataPath()));
		Time curTime;
		if (!index->load(curTime.unix()))
			return -1;
		index->setAutoCreate(config->isAutoCreate(), config->defaultSublevelKeyType(), config->defaultItemKeyType());
		index->startThreads(config->syncThreadsCount());
		
		NomosEvent::setInited(index.get());
		workerGroup->waitThreads();
	}
	catch (...)	
	{
		return -1;
	}
	return 0;
};
