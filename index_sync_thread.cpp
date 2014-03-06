///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Index's data to disk synchronization thread class
///////////////////////////////////////////////////////////////////////////////

#include "index_sync_thread.hpp"
#include "index.hpp"
#include "nomos_log.hpp"

using namespace fl::nomos;

IndexSyncThread::IndexSyncThread()
{
	static const uint32_t SYNC_THREAD_STACK_SIZE = 100000;
	setStackSize(SYNC_THREAD_STACK_SIZE);
	if (!create())
	{
		log::Fatal::L("Can't create an index thread\n");
		throw std::exception();
	}
}

void IndexSyncThread::add(TTopLevelIndexPtr &topLevel)
{
	_sync.lock();
	_needSyncLevels.push_back(topLevel);
	_sync.unLock();
	_cond.sendSignal();
}

void IndexSyncThread::run()
{
	fl::chrono::Time curTime;
	Buffer buf(MAX_BUF_SIZE + 1);
	while (true)
	{
		TTopLevelVector workItems;
		_sync.lock();
		std::swap(_needSyncLevels, workItems);
		_sync.unLock();
		if (workItems.empty())
		{
			_cond.waitSignal();
			continue;
		}
		curTime.update();
		for (auto level = workItems.begin(); level != workItems.end(); level++) {
			if (!(*level)->sync(buf, curTime.unix(), false))	{
				_sync.lock();
				_needSyncLevels.push_back(*level);
				_sync.unLock();
			}
		}
	}
}

