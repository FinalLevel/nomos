#pragma once
#ifndef __FL_NOMOS_INDEX_SYNC_THREAD_HPP
#define	__FL_NOMOS_INDEX_SYNC_THREAD_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Index's data to disk synchronization thread class
///////////////////////////////////////////////////////////////////////////////

#include "types.hpp"
#include "cond_mutex.hpp"
#include "mutex.hpp"
#include "thread.hpp"

namespace fl {
	namespace nomos {
		using fl::threads::Mutex;
		using fl::threads::AutoMutex;

		class IndexSyncThread : public fl::threads::Thread
		{
		public:
			IndexSyncThread();
			virtual ~IndexSyncThread() {}
			void add(TTopLevelIndexPtr &topLevel);
		private:
			virtual void run();
			fl::threads::CondMutex _cond;
			Mutex _sync;
			typedef std::vector<TTopLevelIndexPtr> TTopLevelVector;
			TTopLevelVector _needSyncLevels;
		};
	};
};

#endif	// __FL_NOMOS_INDEX_SYNC_THREAD_HPP
