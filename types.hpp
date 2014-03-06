#pragma once
#ifndef __FL_NOMOS_TYPES_HPP
#define	__FL_NOMOS_TYPES_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Nomos server's iteranl types definition
///////////////////////////////////////////////////////////////////////////////

#include <cstdint>
#include <string>
#include <vector>
#include <memory>

namespace fl {
	namespace nomos {
		enum EKeyType : uint8_t
		{
			KEY_STRING,
			KEY_INT32,
			KEY_INT64,
			KEY_MAX_TYPE = KEY_INT64 // should always be equal max key type
		};
		typedef uint32_t TServerID;
		
		struct Server
		{
			std::string listenIp;
			uint32_t port;
		};
		typedef std::vector<Server> TServerList;
		
		typedef uint32_t TReplicationLogNumber;

		typedef std::shared_ptr<class TopLevelIndex> TTopLevelIndexPtr;
	};
};

#endif	// __FL_NOMOS_TYPES_HPP
