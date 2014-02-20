///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
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


using fl::network::Socket;
using namespace fl::nomos;

int main(int argc, char *argv[])
{
	std::unique_ptr<Config> config;
	try
	{
		config.reset(new Config(argc, argv));
		if (!log::NomosLogSystem::init(config.get()))
			return -1;
	}
	catch (...)	
	{
		return -1;
	}
	log::Info::L("Starting Nomos Storage server\n");
};
