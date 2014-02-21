///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Nomos storage data item classes
///////////////////////////////////////////////////////////////////////////////

#include <cstring>
#include <cstdlib>

#include "item.hpp"

using namespace fl::nomos;

Item::Item()
	: _data(NULL)
{
	bzero(&_header, 0);
}
	
Item::~Item()
{
	free(_data);
}

