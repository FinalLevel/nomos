///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
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
	bzero(&_header, sizeof(_header));
}
	
Item::~Item()
{
	free(_data);
}

void ItemHeader::setTag(const TTime curTime)
{
	static decltype(timeTag._opNumber) opNumber;
	timeTag._time = curTime;
	timeTag._opNumber = __sync_add_and_fetch(&opNumber, 1);
}

