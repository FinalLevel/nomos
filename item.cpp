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
#include "nomos_log.hpp"

using namespace fl::nomos;

Item::Item()
	: _data(NULL)
{
	bzero(&_header, sizeof(_header));
}

Item::Item(const char *data, const ItemHeader &header)
	: _header(header), _data(malloc(_header.size))
{
	if (!_data)
	{
		log::Fatal::L("Can't allocate data for item\n");
		throw std::bad_alloc();
	}
	memcpy(_data, data, _header.size);
}

Item::Item(const char *data, const ItemHeader::TSize size, const ItemHeader::TTime liveTo, const ItemHeader::TTime curTime)
	: _data(malloc(size))
{
	if (!_data)
	{
		log::Fatal::L("Can't allocate data for item\n");
		throw std::bad_alloc();
	}
	memcpy(_data, data, size);
	bzero(&_header, sizeof(_header));
	_header.size = size;
	_header.liveTo = liveTo;
	_header.setTag(curTime);
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

