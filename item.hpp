#pragma once
#ifndef __FL_NOMOS_ITEM_HPP
#define	__FL_NOMOS_ITEM_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright Denys Misko <gdraal@gmail.com>, Final Level, 2014.
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Nomos storage data item classes
///////////////////////////////////////////////////////////////////////////////

#include <cstdint>

namespace fl {
	namespace nomos {
		
		struct ItemHeader
		{
			typedef u_int32_t TTime;
			TTime liveTo;
			typedef u_int32_t TSize;
			TSize size;
			union UTag
			{
				struct
				{
					u_int32_t _opNumber;
					u_int32_t _time;
				};
				u_int64_t tag;
			};
			UTag timeTag;
		};
		
		class Item
		{
		public:
			Item();
			~Item();
			
			Item(const Item &item) = delete;
			Item(Item &&item)
				: _header(item._header), _data(item._data)
			{
				item._data = NULL;
				item._header.size = 0;
			}
		private:
			typedef void *TMemPtr;
			ItemHeader _header;
			TMemPtr _data;
		};
	};
};

#endif	// __FL_NOMOS_ITEM_HPP
