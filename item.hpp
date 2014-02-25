#pragma once
#ifndef __FL_NOMOS_ITEM_HPP
#define	__FL_NOMOS_ITEM_HPP

///////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2014 Final Level
// Author: Denys Misko <gdraal@gmail.com>
// Distributed under BSD (3-Clause) License (See
// accompanying file LICENSE)
//
// Description: Nomos storage data item classes
///////////////////////////////////////////////////////////////////////////////

#include <cstdint>
#include <memory>

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
			void setTag(const TTime curTime);
		};
		
		class Item
		{
		public:
			Item();
			Item(const char *data, const ItemHeader::TSize size, const ItemHeader::TTime liveTo, const ItemHeader::TTime curTime);
			~Item();
			
			Item(const Item &item) = delete;
			Item(Item &&item)
				: _header(item._header), _data(item._data)
			{
				item._data = NULL;
				item._header.size = 0;
			}
			const bool isValid(const ItemHeader::TTime curTime)
			{
				if (_header.liveTo)
				{
					return (_header.liveTo > curTime);
				}
				else
					return true;
			}
			void setDeleted()
			{
				_header.liveTo = 1;
			}
			void setLiveTo(const ItemHeader::TTime setTime, const ItemHeader::TTime curTime)
			{
				_header.liveTo = setTime;
				_header.setTag(curTime);
			}
			const ItemHeader &header() const
			{
				return _header;
			}
			typedef void *TMemPtr;
			TMemPtr data()
			{
				return _data;
			}
			const ItemHeader::TSize size() const
			{
				return _header.size;
			}
		private:
			ItemHeader _header;
			TMemPtr _data;
		};
		typedef std::shared_ptr<Item> TItemSharedPtr;
	};
};

#endif	// __FL_NOMOS_ITEM_HPP
