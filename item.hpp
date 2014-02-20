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
			
		private:
			ItemHeader _header;
			
			typedef void *TMemPtr;
			TMemPtr _data;
		};
	};
};

#endif	// __FL_NOMOS_ITEM_HPP
