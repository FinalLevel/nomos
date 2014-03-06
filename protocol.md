Nomos Storage communication protocol
=====
Copyright (c) 2014 Final Level
Author: Denys Misko <gdraal@gmail.com>
Distributed under BSD (3-Clause) License (See
accompanying file LICENSE)
=====

You're supposed to know Nomos's protocol only if you've decided to write your own library realization of protocol.
You can use one of the many existing libraries for the languages you prefer.

Nomos's protocol is a semi-binary protocol. 

Nomos's request syntax is: V[0-9][0-9],command,argument1,argument2 ... argumentN[\n]
First two digit represent a version number and it is equal	to "01" now.
Comma (',') is used as a delimiter, therefore the arguments should not have it inside.
the command is one char representing an operation which is needed.

The length of the answer is always 10 symbols. It can be:
1) "OK00000000\n" - normal result
2) "ERR0000000\n" - non-critical error result (the connection won't be closed)
3) "ERR_CR0000\n" - critical error result (the connection will be closed)

1. Create command (C)
This command creates a new top level of the index.
Arguments: level's name, sublevel's key type, item's key type.
Sublevel and item key types can be one of these values: INT32, INT64 or STRING.
Answer: OK00000000\n or ERR_CR0002\n
Example: This command creates a new top level "level1" which has 32Bit integer sublevel's key type and 
string item's key type.
V01,C,level1,INT32,STRING\n
OK00000000\n

2. Put command (P) and Update command (U)
These commands have the same syntax and both replace an item to the storage.
However update command check an original item before replacing a new value and if both of them have identical data 
only the original item's header will be updated.
Arguments: level's name, sublevel key, item key, lifetime (in seconds, 0 for persistent items), 
item's size and the item data after "\n"
Answer: OK00000000\n or ERR0000003\n
Example:
V01,P,level1,1,someItemKey,3600,10\n
1234567890
OK00000000\n
This request replace an item which has level=level1, sublevel=1 and item's key=someItemKey, item's data is "1234567890"

Example:
V01,U,level1,1,someItemKey,3600,10\n
1234567890
OK00000000\n
This request replace an item which has level=level1, sublevel=1 and item's key=someItemKey, item's data is "1234567890"
But if the same value had been put into the storage before it would only update lifetime of the previous item.

3. Get command (G)
This command receives an item from the storage.
Arguments: level's name, sublevel key, item key,lifetime (in seconds, 0 - don't change it).
Answer: OKXXXXXXXX\n + data - where XXXXXXXX it is size of an item in a hex representation.
Or ERR0000004\n in an error situation
Example: V01,G,level1,1,someItemKey,0\n
If there is an item of 10 bytes length at level 'level1' and sublevel '1' the answer would be:
OK0000000a
1234567890
and the item's life time would not be changed

Example: V01,G,level1,1,someItemKey,10\n
If there is an item of 10 bytes length at level 'level1' and sublevel '1' the answer would be:
OK0000000a
1234567890
and the item's life time would be changed to ten seconds

4. Touch command (T)
This command only change lifetime of an item
Arguments: level's name, sublevel key, item key,lifetime (in seconds, 0 for persistent items).
Answer: OK00000000\n or ERR0000004\n
Example: this command changes lifetime of an item to 3600 seconds from current moment of time.
V01,T,level1,1,someItemKey,3600\n
OK00000000\n

5. Remove command (R)
This command remove an item from the storage
Arguments: level's name, sublevel key, item key
Answer: OK00000000\n or ERR0000004\n
Example: this command removes an item at level=level1,sublevel=1 with key=someItemKey from the storage.
V01,D,level1,1,someItemKey\n
OK00000000\n
