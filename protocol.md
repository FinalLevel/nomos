## Nomos Storage communication protocol
    This document describes Nomos Storage communication protocol syntax.
    You're supposed to know Nomos protocol only if you've decided to write your own library realization of protocol.
    You can use one of the many existing libraries for the languages you prefer.

***
### Copyrights

(c) 2014 Final Level, Denys Misko <gdraal@gmail.com>

Distributed under `BSD (3-Clause) License` (See accompanying file LICENSE)

***
### Protocol general description

Nomos protocol is a semi-binary protocol. 

Nomos request syntax is: `V[0-9][0-9],command,argument1,argument2 ... argumentN[\n]`
* First two digit represent a version number and it is equal to `01` now.
* Comma (`,`) is used as a delimiter, therefore the arguments should not have it inside.
* `command` is one char representing an operation which is needed.
* Request finished with `\n` line ending char.

The length of the answer is always 10 symbols. It can be:
* `OK00000000\n` - normal result
* `ERR0000000\n` - non-critical error result (the connection won't be closed)
* `ERR_CR0000\n` - critical error result (the connection will be closed)

You can send any number of commands in one connection until you receive critical error result.

***
### 1. Create command (`C`)

**Description**: This command creates a new top level of the index.

**Command char:** `C`

**Arguments:** `level name`, `sublevel key type`, `item key type`. 

**Argument values:** Sublevel and item key types can be one of these values: `INT32`, `INT64` or `STRING`.

**Answers:** `OK00000000\n` or `ERR_CR0002\n`

**Example request:**  This command creates a new top level "level1" which has 32Bit integer sublevel key type and 
string item key type.

    V01,C,level1,INT32,STRING\n

**Example answer:** `OK00000000\n`

***
### 2. Put command (`P`) and Update command (`U`)

**Description**: Both commands have the same syntax and both replace an item to the storage.
However `update command` check an original item before replacing a new value and if both of them have identical data 
only the original item's header will be updated.

**Command chars:** `P` or `U`

**Arguments:** `level name`, `sublevel key`, `item key`, `lifetime`, `item size` and `item data` after `\n`

**Lifetime:** The period of time in seconds or `0 ` for the persistent items.

**Answers:** `OK00000000\n` or `ERR0000003\n`

**Put example request:** This request replace an item which has level=level1, sublevel=1 and item key=someItemKey, 
the item data is "1234567890".
```
V01,P,level1,1,someItemKey,3600,10\n
1234567890
```
**Example answer:** `OK00000000\n`

**Update example request:** This request replace an item which has level=level1, sublevel=1 and item key=someItemKey, 
the item data is "1234567890", but if the same value had been put into the storage before it would only update lifetime 
of the previous item.
```
V01,U,level1,1,someItemKey,3600,10\n
1234567890
```
**Example answer:** `OK00000000\n`

***
### 3. Get command (`G`)

**Description**: This command receives an item from the storage.

**Command char:** `G`

**Arguments:** `level name`,`sublevel key`,`item key`,`new lifetime`

**Lifetime:** The amount of additional time or `0` - don't change the item lifetime.

**Answers:** `OKXXXXXXXX\n` + `data` - where `XXXXXXXX` it is size of an item in a hex representation. 
or `ERR0000004\n` in an error situation.

**Example request:** 
    
    V01,G,level1,1,someItemKey,0\n

**Example answer:** If there is an item of 10 bytes length at level 'level1' and sublevel '1' the answer would be:
```
OK0000000a
1234567890
```
and the item life time won't be changed

**Example request:** 
    
    V01,G,level1,1,someItemKey,10\n

**Example answer:** If there is an item of 10 bytes length at level `level1` and sublevel `1` the answer will be:
```
OK0000000a
1234567890
```
and ten seconds will be added to the item life time.

***
### 4. Touch command (`T`)


**Description:** This command only change lifetime of an item

**Command char:** `T`

**Arguments:** `level name`, `sublevel key`, `item key`,`lifetime`

**Lifetime:** The period of time in seconds or `0 ` for the persistent items.

**Answers:** `OK00000000\n` or `ERR0000004\n`

**Example request:** this request changes lifetime of an item to 3600 seconds from current moment of time.
    
    V01,T,level1,1,someItemKey,3600\n
    
**Example answer:** 

    OK00000000\n


***
### 5. Remove command (`R`)


**Description:** This command remove an item from the storage

**Command char:** `R`

**Arguments:** `level name`, `sublevel key`, `item key`

**Answers:** `OK00000000\n` or `ERR0000004\n`

**Example request:** this command removes an item at `level=level1`, `sublevel=1` with `key=someItemKey` 
from the storage.
    
    V01,D,level1,1,someItemKey\n
    
**Example answer:**     

    OK00000000\n