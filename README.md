# Nomos Storage

Nomos Storage is an in-memory, key-value, persistent and high available server, which is simple but extremely fast.

***
## Features

* Persistent storage (almost at real time the data is been coping on disk)
* Written in C++
* No disk locks dependencies (standalone disk writer threads)
* Integrated server side replication system
* EPoll asynchronous event model
* There are PHP, python and other language libraries available
* _Memcache protocol support (will be added soon)_

***
## Most common usages

* Sessions storage system (there are various  libs for different languages and frameworks)
* Database cache
* Site content cache

***
## Installation from git sources
```bash
git clone https://github.com/FinalLevel/nomos.git
cd nomos
sh ./autogen.sh
./configure --prefix=/usr --sysconfdir=/etc
make
make check
make install
```

***
## Configuration

By default the configuration file is sought at `$(sysconfdir)/nonoms.cnf`, but you can overwrite this by putting 
`-c configFileName` into the start parameters.

**nomos.cnf example:**

```bash
[nomos-server]
; log file name
log=/var/log/nomos
; log suppression level (0 - now logging; 1 - log only fatals; 2 - log only fatals and errors;
; 3 - log only errors, fatals and warnings; 4 - log everything)
logLevel=4
; duplicate or not log on stdout
logStdout=on

; main data dir
dataPath=/var/lib/nomos/data

; server ip
listen=127.0.0.1
; server port
port=7007

; create new top level when a put command comes
autoCreateTopIndex=off
; Types can be INT32, INT64 or STRING
defaultSublevelKeyType=INT32
defaultItemKeyType=INT64

; Disk writing threads number
syncThreadsCount=3

; sever unique ID
serverID=1

; if replicationLogKeepTime is set to 0, replication will be turned off
; it can be set in hours (24h) or days (3d)
replicationLogKeepTime=24h

; replication binary log path
replicationLogPath=/var/lib/nomos/binlog

; port for incoming replication connections
replicationPort=7017

; other masters in ip:port format
masters=127.0.0.1:7018,127.0.0.1:7019
```

***
## Running

`/usr/bin/nomos &` or `/usr/bin/nomos -c someOtherConfigFile &`

## Copyrights 

[Final Level](https://final-level.com/)
