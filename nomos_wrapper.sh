#!/bin/sh
# 
# Copyright (c) 2014 Final Level
# Author: Denys Misko <gdraal@gmail.com>
# Distributed under BSD (3-Clause) License (See
# accompanying file LICENSE)
# 
# Description: Nomos server wrapper script

# Source function library.
. /etc/rc.d/init.d/functions

# default config
CONFIG="/etc/nomos.cnf"
PROCESS="/usr/bin/nomos"
PROCESS_NAME="nomos"
LOGFILE="/var/log/nomos"
PIDFILE="/var/run/nomos.pid"
DESCRIPTION="Nomos server wrapper script"
MAIL_CMD="/bin/mail"

MAILTO="root@localhost"

WPIDFILE="/var/run/wrapper/nomos.pid"

if [ ! -d /var/run/wrapper ]
then
	mkdir /var/run/wrapper
fi

if [ -f /etc/sysconfig/nomos ]; then
  . /etc/sysconfig/nomos
fi

ulimit -c unlimited
ulimit -n 100000


cd /tmp

WPID=$$

while true ; do
	echo "Starting $DESCRIPTION"

	if [ "$CONFIG" != "" ] 
	then
		CONFIGCMD="-c $CONFIG"
	else
		CONFIGCMD=
	fi

	"$PROCESS" $CONFIGCMD > /dev/null &
	DPID=$!
	echo $WPID > "$WPIDFILE"
	echo $DPID > "$PIDFILE"
	wait

	lastlog="$( echo \"*** $PROCESS_NAME DIED!!! ***\"; date; tail -2 $LOGFILE )"

	echo "$lastlog"
	if [ -f $MAIL_CMD ]
	then
		echo "$lastlog" | "MAIL_CMD" -s "$PROCESS_NAME died `hostname -s`" "$MAILTO"
	fi

	sleep 5
done