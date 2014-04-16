#!/bin/sh
# 
# Copyright (c) 2014 Final Level
# Author: Denys Misko <gdraal@gmail.com>
# Distributed under BSD (3-Clause) License (See
# accompanying file LICENSE)
# 
# chkconfig: 2345 61 39
# description: Nomos Storage is a key-value, persistent and high available server 
# processname: nomos
# pidfile: /var/run/nomos.pid

# Source function library.
. /etc/rc.d/init.d/functions

NAME=nomos
DESC="nomos"

PATH=/sbin:/bin:/usr/sbin:/usr/bin

# config
BIN_PATH="/usr/bin/"
NAME="nomos"
WRAPPER_NAME="nomos_wrapper.sh"
WRAPPER="$BIN_PATH$WRAPPER_NAME"
PIDFILE="/var/run/nomos.pid"
WPIDFILE="/var/run/wrapper/nomos.pid"

if [ -f /etc/sysconfig/nomos ]; then
	. /etc/sysconfig/nomos
fi

function check_pid_names {
	pid="$1"
	names="$2"

	PN=$( ps -p $pid -o comm= 2>/dev/null )

	for n in $names ; do
		name="$( echo $n | head -c 15 )"
		[ "_$PN" == "_$name" ] && return 0
	done

	return 1
}


start() {
	if [ -f "$WPIDFILE" ]; then
		wpid=$( cat "$WPIDFILE" )

		if [ "_$wpid" != "_" ] && kill -0 "$wpid" 2>/dev/null && check_pid_names "$wpid" "$WRAPPER_NAME" ; then
			echo "Wrapper $WRAPPER is already running, waiting 10s before next recheck"
			sleep 10
		else
			$WRAPPER &
		fi
	else
		$WRAPPER &
	fi
}

stop() {
	dpid=$( cat "$PIDFILE" )
	wpid=$( cat "$WPIDFILE" )

	check_pid_names "$wpid" "$WRAPPER_NAME" && kill -15 "$wpid" && sleep 5 && kill -9 "$wpid" 2>/dev/null
	check_pid_names "$dpid" "$NAME" && kill -15 "$dpid" && sleep 5 && kill -9 "$dpid" 2>/dev/null

	echo -n > "${PIDFILE}" ; echo -n > "${WPIDFILE}"
}

case "$1" in
	start)
		echo -n "Starting $DESC: $NAME"

		start
		echo "." ; sleep 2
	;;

	stop)
		echo -n "Stopping $DESC: $NAME "

		stop
		exit $?
	;;

	restart|force-reload)
		echo -n "Restarting $DESC: $NAME"

		stop
		sleep 1
		start

		exit $?
	;;

	status)
		echo "Status $DESC: $NAME"

		true
		exit $?
	;;

	*)
		N=/etc/init.d/${NAME}.sh
		echo "Usage: $N {start|stop|restart|force-reload|status}" >&2
		exit 1
        ;;
esac

exit 0
