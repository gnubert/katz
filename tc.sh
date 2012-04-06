#!/bin/sh 

# Copyright 2012 Ludwig Ortmann <ludwig.ortmann@fu-berlin.de>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

USAGE="usage:
${0} set <up> <loss> <delay> <source> <dest> <dport>
${0} reset

up:     upstream kbit or mbit
loss:   lossrate percent (0..100)
delay:  delay ms
target: IP address(a.b.c.d/32) or subnet (a.b.c.d/x)
dport:  only traffic to dest:dport or to source from dport will be
        filtered

If you want to change the rates, you need to reset first.

Example:
${0} set 1580kbit 17 200ms 127.0.0.1/32 127.0.0.1/32 4711
${0} reset
${0} set 10mbit 0.1 10ms 127.0.0.1/32 127.0.0.1/32 4711
" 

# interface to manipulate
UPIF="lo"

# byte [mb | kb]
BURST="10kb"
MTU=15000

#TC="/sbin/tc"
TC="tc"

COMMAND="${1}"
UPRATE="${2}"
LOSS="${3}"
DELAY="${4}"
SOURCE="${5}"
DEST="${6}"
DPORT=${7}

if [ -z "${COMMAND}" ]; then
    echo "${USAGE}"
elif [ "${COMMAND}" = "set" ]; then

    # sanity checks
    if [ -z "${UPRATE}" ]; then
        echo "No upstream limit given"
        exit 1
    elif [ -z "${LOSS}" ]; then
        echo "No loss rate given"
        exit 1
    elif [ -z "${DELAY}" ]; then
        echo "No delay given"
        exit 1
    elif [ -z "${SOURCE}" ]; then
        echo "No target1 given"
        exit 1
    elif [ -z "${DEST}" ]; then
        echo "No target2 given"
        exit 1
    elif [ -z "${DPORT}" ]; then
        echo "No dport given"
        exit 1
    fi
    
    #echo "disable tso"
    #ethtool -K ${IF} tso off

    # prio queueing
    ${TC} qdisc add dev ${UPIF} root handle 1: prio
    # limit queue
    ${TC} qdisc add dev ${UPIF} parent 1:3 handle 30: \
        tbf rate ${UPRATE} \
        latency ${DELAY} burst ${BURST} \
        mtu ${MTU}
    # add loss to limit queue
    ${TC} qdisc add dev ${UPIF} parent 30:1 handle 31: \
        netem loss ${LOSS}%
    # filter source/dest
    ${TC} filter add dev ${UPIF} protocol ip parent 1:0 prio 3 u32 \
        match ip dst ${SOURCE} \
        match ip sport ${DPORT} 0xffff \
        flowid 1:3
    ${TC} filter add dev ${UPIF} protocol ip parent 1:0 prio 3 u32 \
        match ip dst ${DEST} \
        match ip dport ${DPORT} 0xffff \
        flowid 1:3


elif [ "${COMMAND}" = "reset" ]; then

    echo "deleting tc egress qdisc"
    ${TC} qdisc del dev ${UPIF} root

else
    echo "unknown command: ${COMMAND}"
    echo "run ${0} for help"
    exit 1
fi
