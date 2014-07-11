#!/usr/bin/env python2.7

import os
from time import sleep, time
import socket

from dt_utils.status import read_status
import redis

import cPickle as pickle
import struct

hostname = socket.gethostname()
if '.' in hostname:
    hostname = hostname[:hostname.find('.')]

INTERVAL = 10

GRAPHITE_HOST = 'graphite'
GRAPHITE_PORT = 2004

# Redis is <host> -> <service> -> <state>
#   Where state is:
#     <status>:<time>
#   Where status is:
#     up, down, flap
#   Where time is:
#     integer representing time in status

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((GRAPHITE_HOST, GRAPHITE_PORT))

stats_buffer = []
metric_path = "hosts.{0}.DT.{1}.{2}"

def prepare_for_graphite(status, service, now):
    run_time = now - status.tai
    restarted = run_time <= INTERVAL

    service = service.replace('.', '_')

    stats_buffer.append((metric_path.format(hostname, service, "uptime"),
                        (now, run_time)))
    if restarted:
        stats_buffer.append((metric_path.format(hostname, service, "restart"),
                            (now, 1)))

interval_total = 0

def send_to_graphite():
    global stats_buffer, interval_total
    if interval_total >= 6:
        interval_total = 0

        payload = pickle.dumps(stats_buffer)
        header = struct.pack("!L", len(payload))
        message = header + payload
        s.send(message)

    interval_total += 1
    stats_buffer = []


def monitor(root_path, host, port=6379, db=0):
    services = os.listdir(root_path)
    r = redis.StrictRedis(host=host, port=port, db=db)
    host_storage_name = 'dt-monitor:host:' + hostname
    r.sadd('dt-monitor:hosts', host_storage_name)

    heartbeat_accum = 0
    flapping = {}
    states = {}
    while True:
        new_states = {}
        now = int(time())
        for service in services:
            status = read_status(os.path.join(root_path, service))
            up_string = 'up'
            if not status.up:
                up_string = 'down'
            if now - status.tai <= INTERVAL:
                print status
                print flapping
                if service in flapping:
                    up_string = 'flap'
                else:
                    flapping[service] = status.tai
            else:
                if service in flapping:
                    del flapping[service]

            prepare_for_graphite(status, service, now)

            state_string = "{0}:{1}".format(up_string, status.tai if up_string != 'flap' else flapping[service])
            if service in states:
                if states[service] == state_string:
                    continue
            new_states[service] = state_string
        if len(new_states) > 0:
            r.hmset(host_storage_name, new_states)
            for service, state_string in new_states.items():
                states[service] = state_string
                if state_string.startswith('up:'):
                    r.srem('dt-monitor:services:down', host_storage_name + ':' + service)
                    r.srem('dt-monitor:services:flap', host_storage_name + ':' + service)
                elif state_string.startswith('down:'):
                    r.sadd('dt-monitor:services:down', host_storage_name + ':' + service)
                    r.srem('dt-monitor:services:flap', host_storage_name + ':' + service)
                elif state_string.startswith('flap:'):
                    r.sadd('dt-monitor:services:flap', host_storage_name + ':' + service)
                    r.srem('dt-monitor:services:down', host_storage_name + ':' + service)
        sleep(INTERVAL)
        if not r.sismember('dt-monitor:hosts', host_storage_name):
            r.sadd('dt-monitor:hosts', host_storage_name)
            states = {}

        heartbeat_accum += INTERVAL

        if heartbeat_accum % 60 > 0:
            heartbeat_accum = 0
            r.hset('dt-monitor:heartbeat', host_storage_name, now)

        send_to_graphite()

if __name__ == "__main__":
    import sys
    GRAPHITE_HOST = sys.argv[4]
    monitor(sys.argv[1], sys.argv[2], int(sys.argv[3]))
