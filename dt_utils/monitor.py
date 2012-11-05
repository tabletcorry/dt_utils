#!/usr/bin/env python2.7

import os
from time import sleep, time
import socket

from dt_utils.status import read_status
import redis

hostname = socket.gethostname()
hostname = hostname[:hostname.find('.')]

INTERVAL = 10


# Redis is <host> -> <service> -> <state>
#   Where state is:
#     <status>:<time>
#   Where status is:
#     up, down, flap
#   Where time is:
#     integer representing time in status

def monitor(root_path, host, port=6379, db=0):
    services = os.listdir(root_path)
    r = redis.StrictRedis(host=host, port=port, db=db)
    host_storage_name = 'dt-monitor:host:' + hostname
    r.sadd('dt-monitor:hosts', host_storage_name)

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
                if service in flapping:
                    up_string = 'flap'
                flapping[service] = True
            else:
                if service in flapping:
                    del flapping[service]

            state_string = "{0}:{1}".format(up_string, status.tai)
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


if __name__ == "__main__":
    import sys
    monitor(sys.argv[1], sys.argv[2], int(sys.argv[3]))
