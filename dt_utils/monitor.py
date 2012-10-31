#!/usr/bin/env python2.7

import os
from time import sleep, time
import socket

from dt_utils.status import read_status
import redis

hostname = socket.gethostname()
hostname = hostname[:hostname.find('.')]


# Redis is <host> -> <service> -> <state>
#   Where state is:
#     <status>:<time>
#   Where status is:
#     up, down, flap
#   Where time is:
#     integer representing time in status

def monitor(root_path, host, port=6379, db=0):
    services = os.listdir(root_path)
    r = redis.StrictRedis(host=hostname, port=port, db=db)

    flapping = {}
    while True:
        now = int(time())
        states = {}
        for service in services:
            status = read_status(os.path.join(root_path, service))
            up_string = 'up'
            if not status.up:
                up_string = 'down'
            if now - status.tai < 10:
                if service in flapping:
                    up_string = 'flap'
                flapping[service] = True
            else:
                if service in flapping:
                    del flapping[service]

            states[service] = "{0}:{1}".format(up_string, status.tai)
        r.hmset(hostname, states)
        sleep(10)


if __name__ == "__main__":
    import sys
    monitor(sys.argv[1], sys.argv[2], int(sys.argv[3]))
