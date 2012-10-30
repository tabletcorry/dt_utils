#!/usr/bin/env python2.7
# Parser for Daemontools status file
# References:
#   http://cr.yp.to/libtai/tai64.html
#

import os
from collections import namedtuple
import struct

State = namedtuple("State", [
    'service',
    'tai',
    'tai_n',
    'up',
    'paused',
    'normally_up',
    'normally_down',
    'want_up',
    'want_down'])

TAI_MAIN = 2 ** 62
TAI_EXTENSION_1 = 2 ** 63


def read_status(service):
    down_file = False
    if os.path.exists(os.path.join(service, 'down')):
        down_file = True

    with open(os.path.join(service, 'supervise/status'), 'rb') as status_file:
        status_string = status_file.read()

    # TAI64 labels are stored in big-endian format
    tai, tai_n = struct.unpack('>QI', status_string[:12])
    # DT values are stored in little-endian format
    pid, paused, want = struct.unpack('<I?c', status_string[12:])

    if tai < TAI_MAIN:
        tai = TAI_MAIN - tai
    elif TAI_MAIN <= tai < TAI_EXTENSION_1:
        tai = tai - TAI_MAIN
    else:
        raise ValueError(
            "TAI64 label outside expected ranges: {0}".format(tai))

    assert tai_n < 999999999, "TAI64N label outside valid range: {0}".format(
        tai_n)

    up = True
    if pid == 0:
        up = False

    normally_up = False
    normally_down = False
    if up and down_file:
        normally_down = True
    if not up and not down_file:
        normally_up = True

    assert want == 'u' or want == 'd', "Unexpected 'want' value: {0}".format(
        want)

    want_up = False
    want_down = False
    if not up and want == 'u':
        want_up = True
    if up and want == 'd':
        want_down = True

    return State(service, tai, tai_n, up, paused, normally_up, normally_down,
                 want_up, want_down)


if __name__ == "__main__":
    import sys
    for service in sys.argv[1:]:
        print read_status(service)
