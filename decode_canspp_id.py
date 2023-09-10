#!/usr/bin/env python3

import socket, sys
import struct
import threading
import time
import ctypes
import types
import inspect

#####################################################

debug = 1

#####################################################

can_fmt = '=LB3x8s'

if(1):
    #can_pkt = sock.recv(16)

    #can_id, can_len, can_data = struct.unpack(can_fmt, can_pkt)
    can_id, can_len, can_data = ( int(sys.argv[1], 16), int(sys.argv[2]), eval("b'"+sys.argv[3].lstrip('b')+"'") )
    can_id &= socket.CAN_EFF_MASK
    can_data = can_data[:can_len]

    if(debug): print("%08x %d %a" % (can_id, can_len, can_data ) )

    canspp_unknown1   = (can_id & 0x10000000) >> 28
    canspp_broadcast  = (can_id & 0x0f000000) >> 24
    canspp_querytype  = (can_id & 0x00e00000) >> 20
    canspp_device     = (can_id & 0x001f0000) >> 16 # per doku canspp device ids are 5bit; special ids 0x0 or 0x1f ?
    canspp_sender     = (can_id & 0x0000f800) >> 11
    canspp_register   = (can_id & 0x000007fe) >>  1
    canspp_solicited  = (can_id & 0x00000001) >>  0 # merely (active request? in contrast to unsolicited?)

    canspp_value = 0 # can_len==0
    fmt='x'
    if(can_len>4):
        #sys.stderr.write("Unknown data length, ignoring\n")
        fmt='x'*(can_len) # BMS
    else:
        #fmt=('x','b','<h','<l','<l')[can_len]
        fmt=('x','B','<H','<L','<L')[can_len]

        if(can_len>0):
            if(can_len==3): can_data=can_data+b"\0"
            print(can_data)
            print(fmt)
            canspp_value, = struct.unpack(fmt, can_data)

    print("%d %d %x %d %d %3d %d # %d %08x " % ( canspp_unknown1, canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited, canspp_value, canspp_value ))



################################

