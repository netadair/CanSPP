#!/usr/bin/env python3

#
# fakegm_nt.py
#
# Fake a grid manager device from remote grid manager, SDM630 or remote D0/SML meter
# Work in progress
#
#  Mar 20  2021         Michael Rausch          Publish.
#

import socket, sys
import struct
import threading
import time
import ctypes
import types
import inspect
import re
import traceback
import datetime
import os
import binascii

#####################################################

socket.setdefaulttimeout(30)

debug = 0

listenonly=0

GM_PARAMETERS = 'solhybrid.gm.parameters.txt'
WR_PARAMETERS = 'solhybrid.wr.parameters.txt'
ZE_PARAMETERS = 'solhybrid.ze.parameters.txt'

METER_FILE = '/var/run/meter1_reading.txt'


# --------------------------------------------------------------------------- #
# debug
# --------------------------------------------------------------------------- #
import faulthandler, signal
faulthandler.register(signal.SIGUSR1)

#####################################################

can_fmt = '=LB3x8s'

#####################################################

# a bit of perlism :)
pi = { k:int(v) for k, Nil, Nil, v, Nil in ( l.split("\t",4) for l in open(GM_PARAMETERS, mode='r', encoding='utf-8') if not (l in ["\n"]) ) }

# add private registers, cannot be officially read and is not necessarily an integer value
for k,v in {'MR_METER_TS':1024}.items():
    pi[k] = v
    #gm_register_size[v] = 4
    #gm_pw_level[v] = 0
    #gm_digits[v] = 0

pi.update( { '_'+str(v):k for k, v in pi.items() } )
pid = type("Names", (object,), pi)


num_registers = 1+max([ int(k.lstrip('_')) for k in pi.keys() if k.startswith('_') ])
gm_registers = [0] * (num_registers)

gm_register_size = [4] * (num_registers) # default ULONG
gm_pw_level      = [0] * (num_registers)
gm_digits        = [0] * (num_registers)

gm_read_callbacks= [None] * (num_registers)

for l in open(GM_PARAMETERS, mode='r', encoding='utf-8'):
    if not (l in ["\n"]):
        k, Nil, Nil, v, Nil, Nil, Nil, Nil, Nil, size, pwlevel, Nil, Nil, nk, Nil, Nil, Nil, Nil = l.split("\t",18)
        if(k=='Kurzbezeichnung'): continue
        if size in ["USHORT"]:
            gm_register_size[int(v)] = 2
        if size in ["SHORT"]:
            gm_register_size[int(v)] = -2
        if size in ["LONG"]:
            gm_register_size[int(v)] = -4
        gm_pw_level[int(v)] = int(pwlevel.strip('PW'))
        gm_digits[int(v)] = int(nk)

# add private registers, cannot be officially read and is not necessarily an integer value
gm_register_size[pid.MR_METER_TS] = 8
gm_digits[pid.MR_METER_TS] = 3

wr_register_size = [4] * (1024) # default ULONG
#wr_pw_level      = [0] * (1024)
wr_digits        = [0] * (1024)
for l in open(WR_PARAMETERS, mode='r', encoding='utf-8'):
    if not (l in ["\n"]):
        k, Nil, Nil, v, Nil, Nil, Nil, Nil, Nil, size, pwlevel, Nil, Nil, nk, Nil, Nil, Nil, Nil, Nil = l.split("\t",19)
        if(k=='Kurzbezeichnung'): continue
        if size in ["USHORT"]:
            wr_register_size[int(v)] = 2
        if size in ["SHORT"]:
            wr_register_size[int(v)] = -2
        if size in ["LONG"]:
            wr_register_size[int(v)] = -4
        #wr_pw_level[int(v)] = int(pwlevel.strip('PW'))
        wr_digits[int(v)] = int(nk)

ze_register_size = [4] * (1024) # default ULONG
#ze_pw_level      = [0] * (1024)
ze_digits        = [0] * (1024)
for l in open(ZE_PARAMETERS, mode='r', encoding='utf-8'):
    if not (l in ["\n"]):
        k, Nil, Nil, v, Nil, Nil, Nil, Nil, Nil, size, pwlevel, Nil, Nil, nk, Nil, Nil, Nil, Nil, Nil = l.split("\t",19)
        if(k=='Kurzbezeichnung'): continue
        if size in ["USHORT"]:
            ze_register_size[int(v)] = 2
        if size in ["SHORT"]:
            ze_register_size[int(v)] = -2
        if size in ["LONG"]:
            ze_register_size[int(v)] = -4
        #ze_pw_level[int(v)] = int(pwlevel.strip('PW'))
        ze_digits[int(v)] = int(nk)


#####################################################


GM_STATUS_INIT     =  0 # "Initialization",
GM_STATUS_READY    =  1 # "Ready for Operation",
GM_STATUS_COMDFLT  = 31 # "Communication with default setting",
GM_STATUS_OPER     = 58 # "Operation",
#GM_STATUS_IDE      = 59 # "Idle", # ok for GM?
GM_STATUS_ERROR    = 63 # "Operation Error", ????

# send every 40ms/25Hz
gm_registers[pid.STATUS] = GM_STATUS_INIT # default status

# send every 40ms/25Hz
gm_registers[pid.ADE_POWER_L1] = 0
gm_registers[pid.ADE_POWER_L2] = 0
gm_registers[pid.ADE_POWER_L3] = 0
gm_registers[pid.SUMME_LEISTUNG] = gm_registers[pid.ADE_POWER_L1] + gm_registers[pid.ADE_POWER_L2] + gm_registers[pid.ADE_POWER_L3]


# read every 60sec
# Variabler Energiezähler Einspeisung Ln , Variabler Energiezähler Netzversorgung Ln
gm_registers[pid.VECFIL1] = 0
gm_registers[pid.VECGSL1] = 0
gm_registers[pid.VECFIL2] = 0
gm_registers[pid.VECGSL2] = 0
gm_registers[pid.VECFIL3] = 0
gm_registers[pid.VECGSL3] = 0

# read after reset
#
gm_registers[pid.HSR]   = 0         # Hardware Selftest Result
gm_registers[pid.FWV]   = 1090002   # Firmware-Version
gm_registers[pid.DCONF] = 0x17      # EMS Konfiguration; 0x17 = AC,  0x13 = DC
gm_registers[pid.SPR]   = 0         # Sprache
gm_registers[pid.LCODE] = 0         # Laendercode
gm_registers[pid.IC]    = 1         # Installateurcode

gm_registers[pid.GK]    = 1202      # GL_GM_63E With integrated current sensor, for energy manager:

gm_registers[pid.ASPP]  = 4

gm_registers[pid.LBF]   = 0         # keine fehler
gm_registers[pid.LF]    = 0

gm_registers[pid.ADE_GRID_VOLTAGE_L1] = 230 * pow(10,gm_digits[pid.ADE_GRID_VOLTAGE_L1])
gm_registers[pid.ADE_GRID_VOLTAGE_L2] = 230 * pow(10,gm_digits[pid.ADE_GRID_VOLTAGE_L2])
gm_registers[pid.ADE_GRID_VOLTAGE_L3] = 230 * pow(10,gm_digits[pid.ADE_GRID_VOLTAGE_L3])
gm_registers[pid.ADE_GRID_FREQ]       =  50 * pow(10,gm_digits[pid.ADE_GRID_FREQ])

# more?

global_canbus_silence = False

#####################################################

def int32_to_uint32(i):
    return struct.unpack_from("I", struct.pack("i", i))[0]

def int16_to_uint16(i):
    return struct.unpack_from("H", struct.pack("h", i))[0]

def uint16_to_int16(i):
    return struct.unpack_from("h", struct.pack("H", i))[0]

def uint16_2_to_int32(i,j):
    return struct.unpack_from(">i", struct.pack(">HH", i,j))[0]

def uint16_2_to_uint32(i,j):
    return struct.unpack_from(">I", struct.pack(">HH", i,j))[0]

def uint16_2_to_float32(i,j):
    return struct.unpack_from(">f", struct.pack("!HH", i,j))[0]


#####################################################

class TimeCalibration(object):

    def __init__(self, value=0):
        self.base = 1384881120 # date --date="2013-11-19 17:12:00 UTC" +%s  ; s/b 20140101
        self.now = time.time()
        #self.value = value
        self.value = self.now - self.base
        #self.value = 0

    def __call__(self, canspp_unknown1=None, canspp_broadcast=None, canspp_querytype=None, canspp_device=None, canspp_sender=None, canspp_register=None, canspp_solicited=None, canspp_value=None):
        if(canspp_value is not None):
            self.now = time.time()
            self.value = canspp_value
        return int( ( time.time() - self.now ) + self.value)

    def __repr__(self):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.base + self.__call__()))
        #return self.__str__()

    def __str__(self):
        return str(self.__call__())


gm_registers[pid.TC] = TimeCalibration()         # Time Calibration, seconds since 2014


#####################################################

class Password(object):

    def timeout(self):
        self.level=0
        self.resetactive=None

    def __init__(self, level=0):
        self.level=level
        self.resetactive=None

    def __call__(self, canspp_unknown1=None, canspp_broadcast=None, canspp_querytype=None, canspp_device=None, canspp_sender=None, canspp_register=None, canspp_solicited=None, level=None):
        if(level is not None):
            ln=-1
            if(level == 0): ln=0
            if(level == 4): ln=1
            #if(level == ): ln=2
            #if(level == ): ln=3
            if(level == 131112): ln=4
            if(ln!=-1):
                if(self.resetactive is not None): self.resetactive.cancel()
                self.level=ln
                self.resetactive=threading.Timer(5*60, self.timeout) # , args=([self]))
                self.resetactive.start()
                return level # original code, not pw level
        return self.level

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return str(self.__call__())


gm_registers[pid.PW] = Password()

#####################################################

HEARTBEAT_TIMEOUT = 3.1

class Heartbeat_update(object):

    # 0900.735354 81402374 020000001f00000000000000  broadcast  write   *,ALL <-   4,GM  442 (HBCNT     ) 0x0000001f [Without] (nativ: 31)  (unslctd)

    def __init__(self, level=0):
        self.heartbeat_received=[0,0,0,0,0]

    def __call__(self, canspp_unknown1=None, canspp_broadcast=None, canspp_querytype=None, canspp_device=None, canspp_sender=None, canspp_register=None, canspp_solicited=None, canspp_value=None):
        now=time.time()
        hbcnt = 0x10
        if(self.heartbeat_received[1] > now-HEARTBEAT_TIMEOUT): hbcnt = hbcnt | 0x2
        if(self.heartbeat_received[2] > now-HEARTBEAT_TIMEOUT): hbcnt = hbcnt | 0x4
        if(self.heartbeat_received[3] > now-HEARTBEAT_TIMEOUT): hbcnt = hbcnt | 0x8
        #if(self.heartbeat_received[4] > now-HEARTBEAT_TIMEOUT): hbcnt = hbcnt | 0x10
        if(hbcnt == 0x1e): hbcnt=0x1f
        if(canspp_sender!=0 and canspp_solicited==0 and canspp_querytype==4 and ((canspp_device==4 and canspp_broadcast==2) or (canspp_device==0 and canspp_broadcast==1))):
            self.heartbeat_received[canspp_sender] = now
        return hbcnt

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return str(self.__call__())

#send every 1s ; feeds back received heart beats
gm_registers[pid.HBCNT] = Heartbeat_update()

#####################################################

def set_ready():
    if(True or gm_registers[pid.STATUS] != GM_STATUS_OPER):
        gm_registers[pid.STATUS] = GM_STATUS_READY

def set_oper():
    gm_registers[pid.STATUS] = GM_STATUS_OPER

class Configuration(object):

    def silence():
        global_canbus_silence = True

    def perform_reset(self):
        gm_registers[pid.PW] = 0
        gm_registers[pid.TC] = 0 # allow "initialization"?
        self.cfg1 = self.cfg1 & 0xffffffef
        self.resetactive=None
        gm_registers[pid.STATUS] = GM_STATUS_READY
        gm_registers[pid.HBCNT].heartbeat_received=[0,0,0,0,0]
        global_canbus_silence = False
        threading.Timer(5, set_oper).start()


    def __init__(self, cfg1=0x0):
        self.cfg1 = cfg1
        self.resetactive=None

    def __call__(self, canspp_unknown1=None, canspp_broadcast=None, canspp_querytype=None, canspp_device=None, canspp_sender=None, canspp_register=None, canspp_solicited=None, canspp_value=None):

        if(canspp_sender!=0 and canspp_solicited==1 and canspp_querytype==4 and (canspp_device==4 and canspp_broadcast==2)):
            self.cfg1=canspp_value

            # no clue what this bit is...
            if(self.cfg1 & 0x10000 == 0x10000):
                self.cfg1 = self.cfg1 & 0xfffeffff
                pass

            if(self.cfg1 & 0x10 == 0x10 and self.resetactive is not None):
                gm_registers[pid.STATUS] = GM_STATUS_INIT
                self.resetactive=threading.Timer(12, self.perform_reset) # , args=([self]))
                self.resetactive.start()
                threading.Timer(2, self.silence).start()


        return (self.cfg1 & 0xffff)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return str(self.__call__())


gm_registers[pid.CFG1] = Configuration()


#####################################################

def construct_canid(canspp_unknown1, canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited):
    """
    This function construct a CANSPP id from parameters
    """

    canspp_unknown1  = ( canspp_unknown1  << 28 ) & 0x10000000
    canspp_broadcast = ( canspp_broadcast << 24 ) & 0x0f000000
    canspp_querytype = ( canspp_querytype << 20 ) & 0x00e00000
    canspp_device    = ( canspp_device    << 16 ) & 0x001f0000
    canspp_sender    = ( canspp_sender    << 11 ) & 0x0000f800
    canspp_register  = ( canspp_register  <<  1 ) & 0x000007fe
    canspp_solicited = ( canspp_solicited <<  0 ) & 0x00000001

    return ctypes.c_ulong( socket.CAN_EFF_FLAG | canspp_unknown1 | canspp_broadcast | canspp_querytype | canspp_device | canspp_sender | canspp_register | canspp_solicited ).value


class Heartbeat(object):

    def __init__(self, sock, interval=1000):
        self.sock = sock
        self.interval = interval

        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution

    def run(self):
        while True:

            if(1 or gm_registers[pid.STATUS] != GM_STATUS_INIT):

                # $cansend $CANDEV 81402374#1f00           # 42 (HBCNT     ) 0x0000001f, 1Hz

                can_id = construct_canid(0, 1, 4, 0, 4, pid.HBCNT, 0)
                can_len = 2
                can_data = struct.pack('<H', gm_registers[pid.HBCNT]() ) # I know it is a class instance

                if(debug): print("%08x %d %a" % (can_id, can_len, can_data ) )
                can_pkt = struct.pack(can_fmt, can_id, can_len, can_data)
                sock.send(can_pkt)

            time.sleep(self.interval / 1000)


class Status(object):

    def __init__(self, sock, interval=40):

        self.sock = sock
        self.interval = interval

        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution

    def run(self):

        while True:

            # $cansend $CANDEV 81412040#3a00          # 32 (STATUS    ) 58 0.04ms, 25Hz

            can_id = construct_canid(0, 1, 4, 1, 4, pid.STATUS, 0)
            can_len = 2
            can_data = struct.pack('<H', gm_registers[pid.STATUS])

                #print("%08x %d %s" % (can_id, can_len, ':'.join(hex(ord(x))[2:] for x in can_data) ) )
            if(debug): print("%08x %d %a" % (can_id, can_len, can_data ) )
            can_pkt = struct.pack(can_fmt, can_id, can_len, can_data)
            sock.send(can_pkt)

            time.sleep(self.interval / 1000)

######################################

class PeriodicQuery(object):

    def __init__(self, sock, can_device=1, can_sender=4, can_register=0, interval=10000):

        self.sock = sock
        self.can_device=can_device
        self.can_sender=can_sender
        self.can_register=can_register
        self.interval = interval

        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution

    def run(self):

        while True:

            can_id = construct_canid(0, 2, 12, self.can_device, self.can_sender, self.can_register, 1) # unicast query solicited
            can_len = 0
            can_data = b''

            if(debug): print("%08x %d %a" % (can_id, can_len, can_data ) )
            can_pkt = struct.pack(can_fmt, can_id, can_len, can_data)
            sock.send(can_pkt)

            time.sleep(self.interval / 1000)

######################################

Lx_POWER_MOD=25

class Power(object):

    def __init__(self, sock, interval=40):

        self.sock = sock
        self.interval = interval/1000
        self.cnt=0
        self.last_ts=0
        self.interval_mean=0
        self.interval_mean_cnt=0

        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution

    def run(self):

        def send_reading(register):

                value = gm_registers[register]
                if(not isinstance(value,int)): value = value()

                can_id = construct_canid(0, 1, 4, 1, 4, register, 0)
                can_len = 4
                can_data = struct.pack('<l', value)

                if(debug): print("%08x %d %a" % (can_id, can_len, can_data ) )
                can_pkt = struct.pack(can_fmt, can_id, can_len, can_data)
                sock.send(can_pkt)

        while True:
            then = time.time()

            # $cansend $CANDEV 81412058#$(hex8 $power)        # 44 (SUMME_LEISTUNG) 0 [Watt]
            # $cansend $CANDEV 81412052#$(hex8 $power)        # 41 (ADE_POWER_L1) 0 [Watt]
            # $cansend $CANDEV 81412054#00000000      # 42 (ADE_POWER_L2) 0 [Watt]
            # $cansend $CANDEV 81412056#00000000      # 43 (ADE_POWER_L3) 0 [Watt]

            if(gm_registers[pid.STATUS] == GM_STATUS_OPER or gm_registers[pid.STATUS] == GM_STATUS_COMDFLT):

                #if(gm_registers[pid.SUMME_LEISTUNG] < -14000): gm_registers[pid.SUMME_LEISTUNG]=-14000
                send_reading(pid.SUMME_LEISTUNG)
                if(self.cnt % Lx_POWER_MOD == 0): send_reading(pid.ADE_POWER_L1)
                if(self.cnt % Lx_POWER_MOD == 1): send_reading(pid.ADE_POWER_L2)
                if(self.cnt % Lx_POWER_MOD == 2): send_reading(pid.ADE_POWER_L3)

            self.cnt = (self.cnt + 1) % Lx_POWER_MOD

            now = time.time()

            if(self.last_ts != 0): self.interval_mean = self.interval_mean + (now-self.last_ts) - self.interval
            self.interval_mean_cnt=self.interval_mean_cnt+1
            self.last_ts=now
            pll=self.interval_mean/self.interval_mean_cnt
            if(self.interval_mean_cnt>100):
                self.interval_mean=pll
                self.interval_mean_cnt=1

            #print("%0.8f" % pll)

            sleep=self.interval-(pll + 0.0001)-(now-then)
            if(sleep<0): sleep=self.interval
            time.sleep(sleep)

"""

fulltrace:0899.600735 82440b4704000000899e080c0001ffff    unicast  write    4,GM <-   1,ZE  419 (TC        ) 201891465 [Without] (nativ: 201891465)  ()
fulltrace:0899.600823 8201234604000000899e080c00000000    unicast  wrote    1,ZE <-   4,GM  419 Successfully wrote 201891465  (unslctd)

fulltrace:0899.600683 8244084b04000000280002000003ffff    unicast  write    4,GM <-   1,ZE   37 (PW        ) 131112 [Without] (nativ: 131112)  ()
fulltrace:0899.600784 8201204a040000002800020000000000    unicast  wrote    1,ZE <-   4,GM   37 Successfully wrote 131112  (unslctd)


fulltrace:0911.185547 82440b4704000000959e080c0001ffff    unicast  write    4,GM <-   1,ZE  419 (TC        ) 201891477 [Without] (nativ: 201891477)  ()
fulltrace:0911.268945 82c40b47000000000001014800000000    unicast  query    4,GM <-   1,ZE  419 (TC        )  ()

fulltrace2:0035.165916 82440b4704000000109f080c0007ffff    unicast  write    4,GM <-   1,ZE  419 (TC        ) 201891600 [Without] (nativ: 201891600)  ()
fulltrace2:0035.204248 8201234604000000109f080c00000000    unicast  wrote    1,ZE <-   4,GM  419 Successfully wrote 201891600  (unslctd)
fulltrace2:0035.205917 82c40b47000000000000000000000000    unicast  query    4,GM <-   1,ZE  419 (TC        )  ()
fulltrace2:0035.206585 8281234604000000109f080c00000000    unicast answer    1,ZE <-   4,GM  419 (TC        ) 201891600 [Without] (nativ: 201891600)  (unslctd)



"""

######################################

HIRES_MEAN=20

DAEMPFUNG=0.5

def extrapolate(x1,x2,y0,y1,y2):
    """
    y0 = now, x1,y1 = most recent
    """

    try:
        x=x1-x2
        y=y1-y2

        if(y==0 or y2>y1 or y0==y1): return x1
        if(y0>(y1+y1-y2)): return x1

        v= x1 + x * min(1,max(-1,( (y0-y1)/y ))) # * DAEMPFUNG

        return v
    except:
        return x1


class HiresTotalPower(object):

    def __init__(self, value=0):
        self.now = time.time()
        self.value = value
        self.pastnows = [self.now] *(HIRES_MEAN)
        self.pastvalues = [self.value] *(HIRES_MEAN)

    def __call__(self, canspp_unknown1=None, canspp_broadcast=None, canspp_querytype=None, canspp_device=None, canspp_sender=None, canspp_register=None, canspp_solicited=None, canspp_value=None):

        now = time.time()

        if(canspp_value is not None):
            self.now = now
            self.value = canspp_value
            self.pastnows = [ now ] + self.pastnows[:HIRES_MEAN-1]
            self.pastvalues = [ canspp_value ] + self.pastvalues[:HIRES_MEAN-1]

        #value=extrapolate(self.pastvalues[0], self.pastvalues[1], now, self.pastnows[0], self.pastnows[1])

        ts=self.pastnows[0]
        value=self.pastvalues[0]
        # value=uint16_to_int16(value) # written as signed variables

        # lp() positive = Erzeugung/Einspeisung , negativ = Bezug/Ladung
        p_wr_then = lp(2,5,None,ts)
        p_wr_now  = lp(2,5,None,now)
        #p_wr_then = lp(2,209,None,ts)
        #p_wr_now  = lp(2,209,None,now)

        #gm_power_then  = lp(4+0x20,44,None,ts)
        gm_power_now  = lp(4+0x20,44,None,now)

        value_new = value + p_wr_then - p_wr_now
        #value_new = value # - p_wr_now

        #value_alternative = value - gm_power_then + gm_power_now
        value_alternative = value + gm_power_now

        print("now %.6f ts meter %.6f meter %6d wr then %6d wr now %6d gm now %6d sim value %6d sim alt %6d diff %6d" % (now,ts,value,p_wr_then, p_wr_now, gm_power_now, value_new, value_alternative, value_new-gm_power_now) )
        #print("now %.6f ts meter %.6f meter %6d wr now %6d gm now %6d sim value %6d sim alt %6d" % (now,ts,value, p_wr_now, gm_power_now, value_new, value_alternative) )

        #return value_new
        return gm_power_now
        #return value_alternative

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return str(self.__call__())

#########################################################


map_sdm630_meter_gm={ \
0:pid.SN, \
30013:pid.ADE_POWER_L1, 30015:pid.ADE_POWER_L2, 30017:pid.ADE_POWER_L3, 30053:pid.SUMME_LEISTUNG, \
30007:pid.ADE_CURRENT_L1, 30009:pid.ADE_CURRENT_L2, 30011:pid.ADE_CURRENT_L3, \
30001:pid.ADE_GRID_VOLTAGE_L1, 30003:pid.ADE_GRID_VOLTAGE_L2, 30005:pid.ADE_GRID_VOLTAGE_L3, 30071:pid.ADE_GRID_FREQ, \
30347:pid.VECGSL1, 30349:pid.VECGSL2, 30351:pid.VECGSL3, 30353:pid.VECFIL1, 30355:pid.VECFIL2, 30357:pid.VECFIL3, \
30019:pid.ADE_VA_L1, 30021:pid.ADE_VA_L2, 30023:pid.ADE_VA_L3, 30025:pid.ADE_VAR_L1, 30027:pid.ADE_VAR_L2, 30029:pid.ADE_VAR_L3 \
}

map_obis_meter_gm={
'1-0:0.0.0':pid.SN, \
'1-0:36.7.0':pid.ADE_POWER_L1, '1-0:56.7.0':pid.ADE_POWER_L2, '1-0:76.7.0':pid.ADE_POWER_L3, '1-0:16.7.0':pid.SUMME_LEISTUNG, \
#30007:pid.ADE_CURRENT_L1, 30009:pid.ADE_CURRENT_L2, 30011:pid.ADE_CURRENT_L3, \
#30001:pid.ADE_GRID_VOLTAGE_L1, 30003:pid.ADE_GRID_VOLTAGE_L2, 30005:pid.ADE_GRID_VOLTAGE_L3, 30071:pid.ADE_GRID_FREQ, \
'1-0:1.8.0':pid.VECGSL1, '1-0:2.8.0':pid.VECFIL1, \
#30019:pid.ADE_VA_L1, 30021:pid.ADE_VA_L2, 30023:pid.ADE_VA_L3, 30025:pid.ADE_VAR_L1, 30027:pid.ADE_VAR_L2, 30029:pid.ADE_VAR_L3, \
'1-0:0.9.4':pid.MR_METER_TS \
}

METER_FILE_AGE = 10

class MeterFileException(Exception):
    pass

class MeterFileSkipException(Exception):
    pass

class MeterReading(object):

    def __init__(self, interval=1500):
        self.interval = interval
        self.last_meter_file_ts = 0

        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution

    @staticmethod
    def amend_units(reg, value, unit):
        if(map_obis_meter_gm.get(reg) == pid.SN):
            return int(value) % 6553
        elif(map_obis_meter_gm.get(reg) == pid.MR_METER_TS):
            return float(value)
        elif(unit == 'Wh'):
            return float(value)/1000 # return in kWh
        elif(unit == 'W'):
            return float(value)
        else:
            return value
        pass

    def run(self):

        while True:

            now = time.time()
            meterfilets = now

            try:
                meterfilets = os.stat(METER_FILE).st_ctime

                if(meterfilets<now-METER_FILE_AGE):
                   raise MeterFileException('Meter file too old')

                if(meterfilets==self.last_meter_file_ts):
                   raise MeterFileSkipException('No need to re-read same file')

                #print(now, meterfilets)

                #mr = { int(reg):float(val) for reg,desc,val,unit in ( l.split(":") for l in open(METER_FILE, mode='r', encoding='utf-8') if not (l in ["\n"]) ) }
                mr = { reg:self.amend_units(reg,val,unit) for reg,val,unit in ( re.split("\(|\*|\)",l)[:3] for l in open(METER_FILE, mode='r', encoding='utf-8') if not (l in ["\n"]) ) }

                #print(mr)

                for k,v in mr.items():
                   try:
                       kr=map_obis_meter_gm[k]
                       #kr=map_sdm630_meter_gm[k]
                       vr = int( v * pow(10,gm_digits[kr]) )

                       value=gm_registers[kr]
                       if(not isinstance(value,int)):
                           value = value(None, None, None, None, None, kr, None, vr)
                       else:
                           gm_registers[kr] = vr

                   except KeyError:
                       pass

                self.last_meter_file_ts = meterfilets

                if(gm_registers[pid.STATUS] == GM_STATUS_ERROR): gm_registers[pid.STATUS] = GM_STATUS_OPER
                if(gm_registers[pid.STATUS] == GM_STATUS_READY): gm_registers[pid.STATUS] = GM_STATUS_OPER

            except MeterFileSkipException:
                pass
            except MeterFileException:
                if(gm_registers[pid.STATUS] == GM_STATUS_OPER): gm_registers[pid.STATUS] = GM_STATUS_READY
                pass
            except Exception as e:
                #sys.stderr.write(e.repr())
                traceback.print_exc()
                gm_registers[pid.STATUS] = GM_STATUS_ERROR
                pass
            finally:
                if(gm_registers[pid.STATUS] != GM_STATUS_OPER):
                    for kr in ( pid.ADE_POWER_L1, pid.ADE_POWER_L2, pid.ADE_POWER_L3, pid.SUMME_LEISTUNG ):
                        try:
                            value=gm_registers[kr]
                            vr=0
                            if(not isinstance(value,int)):
                                value = value(None, None, None, None, None, kr, None, vr)
                            else:
                                gm_registers[kr] = vr
                        except KeyError:
                            pass
                pass

            now = time.time()
            sleep=(self.interval+10)/1000-(now-meterfilets)
            if(sleep<0): sleep=self.interval/1000
            time.sleep(sleep)

#########################################################

class ListenHash(object):

    def __init__(self):
        self.hashs=[]

    def listen(canspp_unknown1, canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited):
        key = (canspp_unknown1, canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited)
        if(not self.hash.has_key(key) ):
            self.hash[ key ] = (0,0)

    def unlisten(canspp_unknown1, canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited):
        key = (canspp_unknown1, canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited)
        if(self.hash.has_key(key) ):
            del self.hash[key]

    def __call__(self, canspp_unknown1=None, canspp_broadcast=None, canspp_querytype=None, canspp_device=None, canspp_sender=None, canspp_register=None, canspp_solicited=None, canspp_value=None):
        key = (canspp_unknown1, canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited)
        self.now = time.time()

        if(canspp_value is not None and self.hash.has_key(key) ):
            self.hash[ key ] = ( canspp_value, now )
            return self.hash[ key ]

        key = (canspp_unknown1, canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited)
        if(self.hash.has_key(key) ): self.hash[ key ]

        # add best match?!

        return None

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return str(self.__call__())

#########################################################

POWER_HISTORY=200

class ListenPower(object):

    def __init__(self):
        self.registers={}

    @staticmethod
    def bisect(arr, x):
        l=0
        r=len(arr)-1
        if x >= arr[l][1]: return [arr[l],arr[l]]
        if arr[r][1] >= x: return [arr[r],arr[r]]
        while l <= r:
            mid = l+(r - l)//2;
            if arr[mid][1] == x: return [arr[mid],arr[mid]]
            elif arr[mid-1][1] >= x and x > arr[mid][1]: return [arr[mid-1],arr[mid]]
            elif arr[mid][1] > x and x >= arr[mid+1][1]: return [arr[mid],arr[mid+1]]
            elif arr[mid][1] > x: l = mid + 1
            else: r = mid - 1
        return None

    def __call__(self, canspp_sender, canspp_register, canspp_value=None, timestamp=None):
        key = ( canspp_sender, canspp_register )
        now = time.time()

        if(debug):
            if(canspp_value is not None):
                print("lp:",key, now, canspp_value)
            else:
                print("lp:",key, timestamp, "?")

        if(canspp_value is None and self.registers.get(key) is None):
            return 0

        if(canspp_value is not None):
            add=(canspp_value, now)
            if(self.registers.get(key) is None):
                self.registers[key] = [add] * (POWER_HISTORY)
            self.registers[key] = [add] + self.registers[key][:POWER_HISTORY-1]
            return canspp_value

        if(timestamp is None or timestamp>=self.registers[key][0][1]):
            return self.registers[key][0][0]
        if(timestamp<=self.registers[key][POWER_HISTORY-1][1]):
            return self.registers[key][POWER_HISTORY-1][0]

        if(False):
            # linear interpolation between best matching timestamps

            bestafter,bestbefore = self.bisect(self.registers[key],timestamp)

            if(bestbefore[1] == bestafter[1]):
                return bestbefore[0]

            #v = bestbefore[0]
            v = bestbefore[0] + (bestafter[0]-bestbefore[0]) * (timestamp-bestbefore[1])/(bestafter[1]-bestbefore[1])
            return int(v)
            #return None

        if(True):
            # nearest vaulue from best matching timestamps

            bestafter,bestbefore = self.bisect(self.registers[key],timestamp)

            #if(bestbefore[1] == bestafter[1]):
            #    return bestbefore[0]

            v = bestbefore[0]
            #v = bestafter[0]
            #v = bestbefore[0] + (bestafter[0]-bestbefore[0]) * (timestamp-bestbefore[1])/(bestafter[1]-bestbefore[1])
            return int(v)
            #return None



        return self.registers[key][0][0]


    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return str(self.registers) # ?


# save status, but do not historize or interpolate results. save as is.
class OtherStatus(object):

    def __init__(self):
        self.registers={}

    def __call__(self, canspp_sender, canspp_register, canspp_value=None):
        key = ( canspp_sender, canspp_register )
        now = time.time()

        if(debug):
            if(canspp_value is not None):
                print("lp:",key, now, canspp_value)
            else:
                print("lp:",key, timestamp, "?")

        if(canspp_value is None and self.registers.get(key) is None):
            return 0

        if(canspp_value is not None):
            add=(canspp_value, now)
            self.registers[key] = [add]
            return canspp_value

        return self.registers[key][0][0]

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return str(self.registers) # ?


#########################################################

class ReceiveOtherData(object):

    def __init__(self, sock):

        self.sock = sock

        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True                            # Daemonize thread
        thread.start()                                  # Start the execution

    def run(self):

        while True:

            try:
                can_pkt = self.sock.recv(16)
            except KeyboardInterrupt:
                sys.exit(2)
            except socket.timeout:
                pass
            except socket.error as exc:
                print ("socket error occured %s, disabling listener" % exc)
                break

            if(len(can_pkt)!=16):
                continue

            #print ("gm", len(can_pkt), binascii.hexlify(can_pkt))

            ( can_id, can_len, can_data, canspp_unknown1, canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited, canspp_value, fmt ) = \
            parse_canspp(can_pkt, False)
            #parse_canspp(can_pkt, True)

            #print ("gm", canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited, canspp_value )

            # write broadcast unsolicited
            if(canspp_querytype==4 and canspp_broadcast==1 and canspp_solicited==0 and \
            (canspp_sender, canspp_device, canspp_register) in [(4,1,44), (4,1,41), (4,1,42), (4,1,43),] ):
                lp(canspp_sender+0x20, canspp_register, canspp_value)
                continue

            #time.sleep(self.interval / 1000)


#########################################################

def parse_canspp(can_pkt, big_endian=False):

    my_can_fmt = can_fmt
    if(big_endian): my_can_fmt = '>LB3x8s'

    can_id, can_len, can_data = struct.unpack(my_can_fmt, can_pkt)
    can_id &= socket.CAN_EFF_MASK
    can_data = can_data[:can_len]

    can_raw_input = "%08x %d %a" % (can_id, can_len, can_data )
    if(debug): print( can_raw_input )

    canspp_unknown1   = (can_id & 0x10000000) >> 28
    canspp_broadcast  = (can_id & 0x0f000000) >> 24
    canspp_querytype  = (can_id & 0x00e00000) >> 20
    canspp_device     = (can_id & 0x001f0000) >> 16 # per doku canspp device ids are 5bit; special ids 0x0 or 0x1f ?
    canspp_sender     = (can_id & 0x0000f800) >> 11
    canspp_register   = (can_id & 0x000007fe) >>  1
    canspp_solicited  = (can_id & 0x00000001) >>  0 # merely (active request? in contrast to unsolicited?)

    register_size = gm_register_size
    # not 100% ok...
    if(canspp_sender==2 and ((canspp_querytype==8 and canspp_broadcast==2) or (canspp_querytype==4 and canspp_broadcast==1))): register_size = wr_register_size
    if(canspp_sender==1 and ((canspp_querytype==8 and canspp_broadcast==2) or (canspp_querytype==4 and canspp_broadcast==1))): register_size = ze_register_size

    canspp_value = 0 # can_len==0
    fmt='x'
    if(can_len>4):
        #sys.stderr.write("Unknown data length, ignoring\n")
        fmt='x'*(can_len) # BMS
    else:
        try: # canfd or broken data
            if(register_size[canspp_register]<0):
                fmt=('x','b','<h','<l','<l')[can_len]
            else:
                fmt=('x','B','<H','<L','<L')[can_len]
        except Exception:
            pass

        if(can_len>0):
            if(can_len==3): can_data=can_data+b"\0"
            canspp_value, = struct.unpack(fmt, can_data)

    can_len = abs(register_size[canspp_register])
    try:
        if(register_size[canspp_register]<0):
            fmt=('x','b','<h','<l','<l')[can_len]
        else:
            fmt=('x','B','<H','<L','<L')[can_len]
    except Exception:
        pass

    return ( can_id, can_len, can_data, canspp_unknown1, canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited, canspp_value, fmt )


def empty_socket(sock):
    """
    Read outstanding data from (buffered) can sockets; we want to start "now", not "back then"
    """
    emptyto=20000
    try:
        sock.setblocking(False)
        while emptyto>=0:
           sock.recv(16)
           emptyto=emptyto-1
    except BlockingIOError:
        pass
    except:
        pass
    sock.setblocking(True)


#########################################################
#
# main
#

sock = socket.socket(socket.PF_CAN, socket.SOCK_RAW, socket.CAN_RAW)
#interface = "vcan0"
interface = "can2"

try:
    sock.bind((interface,))
except OSError:
    sys.stderr.write("Could not bind to interface '%s'\n" % interface)
    sys.exit(1)

# capture instant power message WR->ZE
lp=ListenPower()
otherstatus=OtherStatus()


# read real GM power straight after inverter
#realgm_sock=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
realgm_sock=socket.socket(socket.PF_CAN, socket.SOCK_RAW, socket.CAN_RAW)
gm_interface= "vcan0"
try:
    #realgm_sock.connect(("172.23.95.11", 2011))
    realgm_sock.bind((gm_interface,))
except OSError:
    sys.stderr.write("Could not bind to interface '%s'\n" % gm_interface)
    sys.exit(1)
except socket.error as exc:
    print("Caught exception socket.error: %s" % exc)
else:
    empty_socket(realgm_sock)
    readgm_receiver = ReceiveOtherData(realgm_sock)


# unsolicited broadcast for heartbeats, status, power
if(not listenonly):
    hb_background = Heartbeat(sock)
    #status_background = Status(sock, 1045)
    #power_background = Power(sock, 1000)
    status_background = Status(sock, 1000)
    power_background = Power(sock, 40)

gm_registers[pid.SUMME_LEISTUNG] = HiresTotalPower(gm_registers[pid.ADE_POWER_L1] + gm_registers[pid.ADE_POWER_L2] + gm_registers[pid.ADE_POWER_L3])
#gm_registers[gm_registers[pid.ADE_POWER_L1]] = HiresTotalPower(gm_registers[pid.ADE_POWER_L1])
#gm_registers[gm_registers[pid.ADE_POWER_L2]] = HiresTotalPower(gm_registers[pid.ADE_POWER_L2])
#gm_registers[gm_registers[pid.ADE_POWER_L3]] = HiresTotalPower(gm_registers[pid.ADE_POWER_L3])
if(True or not listenonly):
    meter_reading = MeterReading(1000)

threading.Timer(2, set_ready).start()
threading.Timer(5, set_oper).start()


#lh=ListenHash()
# 0088.402455 0a104181020000000000000000000000  broadcast  write    1,ZE <-   2,WR    5 (PAC       ) 0 [Watt] (nativ: 0)  (unslctd)
# 0088.399159 a2114181020000000000000000000000  broadcast  write    1,ZE <-   2,WR  209 (PACF      ) 0 [Watt] (nativ: 0)  (unslctd)

# empty buffer, force "reverse" one-shot mode
empty_socket(sock)

# query some registers periodically
pq=PeriodicQuery(sock,1,4,131)


# receive loop
while True:
    try:
        can_pkt = sock.recv(16)
    except KeyboardInterrupt:
        sys.exit(2)

    ( can_id, can_len, can_data, canspp_unknown1, canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited, canspp_value, fmt ) = \
    parse_canspp(can_pkt)


    # query
    # debug: cansend vcan0 82c40841#
    if(canspp_sender!=0 and canspp_solicited==1 and canspp_querytype==12 and (canspp_device==4 and canspp_broadcast==2)):

        if(debug): print( "query unicast:\n"+can_raw_input )

        # answer
        can_id = construct_canid(0, 2, 8, canspp_sender, canspp_device, canspp_register, 0)

        value = gm_registers[canspp_register]
        if(not isinstance(value,int)): value = value()

        can_data = struct.pack(fmt, value)

        if(debug): print("%08x %d %a" % (can_id, can_len, can_data ) )
        can_pkt = struct.pack(can_fmt, can_id, can_len, can_data)
        sock.send(can_pkt)

        continue

    # write unicast
    # debug cansend vcan0 82440841#4711
    if(canspp_sender!=0 and canspp_solicited==1 and canspp_querytype==4 and (canspp_device==4 and canspp_broadcast==2)):

        if(debug): print( "write unicast:\n"+can_raw_input )

        #0035.165916 82440b4704000000109f080c0007ffff    unicast  write    4,GM <-   1,ZE  419 (TC        ) 201891600 [Without] (nativ: 201891600)  ()
        #0035.204248 8201234604000000109f080c00000000    unicast  wrote    1,ZE <-   4,GM  419 Successfully wrote 201891600  (unslctd)

        # wrote
        can_id = construct_canid(0, 2, 0, canspp_sender, canspp_device, canspp_register, 0)

        value = gm_registers[canspp_register]
        if(not isinstance(value,int)):
            value = value(canspp_unknown1, canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited, canspp_value)
        else:
            gm_registers[canspp_register] = canspp_value
            value = gm_registers[canspp_register]

        can_data = struct.pack(fmt, value)

        if(debug): print("%08x %d %a" % (can_id, can_len, can_data ) )
        can_pkt = struct.pack(can_fmt, can_id, can_len, can_data)
        sock.send(can_pkt)

        continue

    # write broadcast
    # debug cansend vcan0 81400b74#
    if(canspp_sender!=0 and canspp_solicited==0 and canspp_querytype==4 and (canspp_device==0 and canspp_broadcast==1)):

        if(debug): print( "write broadcast:\n"+can_raw_input )

        value = gm_registers[canspp_register]
        if(not isinstance(value,int)):
            value = value(canspp_unknown1, canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited, canspp_value)
        else:
            gm_registers[canspp_register] = canspp_value
            value = gm_registers[canspp_register]

        continue

    # read unicast, answer to an asynchronoulsy send read from here
    if(canspp_sender!=0 and canspp_solicited==1 and canspp_querytype==8 and (canspp_device==4 and canspp_broadcast==2)):

        # 0035.206585 8281234604000000109f080c00000000    unicast answer    1,ZE <-   4,GM  419 (TC        ) 201891600 [Without] (nativ: 201891600)  (unslctd)

        if(gm_read_callbacks[canspp_register] is not None):
            gm_read_callbacks[canspp_register](canspp_unknown1, canspp_broadcast, canspp_querytype, canspp_device, canspp_sender, canspp_register, canspp_solicited, canspp_value)
            gm_read_callbacks[canspp_register] = None

        continue

    # write broadcast, other devices
    if(canspp_sender==2 and canspp_solicited==0 and canspp_querytype==4 and canspp_device==1 and canspp_broadcast==1 and \
    canspp_register in [5,209] ):
        # 0088.402455 0a104181020000000000000000000000  broadcast  write    1,ZE <-   2,WR    5 (PAC       ) 0 [Watt] (nativ: 0)  (unslctd)
        # 0088.399159 a2114181020000000000000000000000  broadcast  write    1,ZE <-   2,WR  209 (PACF      ) 0 [Watt] (nativ: 0)  (unslctd)
        lp(canspp_sender, canspp_register, canspp_value) # 50hz!
        continue

    # answer unicast, other devices
    if(canspp_querytype==8 and canspp_broadcast==2 and \
    (canspp_sender, canspp_device, canspp_register) in [(2,1,209)] ):
        # auch answers
        # 0027.176989 a211818202000000ffff000000000000    unicast answer    1,ZE <-   2,WR  209 (PACF      ) -1 [Watt] (nativ: 65535)  (unslctd)
        lp(canspp_sender, canspp_register, canspp_value)
        continue

    # answer unicast, other devices
    if(canspp_querytype==8 and canspp_broadcast==2 and \
    (canspp_sender, canspp_device, canspp_register) in [(1,4,131)] ):
        otherstatus(canspp_sender, canspp_register, canspp_value)
        continue




################################

