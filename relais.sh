# read can/vcan devices and present as raw data on a tcp port

nohup socat INTERFACE:can2,pf=29,type=3,prototype=1 tcp-listen:2010,reuseaddr,fork &
nohup socat INTERFACE:can0,pf=29,type=3,prototype=1 tcp-listen:2011,reuseaddr,fork &
nohup socat INTERFACE:can1,pf=29,type=3,prototype=1 tcp-listen:2012,reuseaddr,fork &

# or loop via remote and inject into vcan

#!/bin/bash

exec >/dev/null 2>&1

while true; do
        socat -b16 -T60 tcp:172.23.95.11:2011  INTERFACE:vcan0,pf=29,type=3,prototype=1
        sleep 10
done

