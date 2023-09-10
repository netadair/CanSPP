#!/usr/bin/perl

#
# parse_canspp.pl
#
# Parse Solutronic Solhybrid devices' (inverter, DCDC, meter, BMS) protocol on the CAN bus and output it in human-readable format.
#
#  Apr 16  2020    Michael Rausch    Initial version
#

use warnings; use strict;

use IO::Socket::INET;
use Socket;
use Socket qw(IPPROTO_TCP TCP_NODELAY);

use Time::HiRes qw(gettimeofday);

use Data::Dump qw(pp);

use FindBin qw($Bin);
use lib "$Bin/.";
use solhybrid_spp_registers;
#use solhybrid_spp;
use solhybrid_spp_output;

no warnings 'experimental::smartmatch';

# auto-flush on socket
$| = 1;

my $nosppcskip=$ARGV[1]; # no skipping
my $sppc_skip=5; # skip displaying messages unless n seconds old

#my $peerhost='127.0.0.1';
my $peerhost='localhost';
my $port=2009;

my $peerloc=$ARGV[0] || 'nada';
if ($peerloc eq 'xxx-1') { $peerhost='1.2.3.4'; $port=2010; } # can2 ze

#my $socket = solhybrid_spp::connect($peerhost);

# socat INTERFACE:slcan0,pf=29,type=3,prototype=1 tcp-listen:2010,reuseaddr,fork &
# socat INTERFACE:slcan1,pf=29,type=3,prototype=1 tcp-listen:2011,reuseaddr,fork &
#
# socat INTERFACE:can2,pf=29,type=3,prototype=1 tcp-listen:2010,reuseaddr,fork &
# socat INTERFACE:can0,pf=29,type=3,prototype=1 tcp-listen:2011,reuseaddr,fork &
# socat INTERFACE:can1,pf=29,type=3,prototype=1 tcp-listen:2012,reuseaddr,fork &

die "Specify relais label" if ($peerloc eq 'nada');

#sub connect($) {
#        my $peerhost = shift;
        #my $port = shift;
        #my $port = 2010;
        #my $port = 2010;

        # create a connecting socket
        my $socket = new IO::Socket::INET (
            PeerHost => $peerhost,
            PeerPort => $port,
            Proto => 'tcp',
            #Proto => 'udp',
        );

        die "cannot connect to the server $!\n" unless $socket;

        #setsockopt($socket,IPPROTO_TCP,TCP_NODELAY,1);

#        $microservice_enabled=0;

        printf STDERR "connected to the CAN relay at %s:%s\n", $peerhost, $port;
#        return $socket;
#}


        my $sppc_cache;

        my $cantemplate = 'L<CXCxxx/a';
        #my $cantemplate = 'L<CXCxxx/(C)';
        $cantemplate =~ s/\</\>/ if (int($port/100)%2==1);

        my $cnt=0;
        while(1==1) {

                # receive a response of up to 1024 characters from server
                my $response = '';
                $socket->recv($response, 16);

                my $loopc=15;
                while(length($response) != 16 and $loopc>0) {
                        my $missingbytes = 16 - length($response);
                        #printf STDERR "Error, didn't receive 16 bytes only %d.\n", length($response);
                        my $missingresp = '';
                        $socket->recv($missingresp, $missingbytes);
                        $response.=$missingresp;
                        $loopc--;
                }
                if($loopc==0) {
                        printf STDERR "Error, excessive receive loop time\n";
                        next;
                }

                $cnt++;
                my $resphex=unpack("H32", $response);

                my ($id, $len, $data) = unpack($cantemplate, $response);

                my $eff = ($id & 0x80000000);
                my $rtr = ($id & 0x40000000);
                my $err = ($id & 0x20000000);
                $id &= ($eff?0x1fffffff:0x7ff);

                #printf "canx  RX %1s %1s  %08x [%d] "." %02x"x($len) ."\n", ($rtr?'R':'-'), ($err?'E':'-'), $id, $len, @data;
                #printf "canx  RX %1s %1s  %08x [%d] %s\n", ($rtr?'R':'-'), ($err?'E':'-'), $id, $len, unpack("H16", $data);

                #printf STDERR "Error: %1s %1s  %08x [%d] %s, skipping.\n", ($rtr?'R':'-'), ($err?'E':'-'), $id, $len, pack("H16", $data) if ($rtr or $err);
                next if $rtr or $err;
                #die if $rtr or $err;

                my $canspp_unknown1     = ($id & 0x10000000) >> 28;
                my $canspp_broadcast    = ($id & 0x0f000000) >> 24;
                my $canspp_querytype    = ($id & 0x00e00000) >> 20;
                my $canspp_device       = ($id & 0x001f0000) >> 16; # per doku canspp device ids are 5bit; special ids 0x0 or 0x1f ?

                my $canspp_flag1        = ($id & 0x00008000) >> 15;
                my $canspp_flag2        = ($id & 0x00004000) >> 14;
                #my $canspp_sender      = ($id & 0x00003800) >> 11;
                my $canspp_sender       = ($id & 0x0000f800) >> 11;

                my $canspp_register     = ($id & 0x000007fe) >>  1;
                my $canspp_flag_read    = ($id & 0x00000001) >>  0; # merely (active request? in contrast to unsolicited?)


                ## skip some stupid bit errors
                #next if (0 and $id ~~ [
                #               0x00000000, 0x1a010000, 0x00000108  # often on a2
                #       ]);

                if (
                        !($canspp_unknown1 == 0 ) or
                        !($canspp_broadcast ~~ [1,2] ) or
                        !($canspp_querytype ~~ [4,8,12,2,0,10] ) or
                        !($canspp_device ~~ [0,1,2,3,4,5] ) or
                        !($canspp_flag1 == 0) or
                        !($canspp_flag2 == 0)
                        )
                {
                        my $canspp_value        = unpack( ($len==4)?'L<':($len==2)?'S<':'', $data) || 0; # len==0 ...

                        printf STDERR "Error, unexpected flag in %08x, %d %d %d %d %d %d %d %08x; raw %s\n", $id, $canspp_unknown1, $canspp_broadcast, $canspp_querytype, $canspp_device, $canspp_flag1, $canspp_flag2, $len, $canspp_value, $resphex;
                        #printf STDERR "Error, unexpected flag in %08x, %d %d %d %d %d %d .\n", $id, $canspp_unknown1, $canspp_broadcast, $canspp_querytype, $canspp_device;
                        next;
                }

# /------>  1=broadcast, 2=unicast?
# |/----->  4=write, 8=answer, c=query, 2=error, 0=error, 10=error?
# ||/--->   geräte id ziel? 0=alle?
# |||
# |||/+++-->       registernummer     abc|d 1|xxx xxxx xxx|0    write error file vdew  | bit 13 c = registerspiegel???  | bit 12 d=1 bei antworten?    | bit 11 master/indirect?  | 10 bits  11-1, | Bit 0 gesetzt: read
#02C20803
#02C30805
#02811804
#02430B76
#02420B76
#01401374
#
#01411802
#014111BC
#
#01412040
#
#022110B0
#
#02C308DD
#028118DC
#
#01402374
#01400B74
#01401374
#01401B74

                my $canspp_value        = unpack( ($len==4 or $len==3)?'L<':($len==2)?'S<':($len==1)?'C':'', $data."\0") || 0; # len==0 ...

                # 0054.949936 070a4482 03000000 100001 f800000000    unicast  write    4,GM <-   1,ZE  259 (CFG1      ) 0x00000000 [Without] (nativ: 0)  ()
                $canspp_value &= 0xffffff if($len==3); # strange error?! sometimes 3 byte writes and 2 bytes answers

                my $canspp_bms_special63 = ($canspp_sender == 5 and $canspp_register == 63);
                my $s63tab = "\t\t\t\t\t\t\t\t\t\t  ";
                my $s63eol="\n";

# 82c5187f000000000000000000000000    unicast  query   5,BMS <- 3,DCDC   63 (MAX_CHARGE_DISCHARGE_STATUS)  (read )
# 8283287f080a9fc80030881388130300    unicast answer  3,DCDC <-  5,BMS   63   (read )
# (Pseudo parameter forconcurrent reading of parameter #32, #60, #61, #99 and #146. See 5.5.2 )
# 8283287f080a9fc8 0030 8813 8813 0300    unicast answer  3,DCDC <-  5,BMS   63   (read )
                my $canspp_bms_special63text = ($canspp_bms_special63 and $canspp_querytype~~[4,8] and $len==8) ? sub {
                                                my (@canspp_values)=unpack('CCS<S<CC', $data);
                                                        format_value($canspp_sender,146,$canspp_values[0],1).$s63eol.
                                                $s63tab.format_value($canspp_sender, 99,$canspp_values[1],1).$s63eol.
                                                $s63tab.format_value($canspp_sender, 61,$canspp_values[2],1).$s63eol.
                                                $s63tab.format_value($canspp_sender, 60,$canspp_values[3],1).$s63eol.
                                                $s63tab.format_value($canspp_sender, 32,$canspp_values[4],1)."\t";
                                                # $canspp_values[5] == 0
                                                }->() :'';

# 82430b76040a9fc80300000000000000    unicast  write  3,DCDC <-   1,ZE  443 (EMSCMD    ) 3 [Without] (nativ: 3)  () dcdc

# trinfo 3 = discahrge?
#=1310 51e
# printf "%x" 4090 ffa

                my $canspp_special443 = ($canspp_register == 443);
                my $canspp_special443text = '';
                $canspp_special443text = sub {
                                                my $reg23 = ($canspp_value & 0xf);
                                                my $reg25 = ($canspp_value >> 4)&0xffff; # # fff???
                                                my $reg79 = ($canspp_value >> 20)&0xfff; # ???
                                                        format_value($canspp_device, 443,$canspp_value,1).$s63eol.
                                                $s63tab.format_value($canspp_device, 23,$reg23,1).$s63eol.
                                                ($s63tab."\t\t\t".('0','1','2','3','4','5','6','WR_FEED_IN','WR_GRID_LOADING','GRID_LOADING?','10','11','12','PV_FEEDIN?','14','15')[$reg23].$s63eol). # SPPC_CMD_xxx
                                                $s63tab.format_value($canspp_device, 25,$reg25,1).$s63eol.
                                                $s63tab.format_value($canspp_device, 79,$reg79,1)."\t";
                                        }->()  if ($canspp_special443 and $canspp_querytype~~[4,8] and $canspp_device==2);

                $canspp_special443text = sub {
                                                my $reg23 = ($canspp_value & 0xf);
                                                my $reg59 = ($canspp_value >> 4)&0xffff; # ???
                                                my $reg79 = ($canspp_value >> 20)&0xfff; # ???
                                                        format_value($canspp_device, 443,$canspp_value,1).$s63eol.
                                                $s63tab."\t\t\t".sprintf("%d", $reg79).$s63eol.
                                                $s63tab."\t\t\t".'SPPC_CMD_DCDC_'.('0','1','2_IDLE','3_NOTHING?','4','5','6_DISCHARGE','7','8','9','10','11','12','13','14','15')[$reg23].$s63eol.
                                                $s63tab.format_value($canspp_device, 59,$reg59,1)."\t";
                                        }->()  if($canspp_special443 and $canspp_querytype~~[4,8] and $canspp_device==3);

#  unicast  write 2   WR  443 86408  (indirect ) 82420b76042287048851010000000000
#  unicast  write 3 DCDC  443 64002  (indirect ) 82430b760422870402fa000000000000
                #next if ($canspp_querytype == 4 and $canspp_device == 2 and $canspp_register == 443 and ($canspp_value || 0) == 0x015188 );
                #next if ($canspp_querytype == 4 and $canspp_device == 3 and $canspp_register == 443 and ($canspp_value || 0) == 0x00fa02 );

                my ($seconds, $microseconds) = gettimeofday; # %10d.%06d

                my $sppc_cache_key = "$canspp_broadcast.$canspp_querytype.$canspp_device.$canspp_sender.$canspp_register";

                if(defined($sppc_cache->{$sppc_cache_key})) {

                        if(
                             ($sppc_cache->{$sppc_cache_key}->{canspp_value} == ($canspp_value||0) )
                             and not (($sppc_cache->{$sppc_cache_key}->{timestamp} + $sppc_skip ) < "$seconds.$microseconds")
                             and not ($nosppcskip)
                          ) {
                                $sppc_cache->{$sppc_cache_key}->{occurance}++;
                                next;
                        }

                }

                printf "%04d.%06d %s  %9s %6s  %6s <- %6s  %3d %s  (%s)\n",
                        $seconds%1000, $microseconds, $resphex,
                        ('-', 'broadcast', 'unicast', '-')[$canspp_broadcast],
                        ('wrote', '-', 'error', '-', 'write', '-', '-','-', 'answer', '-', 'error', '-', 'query')[$canspp_querytype],
                        ('*,ALL', '1,ZE', '2,WR', '3,DCDC', '4,GM', '5,BMS')[$canspp_device],
                        ('*,ALL', '1,ZE', '2,WR', '3,DCDC', '4,GM', '5,BMS')[$canspp_sender],
                        $canspp_register,
                        ($canspp_special443)?"$canspp_special443text":
                        ($canspp_bms_special63)?"$canspp_bms_special63text":
                        ($canspp_querytype==0)?"Successfully wrote $canspp_value":
                        ($canspp_querytype==2)?"Error2=$canspp_value":
                        ($canspp_querytype==10)?"Error10=$canspp_value":
                        ($canspp_querytype==12)?format_value($canspp_device,$canspp_register,0xAAAA5555):
                        ($canspp_querytype~~[4])?format_value($canspp_sender,$canspp_register,$canspp_value):
                        ($canspp_querytype~~[8])?format_value($canspp_sender,$canspp_register,$canspp_value):
                        $canspp_value||'',
                        ('unslctd', '')[$canspp_flag_read],
                        ;

                $sppc_cache->{$sppc_cache_key} = {
                        timestamp=>"$seconds.$microseconds",
                        canspp_value=>$canspp_value||0,
                        occurance=>1
                        };

        }




$socket->close();
