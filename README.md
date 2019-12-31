# golang-mqtt-topn
MQTT-TOPN: display MQTT topic statistics

## Overview

This is a simple MQTT subscriber client in Go to display topic performance statistics
with TopN most "chatty" topics.

It is the command-line interface equivalent of https://github.com/gambitcomminc/mqtt-stats
implemented with Go ncurses https://godoc.org/github.com/gbin/goncurses

## Installation / Requirements

* To install this program, you need to install Go and set your Go workspace first.

* Follow instructions for Eclipse Paho Go library https://github.com/eclipse/paho.mqtt.golang

* go get github.com/pborman/getopt/v2

* go get github.com/gbin/goncurses

* go build mqtt-topn.go

## Usage

Non-curses mode for redirecting into files:

    % ./mqtt-topn --host mqtt.eclipse.org --topic # --verbose 0 --report 1 -N -T 5
    12/31.12:49:51: MQTT-TOPN -- q: exit, p: pause, h: halt
    [12/31.12:49:51: Subscribed to topic # on mqtt.eclipse.org:1883]
    12/31.12:49:52: msgs/s 2218.1, bytes/s 263927.1, bytes/msg 119.0, topics 2149
    - msgs ---- msgs/s ---- bytes -- bytes/s - topic -------------
       14      14.0        70      69.9 shpi/motion
       14      14.0       236     235.6 shpi/lastmotion
        7       7.0        84      83.9 /merakimv/Q2GV-P692-B34D/light
        5       5.0       165     164.7 /merakimv/Q2GV-UJ9A-JRJD/raw_detections
        4       4.0       172     171.7 /merakimv/Q2GV-UJ9A-JRJD/0
    12/31.12:49:53: msgs/s 133.8, bytes/s 6175.6, bytes/msg 46.2, topics 2173
    - msgs ---- msgs/s ---- bytes -- bytes/s - topic -------------
       23       8.9       115      44.6 shpi/motion
       23       8.9       380     142.7 shpi/lastmotion
        6       5.9       198     196.2 /merakimv/Q2GV-P692-B34D/raw_detections
        6       5.9       258     255.7 /merakimv/Q2GV-P692-B34D/0
        6       5.9       258     255.7 /merakimv/Q2GV-P692-B34D/667095694804254788
    12/31.12:49:54: msgs/s 158.8, bytes/s 9315.8, bytes/msg 58.7, topics 2184
    - msgs ---- msgs/s ---- bytes -- bytes/s - topic -------------
       34      11.0       170      54.9 shpi/motion
       34      11.0       562     181.8 shpi/lastmotion
       12       7.0       516     300.6 /merakimv/Q2GV-UJ9A-JRJD/0
       12       7.0       516     300.6 /merakimv/Q2GV-UJ9A-JRJD/615867249042915441
       12       7.0       516     300.6 /merakimv/Q2GV-UJ9A-JRJD/615867249042915443

Curses mode video at https://www.youtube.com/watch?v=M1c--mXYl_Q
