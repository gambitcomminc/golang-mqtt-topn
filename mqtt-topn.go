// Copyright (c) 2019 Gambit Communications, Inc.
//
// MQTT-TOPN - display MQTT topic statistics similar to the Unix "top"
// utility

package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	"fmt"
	"bufio"
//	"sort"
	"log"
	"time"
	"strconv"
	"github.com/pborman/getopt/v2"

	gc "github.com/gbin/goncurses"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

// constants
type LogLevel int
const (
	INFO LogLevel = 0
	WARN LogLevel = 1
	ERROR LogLevel = 2
	DEBUG LogLevel = 3
)

const (
	COLOR_INFO = 1
	COLOR_WARN = 2
	COLOR_ERROR = 3
	COLOR_DEBUG = 4
	COLOR_HIGHLIGHT = 5
)

// globals
var gReportSeconds int = 5
var gVerbose int = 0
var gCollapse bool = true
var gTopN = 0
var gBy string = "msgs/s"
var gNonCurses bool = false
var gPaused bool = false
var gHalted bool = false

var gStdscr *gc.Window

type MyConfig struct {
	broker string
	topic string
	user string
	password string
	store string
	qos int
	workers int
}
var gMyConfig = MyConfig { broker: "", topic: "", user: "", password:"", store: "", qos: 0, workers: 1}

// associative array of topics, key is topic name, index is index into
// topic statistics array
var gTopics map[string] int = make (map[string]int)

// array of topic statistics, one for each discovered topic
type TopicStat struct {
	name string
	count int
	bytes int
	lastseen time.Time
}

// we have 2 topic statistics arrays, the one that was last reported (Prev),
// and the one being collected in this report interval (Now)
// that allows us to find the delta for msgs/s and bytes/s statistics
var gTopicStats1 []TopicStat
var gTopicStats2 []TopicStat

// at the end of a report interval, we copy the current values to the previous
// values
var gTopicStats []TopicStat = gTopicStats1[:]
var gTopicPrev []TopicStat = gTopicStats2[:]

func updateTopicStats (name string, bytes int, now time.Time) (error) {
	_, ok := gTopics[name]
	if ! ok {
		topic := TopicStat {
		name: name,
		count: 1,
		bytes: bytes,
		lastseen: now,
		}
		newindex := len (gTopicStats)
		gTopics[name] = newindex
		gTopicStats = append (gTopicStats, topic)
		gSortedTopics = append (gSortedTopics, newindex)
	} else {
		index := gTopics[name]
		gTopicStats[index].count++
		gTopicStats[index].bytes += bytes
		gTopicStats[index].lastseen = now
	}

	return nil
}

func copyTopicStats () (error) {

	gTopicPrev = make ([]TopicStat, len(gTopicStats), cap(gTopicStats))
	copy (gTopicPrev, gTopicStats)

	return nil
}

// sorting support
// insertion sort seems to work well for nearly sorted data
// https://www.toptal.com/developers/sorting-algorithms
var gSortedTopics []int

// largest values first
// new entries (likely with smallest value) get appended
func less (item1 int, item2 int) (bool) {
	if (gBy == "bytes") {
		return gTopicStats[item1].bytes < gTopicStats[item2].bytes
	}
	if (gBy == "msgs") {
		return gTopicStats[item1].count < gTopicStats[item2].count
	}
	if (gBy == "msgs/s") {
		last1 := 0
		if len(gTopicPrev) > item1 {
			last1 = gTopicPrev[item1].count
		}
		delta1 := gTopicStats[item1].count - last1
		last2 := 0
		if len(gTopicPrev) > item2 {
			last2 = gTopicPrev[item2].count
		}
		delta2 := gTopicStats[item2].count - last2
		return delta1 < delta2
	}

	return false
}

func sortTopics () (error) {

	for i := 1; i < len (gSortedTopics); i++ {
		j := i
		for j > 0 {
			if less (gSortedTopics[j-1], gSortedTopics[j]) {
				gSortedTopics[j-1], gSortedTopics[j] = gSortedTopics[j], gSortedTopics[j-1]
			}
			j = j - 1
		}
	}
	return nil
}

// worker handles MQTT messages
func mqttWorker (inChan <-chan [2]string, doneChan chan<- bool, workerno int) {
	if gVerbose > 0 {
		fmt.Printf("DEBUG: mqttWorker %d start\n", workerno)
	}
	receiveCount := 0
	lastReportedCount := 0
	lastReportedTime := time.Now()
	countBytes := 0
	lastReportedWasZero := false
	for {
		// synchronization point: receive a MQTT message
		incoming := <-inChan

		// timer message comes on illegal topic #timer
		now := time.Now()
		if incoming[0] == "#done" {
			break
		}
		if incoming[0] == "#pause" {
			gPaused = true
			displayStatusLine (DEBUG, "DEBUG: paused")
			continue
		}
		if incoming[0] == "#halt" {
			gHalted = true
			displayStatusLine (DEBUG, "DEBUG: halted")
			continue
		}
		if incoming[0] == "#resume" {
			gPaused = false
			gHalted = false
			displayStatusLine (DEBUG, "DEBUG: resumed")
			continue
		}

		// in halted state, discard all messages
		if gHalted {
			continue
		}

		if incoming[0] == "#report" {
			elapsed := now.Sub(lastReportedTime)
			msgPerSecond := float64(receiveCount - lastReportedCount) / elapsed.Seconds()
			bytesPerSec := float64(countBytes) / elapsed.Seconds()
			var bytesPerMsg float64
			var do_print bool = true
			if msgPerSecond > 0.0 {
				bytesPerMsg = bytesPerSec / msgPerSecond
				lastReportedWasZero = false
			} else {
				bytesPerMsg = 0.0

				// collapse 0-msg intervals
				// to avoid many lines with msgs/s 0.0
				if lastReportedWasZero {
					if gCollapse {
						do_print = false
					}
				}
				lastReportedWasZero = true
			}

			// if paused, then don't report, but keep incrementing
			// stats
			if gPaused {
				continue
			}

			lastReportedTime = now
			lastReportedCount = receiveCount
			countBytes = 0

			if ! do_print {
				continue
			}

			displayHighlight (fmt.Sprintf ("%s: msgs/s %.1f, bytes/s %.1f, bytes/msg %.1f, topics %d", now.Format("01/02.03:04:05"), msgPerSecond, bytesPerSec, bytesPerMsg, len(gTopics)))

			// optionally, chattiest top N topics
			// if interactive, the topN fill the window
			topn := gTopN
			if ! gNonCurses {
				y, _ :=  gStdscr.MaxYX ()
				// minus top line, highlight line and statusbar
				// and -1 for being 0-based
				topn = y - 3 -1
			}
			if topn > 0 {
				err := sortTopics ()
				if err != nil {
					panic(err)
				}

				count := 0
				displayPrintln ("- msgs ---- msgs/s ---- bytes -- bytes/s - topic -------------");
				for i := range gSortedTopics {
					if count >= topn {
						break
					}
					count++

					msgs := gTopicStats[gSortedTopics[i]].count
					// did we have a count last time?
					var lastmsgs int
					if len(gTopicPrev) > gSortedTopics[i] {
						lastmsgs = gTopicPrev[gSortedTopics[i]].count
					} else {
						lastmsgs = 0
					}
					msgPerSec := float64(msgs - lastmsgs) / elapsed.Seconds()
					bytes := gTopicStats[gSortedTopics[i]].bytes
					bytesPerSec := 0.0
					if msgPerSec > 0.0 {
						lastbytes := 0
						if len(gTopicPrev) > gSortedTopics[i] {
							lastbytes = gTopicPrev[gSortedTopics[i]].bytes
						}
						bytesPerSec = float64(bytes - lastbytes) / elapsed.Seconds()
					}
					displayPrintln (fmt.Sprintf ("%9d %9.1f %9d %9.1f %s", msgs, msgPerSec, bytes, bytesPerSec, gTopicStats[gSortedTopics[i]].name))

				} 
			}

			// copy the current statistics to the previous
			copyTopicStats ()
			continue
		}

		// MQTT message received
		receiveCount++
		bytes := len(incoming[1])
		countBytes = countBytes + bytes

		// handle a MQTT message
		if gVerbose > 0 {
			fmt.Printf("DEBUG: worker %d: topic %s, msg: %s\n", workerno, incoming[0], incoming[1])
		}

		// store in the gTopics map
		err := updateTopicStats (incoming[0], bytes, now)
		if err != nil {
			panic(err)
		}
	}

	doneChan <- true
}

// worker handles timer ticks
func timerWorker (inChan chan<- [2]string, doneChan chan<- bool) {
	iter := 0
	reportInterval := time.Second * time.Duration(gReportSeconds)
	for {
		// synchronization point: every report interval
		time.Sleep(reportInterval)
		iter++

		inChan <- [2]string{"#report", strconv.Itoa(iter)}
	}
}

// handles interactive keyboard input
func keyboardWorker (inChan chan<- [2]string, doneChan chan<- bool) {
	for {
		var mych string
		if gNonCurses {
			reader := bufio.NewReader(os.Stdin)
			ch, _, err := reader.ReadRune()
			if err != nil {
				continue
			}
			mych = string(ch)
		} else {
			ch := gStdscr.GetChar()
			mych =  gc.KeyString(ch)
		}

		switch mych {
		case "q":
			inChan <- [2]string{"#done"}
		case "p":
			inChan <- [2]string{"#pause"}
		case "h":
			inChan <- [2]string{"#halt"}
		case "r":
			inChan <- [2]string{"#resume"}
//		case "down":
//			menu.Driver(goncurses.REQ_DOWN)
//		case "up":
//			menu.Driver(goncurses.REQ_UP)
		}
	}
}

func parseCommandLineOptions() (error) {
	optHost := getopt.StringLong("host", 'h', "", "broker host")
	optPort := getopt.StringLong("port", 'p', "1883", "port (default 1883)")
	optTopic := getopt.StringLong("topic", 't', "#", "topic (default #)")
	optQos := getopt.IntLong("qos", 'q', 0, "QOS (default 0)")
	optUser := getopt.StringLong("user", 'u', "", "username")
	optPassword := getopt.StringLong("password", 'P', "", "password")
	optStore := getopt.StringLong("store", 's', ":memory:", "store directory (default :memory)")
	optWorkers := getopt.IntLong("workers", 'w', 1, "workers (default 1)")
	optHelp := getopt.BoolLong("help", '?', "Help")
	optVerbose := getopt.IntLong("verbose", 'v', 0, "verbose (default 0)")
	optReport := getopt.IntLong("report", 'r', 0, "report interval (default 5)")
	getopt.FlagLong(&gCollapse, "collapse", 'c', "collapse 0-msg intervals (default on)")

	// topic reporting
	optTopN := getopt.IntLong("topn", 'T', 0, "top N topics (default none)")
	optBy := getopt.StringLong("by", 'B', "msgs/s", "by column (default msgs/s)")
	getopt.FlagLong(&gNonCurses, "non-terminal", 'N', "non-terminal (default terminal)")

	getopt.Parse()

	if *optHelp {
		getopt.Usage()
		os.Exit(0)
	}

	if *optHost == "" {
		fmt.Println("ERROR: Need --host ")
		getopt.Usage()
		os.Exit(-1)
	}

	if *optTopic == "" {
		fmt.Println("ERROR: Need --topic ")
		getopt.Usage()
		os.Exit(-1)
	}

	if *optVerbose > 0 {
		gVerbose = *optVerbose
	}

	if *optReport > 0 {
		gReportSeconds = *optReport
	}

	if *optTopN > 0 {
		gTopN = *optTopN
	}

	if *optBy == "" {
		fmt.Println("ERROR: Need --by ")
		getopt.Usage()
		os.Exit(-1)
	}
	gBy = *optBy

	gMyConfig.broker = *optHost + ":" + *optPort
	gMyConfig.topic = *optTopic
	gMyConfig.user = *optUser
	gMyConfig.password = *optPassword
	gMyConfig.store = *optStore
	gMyConfig.qos = *optQos
	gMyConfig.workers = *optWorkers
	return nil
}

// should be called after parsing command line options
func displayInit() (error) {
	// nothing to do
	if gNonCurses {
		return nil
	}

	// You should always test to make sure ncurses has initialized properly.
	// In order for your error messages to be visible on the terminal you will
	// need to either log error messages or output them to to stderr.
	stdscr, err := gc.Init()
	if err != nil {
		log.Fatal("init:", err)
	}
	gStdscr = stdscr

	// Turn off character echo, hide the cursor and disable input buffering
	gc.Echo(false)
	gc.CBreak(true)
	gc.Cursor(0)

	// HasColors can be used to determine whether the current terminal
	// has the capability of using colours. You could then chose whether or
	// not to use some other mechanism, like using A_REVERSE, instead
	if !gc.HasColors() {
		log.Fatal("Example requires a colour capable terminal")
	}

	// Must be called after Init but before using any colour related functions
	if err := gc.StartColor(); err != nil {
		log.Fatal(err)
	}

	// Initialize a colour pair. Should only fail if an improper pair value
	// is given
	// this is the INFO color
	if err := gc.InitPair(COLOR_INFO, gc.C_GREEN, gc.C_WHITE); err != nil {
		log.Fatal("InitPair failed: ", err)
	}

	if err = gc.InitPair(COLOR_WARN, gc.C_YELLOW, gc.C_BLACK); err != nil {
		log.Fatal("InitPair failed: ", err)
	}

	if err = gc.InitPair(COLOR_ERROR, gc.C_RED, gc.C_WHITE); err != nil {
		log.Fatal("InitPair failed: ", err)
	}

	if err = gc.InitPair(COLOR_DEBUG, gc.C_BLACK, gc.C_CYAN); err != nil {
		log.Fatal("InitPair failed: ", err)
	}

	if err = gc.InitPair(COLOR_HIGHLIGHT, gc.C_BLACK, gc.C_WHITE); err != nil {
		log.Fatal("InitPair failed: ", err)
	}

	gStdscr.Keypad(true)

	gStdscr.Refresh()

	go catchSigwinch ()

	return nil
}


// handle terminal resize signal
var wg sync.WaitGroup

func catchSigwinch () {
	wg.Add(1)
	resizeChannel := make(chan os.Signal)
	signal.Notify(resizeChannel, syscall.SIGWINCH)
	go onResize(resizeChannel)
	wg.Wait()
}

func onResize(sigchannel chan os.Signal) {

	gStdscr.ScrollOk(true)
	gc.NewLines(true)
	for {
		<-sigchannel
		gc.StdScr().Clear()
		gc.End()
		gc.Update()

		y, x := gc.StdScr().MaxYX()
		displayStatusLine (DEBUG, "DEBUG: resized " + strconv.Itoa(x) + ", " + strconv.Itoa(y))
		gc.ResizeTerm(y, x)

	}
	wg.Done()
}

func init() {
	err := parseCommandLineOptions ()
	if err != nil {
		log.Fatal("init:", err)
	}

	err = displayInit ()
	if err != nil {
		log.Fatal("init:", err)
	}

}

func displayTitle (line string) {
	if gNonCurses {
		fmt.Println (line)
		return
	}

	gStdscr.MovePrint(0, 0, line)
	gStdscr.Refresh()
}

func displayStatusLine (level LogLevel, args... interface{}) {
	if gNonCurses {
		fmt.Println (args)
		return
	}

	switch level {
	case INFO:
		gStdscr.ColorOn(COLOR_INFO)
	case WARN:
		gStdscr.ColorOn(COLOR_WARN)
	case ERROR:
		gStdscr.ColorOn(COLOR_ERROR)
	case DEBUG:
		gStdscr.ColorOn(COLOR_DEBUG)
	}

	// status line is last line of window
	y, _ :=  gStdscr.MaxYX ()
	gStdscr.MovePrint(y-1, 0, args)
	gStdscr.ColorOff(1)
	gStdscr.Refresh()
}

// print a line in the highlight area underneath menu
func displayHighlight (line string) {
	if gNonCurses {
		fmt.Println (line)
		return
	}

	gStdscr.ColorOn(COLOR_HIGHLIGHT)

	gStdscr.MovePrint(1, 0, line + "\n")
	gStdscr.ColorOff(COLOR_HIGHLIGHT)
	gStdscr.Refresh()
}

// print a line in the screen, truncating what does not fit
func displayPrintln (line string) {
	if gNonCurses {
		fmt.Println (line)
		return
	}

	_, x :=  gStdscr.MaxYX ()
	todisplay := line
	if x < len(line) {
		todisplay = line[:x-1]
	}
	gStdscr.Println (todisplay)
	gStdscr.Refresh()
}

// called on exit
func cleanup() {
	// clean up the display on exit
	if ! gNonCurses {
		gc.End()
	}
}

func main() {

	// on exit
	defer cleanup()

	now := time.Now()
	displayTitle (now.Format("01/02.03:04:05") + ": MQTT-TOPN -- q: exit, p: pause, h: halt")

	displayStatusLine (INFO, now.Format("01/02.03:04:05") + ": Subscribed to topic " + gMyConfig.topic + " on " + gMyConfig.broker)
	
	mqttOpts := MQTT.NewClientOptions()
	mqttOpts.AddBroker(gMyConfig.broker)
	mqttOpts.SetUsername(gMyConfig.user)
	mqttOpts.SetPassword(gMyConfig.password)
	if gMyConfig.store != ":memory:" {
		mqttOpts.SetStore(MQTT.NewFileStore(gMyConfig.store))
	}

	inChan := make(chan [2]string)

	// automatically reconnect and subscribe again
	mqttOpts.SetOnConnectHandler(func(client MQTT.Client) {
		if gVerbose > 0 {
			displayStatusLine (DEBUG, "DEBUG: connected, subscribing...")
		}
		if token := client.Subscribe(gMyConfig.topic, byte(gMyConfig.qos), nil); token.Wait() && token.Error() != nil {
			fmt.Println(token.Error())
			return
		}
	})

	mqttOpts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		inChan <- [2]string{msg.Topic(), string(msg.Payload())}
	})

	client := MQTT.NewClient(mqttOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// separate MQTT message handler thread(s)
	doneChan := make(chan bool, gMyConfig.workers)
	for workerno := 0; workerno < gMyConfig.workers; workerno++ {
		if gVerbose > 0 {
			displayStatusLine (DEBUG, "DEBUG: go worker %d\n", workerno)
		}
		go mqttWorker (inChan, doneChan, workerno)
	}

	// timer ticks
	go timerWorker (inChan, doneChan)

	// interactive key handling
	go keyboardWorker (inChan, doneChan)

	// synchronization point
	<- doneChan

	client.Disconnect(500)

}
