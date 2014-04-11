// Command logrotate writes and rotates logs read from stdin.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/moshee/logrotate/rotator"
)

var (
	flagT = flag.Bool("t", false, "Behave like tee(1)")
	flagC = flag.Int("c", 5000, "Max (uncompressed) logfile size in kB")
)

func init() {
	log.SetFlags(0)
	log.SetPrefix(os.Args[0] + ": ")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: <process that outputs to stdout> | logrotate [-t] [-c <N>] <filename>")
		flag.PrintDefaults()
	}
	flag.Parse()
}

func main() {
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	r, err := rotator.New(os.Stdin, flag.Arg(0), int64(*flagC), *flagT)
	if err != nil {
		log.Fatal(err)
	}
	defer r.Close()

	if err := r.Run(); err != nil {
		log.Print(err)
		return
	}
}
