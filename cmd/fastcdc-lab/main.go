// Command fastcdc-lab provides reproducible experiments for fastcdc-go.
//
// It is deliberately separate from the end-user fastcdc command. Its output
// formats favor reviewable data over a polished interactive interface.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
)

const usageText = `usage: fastcdc-lab <command> [options]

Commands:
  bench         measure streaming throughput on deterministic input
  distribution  write an observed and analytical chunk-size distribution
  dedup         compare deduplication across directories or Git revisions

Run "fastcdc-lab <command> -help" for command-specific options.
`

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if err == flag.ErrHelp {
			return
		}
		_, _ = fmt.Fprintln(os.Stderr, "fastcdc-lab:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		if _, err := io.WriteString(stderr, usageText); err != nil {
			return err
		}
		return flag.ErrHelp
	}

	switch args[0] {
	case "bench":
		return runBench(args[1:], stdout, stderr)
	case "distribution":
		return runDistribution(args[1:], stdout, stderr)
	case "dedup":
		return runDedup(args[1:], stdout, stderr)
	case "help", "-help", "--help", "-h":
		_, err := io.WriteString(stdout, usageText)
		return err
	default:
		if _, err := io.WriteString(stderr, usageText); err != nil {
			return err
		}
		return fmt.Errorf("unknown command %q", args[0])
	}
}
