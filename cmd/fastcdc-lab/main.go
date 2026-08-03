// Command fastcdc-lab measures FastCDC chunk distributions and cross-snapshot
// deduplication. It writes canonical CSV for review or plotting.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
)

const usageText = `usage: fastcdc-lab <command> [options]

Commands:
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
		return fmt.Errorf("missing command")
	}

	switch args[0] {
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
