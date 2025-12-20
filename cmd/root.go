package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Version information - set via ldflags during build
var (
	Version   = "1.0.0"
	BuildDate = "unknown"
	GitCommit = "unknown"
)

var rootCmd = &cobra.Command{
	Use:   "sql-splitter",
	Short: "Split large SQL dump files into individual table files",
	Long: `A high-performance CLI tool for splitting large SQL dump files.

sql-splitter uses efficient streaming and buffering techniques to process
gigabyte-scale SQL files with minimal memory usage. It splits files based on
CREATE TABLE statements and routes INSERTs to their respective table files.`,
	Version: Version,
}

// Execute runs the root command
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	// Custom version template for more detailed output
	rootCmd.SetVersionTemplate(`sql-splitter {{.Version}}
`)
}
