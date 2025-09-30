package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/helgesverre/sql-splitter/internal/splitter"
	"github.com/spf13/cobra"
)

var (
	outputDir string
	verbose   bool
)

var splitCmd = &cobra.Command{
	Use:   "split [file]",
	Short: "Split a SQL file into individual table files",
	Long: `Split a large SQL dump file into individual files, one per table.

The split command reads the SQL file and creates separate output files for
each table found. CREATE TABLE, INSERT, and related statements are routed
to the appropriate table file.

Example:
  sql-splitter split large-dump.sql --output=tables
  sql-splitter split database.sql -o output -v`,
	Args: cobra.ExactArgs(1),
	RunE: runSplit,
}

func init() {
	rootCmd.AddCommand(splitCmd)

	splitCmd.Flags().StringVarP(&outputDir, "output", "o", "output", "Output directory for split files")
	splitCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output")
}

func runSplit(cmd *cobra.Command, args []string) error {
	inputFile := args[0]

	// Check if input file exists
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		return fmt.Errorf("input file does not exist: %s", inputFile)
	}

	// Get file info for display
	fileInfo, err := os.Stat(inputFile)
	if err != nil {
		return fmt.Errorf("failed to stat input file: %w", err)
	}

	fmt.Printf("Splitting SQL file: %s (%.2f MB)\n", inputFile, float64(fileInfo.Size())/(1024*1024))
	fmt.Printf("Output directory: %s\n", outputDir)
	fmt.Println()

	// Create splitter
	s := splitter.NewSplitter(inputFile, outputDir)

	// Start timing
	startTime := time.Now()

	// Perform split
	if err := s.Split(); err != nil {
		return fmt.Errorf("split failed: %w", err)
	}

	// Get statistics
	stats := s.GetStats()
	elapsed := time.Since(startTime)

	// Display results
	fmt.Println("✓ Split completed successfully!")
	fmt.Printf("\nStatistics:\n")
	fmt.Printf("  Statements processed: %d\n", stats.StatementsProcessed)
	fmt.Printf("  Bytes processed: %.2f MB\n", float64(stats.BytesProcessed)/(1024*1024))
	fmt.Printf("  Elapsed time: %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Throughput: %.2f MB/s\n", float64(stats.BytesProcessed)/(1024*1024)/elapsed.Seconds())

	if verbose {
		fmt.Printf("\nOutput files created in: %s\n", outputDir)
	}

	return nil
}
