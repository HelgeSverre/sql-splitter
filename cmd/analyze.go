package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/helgesverre/sql-splitter/internal/analyzer"
	"github.com/spf13/cobra"
)

var (
	showProgress bool
)

var analyzeCmd = &cobra.Command{
	Use:   "analyze [file]",
	Short: "Analyze a SQL file and display statistics",
	Long: `Analyze a SQL dump file to gather statistics about tables and statements.

The analyze command scans the SQL file and reports on:
  - Number of tables found
  - INSERT statement counts per table
  - CREATE TABLE statement counts
  - Total bytes per table

Results are sorted by INSERT count in descending order.

Example:
  sql-splitter analyze large-dump.sql
  sql-splitter analyze database.sql --progress`,
	Args: cobra.ExactArgs(1),
	RunE: runAnalyze,
}

func init() {
	rootCmd.AddCommand(analyzeCmd)

	analyzeCmd.Flags().BoolVarP(&showProgress, "progress", "p", false, "Show progress bar during analysis")
}

func runAnalyze(cmd *cobra.Command, args []string) error {
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

	fmt.Printf("Analyzing SQL file: %s (%.2f MB)\n", inputFile, float64(fileInfo.Size())/(1024*1024))
	fmt.Println()

	// Create analyzer
	a := analyzer.NewAnalyzer(inputFile)

	// Start timing
	startTime := time.Now()

	var stats []*analyzer.TableStats

	// Perform analysis with optional progress
	if showProgress {
		var lastProgress int
		stats, err = a.AnalyzeWithProgress(func(bytesRead int64) {
			progress := int(float64(bytesRead) / float64(fileInfo.Size()) * 100)
			if progress > lastProgress && progress%5 == 0 {
				fmt.Printf("\rProgress: %d%%", progress)
				lastProgress = progress
			}
		})
		fmt.Println() // New line after progress
	} else {
		stats, err = a.Analyze()
	}

	if err != nil {
		return fmt.Errorf("analysis failed: %w", err)
	}

	elapsed := time.Since(startTime)

	// Display results
	fmt.Printf("✓ Analysis completed in %s\n\n", elapsed.Round(time.Millisecond))

	if len(stats) == 0 {
		fmt.Println("No tables found in SQL file.")
		return nil
	}

	// Display table statistics
	fmt.Printf("Found %d tables:\n\n", len(stats))
	fmt.Printf("%-40s %12s %12s %12s\n", "Table Name", "INSERTs", "Total Stmts", "Size (MB)")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────")

	var totalInserts int64
	var totalBytes int64

	for _, stat := range stats {
		fmt.Printf("%-40s %12d %12d %12.2f\n",
			truncateString(stat.TableName, 40),
			stat.InsertCount,
			stat.StatementCount,
			float64(stat.TotalBytes)/(1024*1024))

		totalInserts += stat.InsertCount
		totalBytes += stat.TotalBytes
	}

	fmt.Println("─────────────────────────────────────────────────────────────────────────────────")
	fmt.Printf("%-40s %12d %12s %12.2f\n", "TOTAL", totalInserts, "-", float64(totalBytes)/(1024*1024))

	return nil
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
