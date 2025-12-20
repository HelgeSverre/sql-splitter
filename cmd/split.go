package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/helgesverre/sql-splitter/internal/splitter"
	"github.com/spf13/cobra"
)

var (
	outputDir       string
	verbose         bool
	dryRun          bool
	splitProgress   bool
	tableFilter     string
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
  sql-splitter split database.sql -o output -v
  sql-splitter split database.sql --tables=users,posts
  sql-splitter split database.sql --dry-run`,
	Args: cobra.ExactArgs(1),
	RunE: runSplit,
}

func init() {
	rootCmd.AddCommand(splitCmd)

	splitCmd.Flags().StringVarP(&outputDir, "output", "o", "output", "Output directory for split files")
	splitCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output")
	splitCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Preview what would be split without writing files")
	splitCmd.Flags().BoolVarP(&splitProgress, "progress", "p", false, "Show progress during processing")
	splitCmd.Flags().StringVarP(&tableFilter, "tables", "t", "", "Only split specific tables (comma-separated)")
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

	if dryRun {
		fmt.Printf("Dry run: analyzing SQL file: %s (%.2f MB)\n", inputFile, float64(fileInfo.Size())/(1024*1024))
	} else {
		fmt.Printf("Splitting SQL file: %s (%.2f MB)\n", inputFile, float64(fileInfo.Size())/(1024*1024))
		fmt.Printf("Output directory: %s\n", outputDir)
	}
	fmt.Println()

	// Build options
	var opts []splitter.Option

	// Add table filter if specified
	if tableFilter != "" {
		tables := strings.Split(tableFilter, ",")
		for i := range tables {
			tables[i] = strings.TrimSpace(tables[i])
		}
		opts = append(opts, splitter.WithTableFilter(tables))
		fmt.Printf("Filtering to tables: %s\n\n", tableFilter)
	}

	// Add dry-run option
	if dryRun {
		opts = append(opts, splitter.WithDryRun(true))
	}

	// Add progress callback if requested
	if splitProgress {
		var lastProgress int
		opts = append(opts, splitter.WithProgress(func(bytesRead int64) {
			progress := int(float64(bytesRead) / float64(fileInfo.Size()) * 100)
			if progress > lastProgress && progress%5 == 0 {
				fmt.Printf("\rProgress: %d%%", progress)
				lastProgress = progress
			}
		}))
	}

	// Create splitter with options
	s := splitter.NewSplitter(inputFile, outputDir, opts...)

	// Start timing
	startTime := time.Now()

	// Perform split
	if err := s.Split(); err != nil {
		return fmt.Errorf("split failed: %w", err)
	}

	if splitProgress {
		fmt.Println() // New line after progress
	}

	// Get statistics
	stats := s.GetStats()
	elapsed := time.Since(startTime)

	// Display results
	if dryRun {
		fmt.Println("✓ Dry run completed!")
		fmt.Printf("\nWould create %d table files:\n", stats.TablesFound)
		for _, name := range stats.TableNames {
			fmt.Printf("  - %s.sql\n", name)
		}
	} else {
		fmt.Println("✓ Split completed successfully!")
	}

	fmt.Printf("\nStatistics:\n")
	fmt.Printf("  Tables found: %d\n", stats.TablesFound)
	fmt.Printf("  Statements processed: %d\n", stats.StatementsProcessed)
	fmt.Printf("  Bytes processed: %.2f MB\n", float64(stats.BytesProcessed)/(1024*1024))
	fmt.Printf("  Elapsed time: %s\n", elapsed.Round(time.Millisecond))
	if elapsed.Seconds() > 0 {
		fmt.Printf("  Throughput: %.2f MB/s\n", float64(stats.BytesProcessed)/(1024*1024)/elapsed.Seconds())
	}

	if verbose && !dryRun {
		fmt.Printf("\nOutput files created in: %s\n", outputDir)
	}

	return nil
}
