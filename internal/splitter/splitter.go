package splitter

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/helgesverre/sql-splitter/internal/parser"
	"github.com/helgesverre/sql-splitter/internal/writer"
)

// Splitter handles the SQL file splitting with concurrent processing
type Splitter struct {
	inputFile    string
	outputDir    string
	writerPool   *writer.WriterPool
	stats        *Stats
	mu           sync.Mutex
	tableFilter  map[string]bool // If set, only split these tables
	dryRun       bool            // If true, don't write files
	progressFn   func(bytesRead int64)
	tablesSeen   map[string]bool // Track unique tables for dry-run
}

// Stats tracks processing statistics
type Stats struct {
	StatementsProcessed int64
	TablesFound         int
	BytesProcessed      int64
	TableNames          []string // For dry-run output
}

// Option is a functional option for configuring a Splitter
type Option func(*Splitter)

// WithTableFilter sets a filter to only split specific tables
func WithTableFilter(tables []string) Option {
	return func(s *Splitter) {
		if len(tables) > 0 {
			s.tableFilter = make(map[string]bool)
			for _, t := range tables {
				s.tableFilter[t] = true
			}
		}
	}
}

// WithDryRun enables dry-run mode (analyze without writing)
func WithDryRun(dryRun bool) Option {
	return func(s *Splitter) {
		s.dryRun = dryRun
	}
}

// WithProgress sets a progress callback
func WithProgress(fn func(bytesRead int64)) Option {
	return func(s *Splitter) {
		s.progressFn = fn
	}
}

// NewSplitter creates a new SQL file splitter
func NewSplitter(inputFile, outputDir string, opts ...Option) *Splitter {
	s := &Splitter{
		inputFile:  inputFile,
		outputDir:  outputDir,
		writerPool: writer.NewWriterPool(),
		stats:      &Stats{},
		tablesSeen: make(map[string]bool),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Split performs the splitting operation
func (s *Splitter) Split() error {
	// Create output directory if it doesn't exist (skip for dry-run)
	if !s.dryRun {
		if err := os.MkdirAll(s.outputDir, 0755); err != nil {
			return fmt.Errorf("failed to create output directory: %w", err)
		}
	}

	// Open input file
	file, err := os.Open(s.inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	// Get file size for buffer optimization
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat input file: %w", err)
	}

	// Wrap file with progress reader if callback is set
	var reader io.Reader = file
	if s.progressFn != nil {
		reader = &progressReader{
			reader:   file,
			callback: s.progressFn,
		}
	}

	// Create parser with optimal buffer size
	bufferSize := parser.DetermineBufferSize(fileInfo.Size())
	p := parser.NewParser(reader, bufferSize)

	// Process statements sequentially (writes are concurrent via writer pool)
	for {
		stmt, err := p.ReadStatement()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading statement: %w", err)
		}

		// Parse statement type and table name
		stmtType, tableName := p.ParseStatement(stmt)

		// Skip unknown statements or those without table names
		if stmtType == parser.Unknown || tableName == "" {
			continue
		}

		// Apply table filter if set
		if s.tableFilter != nil && !s.tableFilter[tableName] {
			continue
		}

		// Track unique tables
		if !s.tablesSeen[tableName] {
			s.tablesSeen[tableName] = true
			s.mu.Lock()
			s.stats.TablesFound++
			s.stats.TableNames = append(s.stats.TableNames, tableName)
			s.mu.Unlock()
		}

		// In dry-run mode, just count without writing
		if !s.dryRun {
			// Get or create writer for this table
			filename := filepath.Join(s.outputDir, fmt.Sprintf("%s.sql", tableName))
			w, err := s.writerPool.GetWriter(tableName, filename)
			if err != nil {
				return fmt.Errorf("failed to get writer for table %s: %w", tableName, err)
			}

			// Write statement (writer handles buffering)
			if err := w.WriteStatement(stmt); err != nil {
				return fmt.Errorf("failed to write statement for table %s: %w", tableName, err)
			}
		}

		// Update stats
		s.mu.Lock()
		s.stats.StatementsProcessed++
		s.stats.BytesProcessed += int64(len(stmt))
		s.mu.Unlock()
	}

	// Close all writers (skip for dry-run)
	if !s.dryRun {
		if err := s.writerPool.CloseAll(); err != nil {
			return fmt.Errorf("error closing writers: %w", err)
		}
	}

	return nil
}

// progressReader wraps an io.Reader to track progress
type progressReader struct {
	reader   io.Reader
	callback func(int64)
	read     int64
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.reader.Read(p)
	pr.read += int64(n)
	if pr.callback != nil {
		pr.callback(pr.read)
	}
	return n, err
}

// GetStats returns a copy of the current statistics
func (s *Splitter) GetStats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.stats
}
