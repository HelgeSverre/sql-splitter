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
	inputFile  string
	outputDir  string
	writerPool *writer.WriterPool
	stats      *Stats
	mu         sync.Mutex
}

// Stats tracks processing statistics
type Stats struct {
	StatementsProcessed int64
	TablesFound         int
	BytesProcessed      int64
}

// NewSplitter creates a new SQL file splitter
func NewSplitter(inputFile, outputDir string) *Splitter {
	return &Splitter{
		inputFile:  inputFile,
		outputDir:  outputDir,
		writerPool: writer.NewWriterPool(),
		stats:      &Stats{},
	}
}

// Split performs the splitting operation
func (s *Splitter) Split() error {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(s.outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
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

	// Create parser with optimal buffer size
	bufferSize := parser.DetermineBufferSize(fileInfo.Size())
	p := parser.NewParser(file, bufferSize)

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

		// Update stats
		s.mu.Lock()
		s.stats.StatementsProcessed++
		s.stats.BytesProcessed += int64(len(stmt))
		s.mu.Unlock()
	}

	// Close all writers
	if err := s.writerPool.CloseAll(); err != nil {
		return fmt.Errorf("error closing writers: %w", err)
	}

	return nil
}

// GetStats returns a copy of the current statistics
func (s *Splitter) GetStats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.stats
}
