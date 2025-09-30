package analyzer

import (
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/helgesverre/sql-splitter/internal/parser"
)

// TableStats holds statistics for a single table
type TableStats struct {
	TableName      string
	InsertCount    int64
	CreateCount    int64
	TotalBytes     int64
	StatementCount int64
}

// Analyzer analyzes SQL files to gather statistics
type Analyzer struct {
	inputFile string
	stats     map[string]*TableStats
	mu        sync.RWMutex
}

// NewAnalyzer creates a new SQL file analyzer
func NewAnalyzer(inputFile string) *Analyzer {
	return &Analyzer{
		inputFile: inputFile,
		stats:     make(map[string]*TableStats),
	}
}

// Analyze performs the analysis and returns statistics sorted by insert count
func (a *Analyzer) Analyze() ([]*TableStats, error) {
	// Open input file
	file, err := os.Open(a.inputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	// Get file size for buffer optimization
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat input file: %w", err)
	}

	// Create parser with optimal buffer size
	bufferSize := parser.DetermineBufferSize(fileInfo.Size())
	p := parser.NewParser(file, bufferSize)

	// Process statements
	for {
		stmt, err := p.ReadStatement()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error reading statement: %w", err)
		}

		// Parse statement type and table name
		stmtType, tableName := p.ParseStatement(stmt)

		// Skip unknown statements or those without table names
		if stmtType == parser.Unknown || tableName == "" {
			continue
		}

		// Update statistics
		a.updateStats(tableName, stmtType, int64(len(stmt)))
	}

	// Convert map to sorted slice
	return a.getSortedStats(), nil
}

// updateStats updates statistics for a table
func (a *Analyzer) updateStats(tableName string, stmtType parser.StatementType, bytes int64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	stats, exists := a.stats[tableName]
	if !exists {
		stats = &TableStats{
			TableName: tableName,
		}
		a.stats[tableName] = stats
	}

	stats.StatementCount++
	stats.TotalBytes += bytes

	switch stmtType {
	case parser.CreateTable:
		stats.CreateCount++
	case parser.Insert:
		stats.InsertCount++
	}
}

// getSortedStats returns statistics sorted by insert count (descending)
func (a *Analyzer) getSortedStats() []*TableStats {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Convert map to slice
	result := make([]*TableStats, 0, len(a.stats))
	for _, stats := range a.stats {
		result = append(result, stats)
	}

	// Sort by insert count (descending)
	sort.Slice(result, func(i, j int) bool {
		return result[i].InsertCount > result[j].InsertCount
	})

	return result
}

// AnalyzeWithProgress performs analysis with progress callback
func (a *Analyzer) AnalyzeWithProgress(progressFn func(bytesRead int64)) ([]*TableStats, error) {
	// Open input file
	file, err := os.Open(a.inputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	// Get file size for buffer optimization
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat input file: %w", err)
	}

	// Wrap file with progress reader
	progressReader := &progressReader{
		reader:     file,
		callback:   progressFn,
		totalBytes: fileInfo.Size(),
	}

	// Create parser with optimal buffer size
	bufferSize := parser.DetermineBufferSize(fileInfo.Size())
	p := parser.NewParser(progressReader, bufferSize)

	// Process statements
	for {
		stmt, err := p.ReadStatement()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error reading statement: %w", err)
		}

		// Parse statement type and table name
		stmtType, tableName := p.ParseStatement(stmt)

		// Skip unknown statements or those without table names
		if stmtType == parser.Unknown || tableName == "" {
			continue
		}

		// Update statistics
		a.updateStats(tableName, stmtType, int64(len(stmt)))
	}

	// Convert map to sorted slice
	return a.getSortedStats(), nil
}

// progressReader wraps an io.Reader to track progress
type progressReader struct {
	reader     io.Reader
	callback   func(int64)
	totalBytes int64
	readBytes  int64
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.reader.Read(p)
	pr.readBytes += int64(n)
	if pr.callback != nil {
		pr.callback(pr.readBytes)
	}
	return n, err
}
