package writer

import (
	"bufio"
	"os"
	"sync"
)

const (
	// WriterBufferSize is the default buffer size for file writers
	WriterBufferSize = 256 * 1024 // 256KB

	// StmtBufferCount is the number of statements to buffer before flushing
	StmtBufferCount = 100
)

// TableWriter handles buffered writing to table-specific output files
type TableWriter struct {
	file          *os.File
	writer        *bufio.Writer
	stmtBuffer    [][]byte
	maxStmtBuffer int
	mu            sync.Mutex
}

// NewTableWriter creates a new table writer for the given filename
func NewTableWriter(filename string) (*TableWriter, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	writer := bufio.NewWriterSize(file, WriterBufferSize)

	return &TableWriter{
		file:          file,
		writer:        writer,
		stmtBuffer:    make([][]byte, 0, StmtBufferCount),
		maxStmtBuffer: StmtBufferCount,
	}, nil
}

// WriteStatement buffers a statement and flushes when buffer is full
func (w *TableWriter) WriteStatement(stmt []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Copy statement to avoid data races
	stmtCopy := make([]byte, len(stmt))
	copy(stmtCopy, stmt)

	w.stmtBuffer = append(w.stmtBuffer, stmtCopy)

	// Flush when buffer is full
	if len(w.stmtBuffer) >= w.maxStmtBuffer {
		return w.flushUnlocked()
	}

	return nil
}

// Flush writes all buffered statements to disk
func (w *TableWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushUnlocked()
}

// flushUnlocked performs the actual flush (caller must hold lock)
func (w *TableWriter) flushUnlocked() error {
	for _, stmt := range w.stmtBuffer {
		if _, err := w.writer.Write(stmt); err != nil {
			return err
		}
		if _, err := w.writer.WriteString("\n"); err != nil {
			return err
		}
	}

	// Clear buffer (reuse capacity)
	w.stmtBuffer = w.stmtBuffer[:0]

	// Flush bufio.Writer
	return w.writer.Flush()
}

// Close flushes remaining data and closes the file
func (w *TableWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush any remaining statements
	if err := w.flushUnlocked(); err != nil {
		return err
	}

	// Flush bufio.Writer
	if err := w.writer.Flush(); err != nil {
		return err
	}

	// Close file
	return w.file.Close()
}

// WriterPool manages a pool of table writers for concurrent access
type WriterPool struct {
	writers map[string]*TableWriter
	mu      sync.RWMutex
}

// NewWriterPool creates a new writer pool
func NewWriterPool() *WriterPool {
	return &WriterPool{
		writers: make(map[string]*TableWriter),
	}
}

// GetWriter returns a writer for the given table, creating it if necessary
func (p *WriterPool) GetWriter(tableName, filename string) (*TableWriter, error) {
	// Try read lock first (fast path)
	p.mu.RLock()
	writer, exists := p.writers[tableName]
	p.mu.RUnlock()

	if exists {
		return writer, nil
	}

	// Need to create writer (write lock)
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if writer, exists := p.writers[tableName]; exists {
		return writer, nil
	}

	// Create new writer
	writer, err := NewTableWriter(filename)
	if err != nil {
		return nil, err
	}

	p.writers[tableName] = writer
	return writer, nil
}

// CloseAll closes all writers in the pool
func (p *WriterPool) CloseAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for _, writer := range p.writers {
		if err := writer.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}
