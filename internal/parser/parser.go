package parser

import (
	"bufio"
	"bytes"
	"io"
	"regexp"
	"sync"
)

const (
	// Buffer sizes for different file sizes
	SmallBufferSize  = 64 * 1024       // 64KB
	MediumBufferSize = 256 * 1024      // 256KB
	LargeBufferSize  = 1 * 1024 * 1024 // 1MB
	HugeBufferSize   = 4 * 1024 * 1024 // 4MB
)

// StatementType represents the type of SQL statement
type StatementType int

const (
	Unknown StatementType = iota
	CreateTable
	Insert
	CreateIndex
	AlterTable
	DropTable
)

// Statement represents a parsed SQL statement
type Statement struct {
	Type      StatementType
	TableName string
	Data      []byte
}

// Parser handles efficient SQL file parsing
type Parser struct {
	reader        *bufio.Reader
	buffer        []byte
	bufferPool    *sync.Pool
	createTableRe *regexp.Regexp
	insertIntoRe  *regexp.Regexp
}

// bufferPool for reusing statement buffers
var stmtBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 8192) // 8KB initial capacity
		return &buf
	},
}

// NewParser creates a new SQL parser with the given reader and buffer size
func NewParser(reader io.Reader, bufferSize int) *Parser {
	return &Parser{
		reader:        bufio.NewReaderSize(reader, bufferSize),
		buffer:        make([]byte, 0, bufferSize),
		bufferPool:    &stmtBufPool,
		createTableRe: regexp.MustCompile(`(?i)^\s*CREATE\s+TABLE\s+` + "`?" + `([^\s` + "`" + `(]+)` + "`?"),
		insertIntoRe:  regexp.MustCompile(`(?i)^\s*INSERT\s+INTO\s+` + "`?" + `([^\s` + "`" + `(]+)` + "`?"),
	}
}

// ReadStatement reads a complete SQL statement (until semicolon outside strings)
// Returns the statement bytes and any error encountered
func (p *Parser) ReadStatement() ([]byte, error) {
	// Get buffer from pool
	bufPtr := p.bufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0] // Reset length, keep capacity

	insideString := false
	insideSingleQuote := false
	insideDoubleQuote := false
	escaped := false

	for {
		b, err := p.reader.ReadByte()
		if err != nil {
			if err == io.EOF && len(buf) > 0 {
				// Return buffer to pool after copying
				result := make([]byte, len(buf))
				copy(result, buf)
				p.bufferPool.Put(bufPtr)
				return result, nil
			}
			p.bufferPool.Put(bufPtr)
			return nil, err
		}

		buf = append(buf, b)

		// Track string boundaries - handle both single and double quotes
		if !escaped {
			if b == '\'' && !insideDoubleQuote {
				insideSingleQuote = !insideSingleQuote
				insideString = insideSingleQuote || insideDoubleQuote
			} else if b == '"' && !insideSingleQuote {
				insideDoubleQuote = !insideDoubleQuote
				insideString = insideSingleQuote || insideDoubleQuote
			}
		}

		// Track escaping with backslash
		if b == '\\' && !escaped {
			escaped = true
		} else {
			escaped = false
		}

		// Statement terminator outside strings
		if b == ';' && !insideString {
			// Copy buffer before returning to pool
			result := make([]byte, len(buf))
			copy(result, buf)
			p.bufferPool.Put(bufPtr)
			return result, nil
		}
	}
}

// ParseStatement determines the statement type and extracts table name
func (p *Parser) ParseStatement(stmt []byte) (StatementType, string) {
	// Trim whitespace for faster matching
	stmt = bytes.TrimSpace(stmt)

	// Fast path for common cases using byte prefix checking
	if len(stmt) < 6 {
		return Unknown, ""
	}

	// Convert first word to uppercase for comparison
	upperPrefix := bytes.ToUpper(stmt[:min(20, len(stmt))])

	// Fast check for CREATE TABLE
	if bytes.HasPrefix(upperPrefix, []byte("CREATE TABLE")) {
		if matches := p.createTableRe.FindSubmatch(stmt); matches != nil && len(matches) > 1 {
			return CreateTable, string(matches[1])
		}
	}

	// Fast check for INSERT INTO
	if bytes.HasPrefix(upperPrefix, []byte("INSERT INTO")) {
		if matches := p.insertIntoRe.FindSubmatch(stmt); matches != nil && len(matches) > 1 {
			return Insert, string(matches[1])
		}
	}

	// Check for other statement types
	if bytes.HasPrefix(upperPrefix, []byte("CREATE INDEX")) {
		if matches := regexp.MustCompile(`(?i)ON\s+` + "`?" + `([^\s` + "`" + `(;]+)` + "`?").FindSubmatch(stmt); matches != nil && len(matches) > 1 {
			return CreateIndex, string(matches[1])
		}
	}

	if bytes.HasPrefix(upperPrefix, []byte("ALTER TABLE")) {
		if matches := regexp.MustCompile(`(?i)ALTER\s+TABLE\s+` + "`?" + `([^\s` + "`" + `;]+)` + "`?").FindSubmatch(stmt); matches != nil && len(matches) > 1 {
			return AlterTable, string(matches[1])
		}
	}

	if bytes.HasPrefix(upperPrefix, []byte("DROP TABLE")) {
		if matches := regexp.MustCompile(`(?i)DROP\s+TABLE\s+` + "`?" + `([^\s` + "`" + `;]+)` + "`?").FindSubmatch(stmt); matches != nil && len(matches) > 1 {
			return DropTable, string(matches[1])
		}
	}

	return Unknown, ""
}

// DetermineBufferSize returns optimal buffer size based on file size
func DetermineBufferSize(fileSize int64) int {
	switch {
	case fileSize > 1*1024*1024*1024: // > 1GB
		return HugeBufferSize
	case fileSize > 100*1024*1024: // > 100MB
		return LargeBufferSize
	case fileSize > 10*1024*1024: // > 10MB
		return MediumBufferSize
	default:
		return SmallBufferSize
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
