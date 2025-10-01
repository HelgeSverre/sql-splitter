package parser

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestParser_ReadStatement(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "simple create table",
			input:    "CREATE TABLE users (id INT);",
			expected: []string{"CREATE TABLE users (id INT);"},
		},
		{
			name:     "multiple statements",
			input:    "CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);",
			expected: []string{"CREATE TABLE users (id INT);", "\nINSERT INTO users VALUES (1);"},
		},
		{
			name:     "statement with string containing semicolon",
			input:    "INSERT INTO users VALUES (1, 'hello;world');",
			expected: []string{"INSERT INTO users VALUES (1, 'hello;world');"},
		},
		{
			name:     "statement with escaped quote",
			input:    "INSERT INTO users VALUES (1, 'it\\'s working');",
			expected: []string{"INSERT INTO users VALUES (1, 'it\\'s working');"},
		},
		{
			name: "multiline statement",
			input: `CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);`,
			expected: []string{`CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			p := NewParser(reader, SmallBufferSize)

			for i, expected := range tt.expected {
				stmt, err := p.ReadStatement()
				if err != nil {
					t.Fatalf("statement %d: unexpected error: %v", i, err)
				}
				if string(stmt) != expected {
					t.Errorf("statement %d:\nwant: %q\ngot:  %q", i, expected, string(stmt))
				}
			}

			// Should return EOF after all statements
			_, err := p.ReadStatement()
			if err != io.EOF {
				t.Errorf("expected EOF, got: %v", err)
			}
		})
	}
}

func TestParser_ParseStatement(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedType  StatementType
		expectedTable string
	}{
		{
			name:          "create table",
			input:         "CREATE TABLE users (id INT);",
			expectedType:  CreateTable,
			expectedTable: "users",
		},
		{
			name:          "create table with backticks",
			input:         "CREATE TABLE `users` (id INT);",
			expectedType:  CreateTable,
			expectedTable: "users",
		},
		{
			name:          "insert into",
			input:         "INSERT INTO users VALUES (1, 'test');",
			expectedType:  Insert,
			expectedTable: "users",
		},
		{
			name:          "insert into with backticks",
			input:         "INSERT INTO `users` VALUES (1, 'test');",
			expectedType:  Insert,
			expectedTable: "users",
		},
		{
			name:          "create index",
			input:         "CREATE INDEX idx_name ON users (name);",
			expectedType:  CreateIndex,
			expectedTable: "users",
		},
		{
			name:          "alter table",
			input:         "ALTER TABLE users ADD COLUMN email VARCHAR(255);",
			expectedType:  AlterTable,
			expectedTable: "users",
		},
		{
			name:          "drop table",
			input:         "DROP TABLE users;",
			expectedType:  DropTable,
			expectedTable: "users",
		},
		{
			name:          "unknown statement",
			input:         "SELECT * FROM users;",
			expectedType:  Unknown,
			expectedTable: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(strings.NewReader(""), SmallBufferSize)
			stmtType, tableName := p.ParseStatement([]byte(tt.input))

			if stmtType != tt.expectedType {
				t.Errorf("type: want %v, got %v", tt.expectedType, stmtType)
			}
			if tableName != tt.expectedTable {
				t.Errorf("table: want %q, got %q", tt.expectedTable, tableName)
			}
		})
	}
}

func TestDetermineBufferSize(t *testing.T) {
	tests := []struct {
		fileSize int64
		expected int
		name     string
	}{
		{1 * 1024 * 1024, SmallBufferSize, "1MB file"},
		{50 * 1024 * 1024, SmallBufferSize, "50MB file"},       // 64KB (optimized)
		{500 * 1024 * 1024, SmallBufferSize, "500MB file"},     // 64KB (optimized)
		{2 * 1024 * 1024 * 1024, MediumBufferSize, "2GB file"}, // 256KB for very large files
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DetermineBufferSize(tt.fileSize)
			if result != tt.expected {
				t.Errorf("want %d, got %d", tt.expected, result)
			}
		})
	}
}

// Benchmarks

func BenchmarkParser_ReadStatement(b *testing.B) {
	stmt := "INSERT INTO users VALUES (1, 'test data with some content');\n"
	data := strings.Repeat(stmt, 1000)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	for i := 0; i < b.N; i++ {
		reader := strings.NewReader(data)
		p := NewParser(reader, MediumBufferSize)

		for {
			_, err := p.ReadStatement()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkParser_ParseStatement(b *testing.B) {
	stmt := []byte("INSERT INTO users VALUES (1, 'test');")
	p := NewParser(strings.NewReader(""), SmallBufferSize)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p.ParseStatement(stmt)
	}
}

func BenchmarkParser_BufferSizes(b *testing.B) {
	// Generate test data
	stmt := "INSERT INTO users VALUES (1, 'test data');\n"
	data := strings.Repeat(stmt, 10000) // ~400KB

	sizes := []struct {
		name string
		size int
	}{
		{"4KB", 4 * 1024},
		{"64KB", 64 * 1024},
		{"256KB", 256 * 1024},
		{"1MB", 1024 * 1024},
	}

	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))

			for i := 0; i < b.N; i++ {
				reader := strings.NewReader(data)
				p := NewParser(reader, s.size)

				count := 0
				for {
					_, err := p.ReadStatement()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
					count++
				}
			}
		})
	}
}

func BenchmarkParser_StringVsBytes(b *testing.B) {
	data := []byte("CREATE TABLE users (id INT PRIMARY KEY);")

	b.Run("BytesContains", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = bytes.Contains(data, []byte("TABLE"))
		}
	})

	b.Run("StringContains", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s := string(data)
			_ = strings.Contains(s, "TABLE")
		}
	})

	b.Run("BytesHasPrefix", func(b *testing.B) {
		upper := bytes.ToUpper(data[:20])
		for i := 0; i < b.N; i++ {
			_ = bytes.HasPrefix(upper, []byte("CREATE TABLE"))
		}
	})
}
