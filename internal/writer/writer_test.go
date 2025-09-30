package writer

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTableWriter_WriteStatement(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test.sql")

	w, err := NewTableWriter(filename)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Write some statements
	stmts := [][]byte{
		[]byte("CREATE TABLE users (id INT);"),
		[]byte("INSERT INTO users VALUES (1);"),
		[]byte("INSERT INTO users VALUES (2);"),
	}

	for _, stmt := range stmts {
		if err := w.WriteStatement(stmt); err != nil {
			t.Fatalf("failed to write statement: %v", err)
		}
	}

	// Close to flush
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	// Read file and verify
	content, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	expected := "CREATE TABLE users (id INT);\nINSERT INTO users VALUES (1);\nINSERT INTO users VALUES (2);\n"
	if string(content) != expected {
		t.Errorf("content mismatch:\nwant: %q\ngot:  %q", expected, string(content))
	}
}

func TestWriterPool(t *testing.T) {
	tmpDir := t.TempDir()
	pool := NewWriterPool()

	// Get writer for table1
	w1, err := pool.GetWriter("table1", filepath.Join(tmpDir, "table1.sql"))
	if err != nil {
		t.Fatalf("failed to get writer: %v", err)
	}

	// Get same writer again (should return existing)
	w2, err := pool.GetWriter("table1", filepath.Join(tmpDir, "table1.sql"))
	if err != nil {
		t.Fatalf("failed to get writer: %v", err)
	}

	if w1 != w2 {
		t.Error("expected same writer instance")
	}

	// Get writer for different table
	w3, err := pool.GetWriter("table2", filepath.Join(tmpDir, "table2.sql"))
	if err != nil {
		t.Fatalf("failed to get writer: %v", err)
	}

	if w1 == w3 {
		t.Error("expected different writer instance")
	}

	// Close all
	if err := pool.CloseAll(); err != nil {
		t.Fatalf("failed to close all: %v", err)
	}
}

func BenchmarkTableWriter_WriteStatement(b *testing.B) {
	tmpDir := b.TempDir()
	filename := filepath.Join(tmpDir, "bench.sql")

	w, err := NewTableWriter(filename)
	if err != nil {
		b.Fatalf("failed to create writer: %v", err)
	}
	defer w.Close()

	stmt := []byte("INSERT INTO users VALUES (1, 'test data');")

	b.ResetTimer()
	b.SetBytes(int64(len(stmt)))

	for i := 0; i < b.N; i++ {
		if err := w.WriteStatement(stmt); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTableWriter_Buffering(b *testing.B) {
	stmt := []byte("INSERT INTO users VALUES (1, 'test');")

	bufferCounts := []int{10, 50, 100, 500, 1000}

	for _, count := range bufferCounts {
		b.Run(string(rune(count)), func(b *testing.B) {
			tmpDir := b.TempDir()
			filename := filepath.Join(tmpDir, "bench.sql")

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				w, err := NewTableWriter(filename)
				if err != nil {
					b.Fatal(err)
				}

				w.maxStmtBuffer = count

				for j := 0; j < 1000; j++ {
					if err := w.WriteStatement(stmt); err != nil {
						b.Fatal(err)
					}
				}

				if err := w.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
