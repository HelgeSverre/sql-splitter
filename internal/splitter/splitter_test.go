package splitter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSplitter_Split(t *testing.T) {
	// Create temp directories
	tmpDir := t.TempDir()
	inputFile := filepath.Join(tmpDir, "input.sql")
	outputDir := filepath.Join(tmpDir, "output")

	// Create input file with multiple tables
	content := `CREATE TABLE users (id INT, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
CREATE TABLE posts (id INT, user_id INT, title VARCHAR(255));
INSERT INTO posts VALUES (1, 1, 'Hello World');
INSERT INTO posts VALUES (2, 2, 'Goodbye World');
`

	if err := os.WriteFile(inputFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write input file: %v", err)
	}

	// Create splitter and run
	s := NewSplitter(inputFile, outputDir)
	if err := s.Split(); err != nil {
		t.Fatalf("split failed: %v", err)
	}

	// Verify output directory was created
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		t.Error("output directory was not created")
	}

	// Verify users.sql was created with correct content
	usersContent, err := os.ReadFile(filepath.Join(outputDir, "users.sql"))
	if err != nil {
		t.Fatalf("failed to read users.sql: %v", err)
	}

	// Check that all expected statements are present
	usersStr := string(usersContent)
	if !strings.Contains(usersStr, "CREATE TABLE users (id INT, name VARCHAR(255));") {
		t.Error("users.sql should contain CREATE TABLE")
	}
	if !strings.Contains(usersStr, "INSERT INTO users VALUES (1, 'Alice');") {
		t.Error("users.sql should contain first INSERT")
	}
	if !strings.Contains(usersStr, "INSERT INTO users VALUES (2, 'Bob');") {
		t.Error("users.sql should contain second INSERT")
	}

	// Verify posts.sql was created with correct content
	postsContent, err := os.ReadFile(filepath.Join(outputDir, "posts.sql"))
	if err != nil {
		t.Fatalf("failed to read posts.sql: %v", err)
	}

	postsStr := string(postsContent)
	if !strings.Contains(postsStr, "CREATE TABLE posts (id INT, user_id INT, title VARCHAR(255));") {
		t.Error("posts.sql should contain CREATE TABLE")
	}
	if !strings.Contains(postsStr, "INSERT INTO posts VALUES (1, 1, 'Hello World');") {
		t.Error("posts.sql should contain first INSERT")
	}
	if !strings.Contains(postsStr, "INSERT INTO posts VALUES (2, 2, 'Goodbye World');") {
		t.Error("posts.sql should contain second INSERT")
	}

	// Verify stats
	stats := s.GetStats()
	if stats.StatementsProcessed != 6 {
		t.Errorf("expected 6 statements processed, got %d", stats.StatementsProcessed)
	}
}

func TestSplitter_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	inputFile := filepath.Join(tmpDir, "empty.sql")
	outputDir := filepath.Join(tmpDir, "output")

	if err := os.WriteFile(inputFile, []byte(""), 0644); err != nil {
		t.Fatalf("failed to write input file: %v", err)
	}

	s := NewSplitter(inputFile, outputDir)
	if err := s.Split(); err != nil {
		t.Fatalf("split failed: %v", err)
	}

	stats := s.GetStats()
	if stats.StatementsProcessed != 0 {
		t.Errorf("expected 0 statements, got %d", stats.StatementsProcessed)
	}
}

func TestSplitter_FileNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	outputDir := filepath.Join(tmpDir, "output")

	s := NewSplitter("/nonexistent/file.sql", outputDir)
	err := s.Split()
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestSplitter_UnknownStatements(t *testing.T) {
	tmpDir := t.TempDir()
	inputFile := filepath.Join(tmpDir, "input.sql")
	outputDir := filepath.Join(tmpDir, "output")

	// File with unknown statements that should be skipped
	content := `SELECT * FROM users;
CREATE TABLE users (id INT);
UPDATE users SET name = 'test';
INSERT INTO users VALUES (1);
DELETE FROM users WHERE id = 1;
`

	if err := os.WriteFile(inputFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write input file: %v", err)
	}

	s := NewSplitter(inputFile, outputDir)
	if err := s.Split(); err != nil {
		t.Fatalf("split failed: %v", err)
	}

	// Only CREATE TABLE and INSERT should be processed
	stats := s.GetStats()
	if stats.StatementsProcessed != 2 {
		t.Errorf("expected 2 statements processed, got %d", stats.StatementsProcessed)
	}
}

func TestSplitter_BacktickedTableNames(t *testing.T) {
	tmpDir := t.TempDir()
	inputFile := filepath.Join(tmpDir, "input.sql")
	outputDir := filepath.Join(tmpDir, "output")

	content := "CREATE TABLE `my_table` (id INT);\nINSERT INTO `my_table` VALUES (1);\n"

	if err := os.WriteFile(inputFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write input file: %v", err)
	}

	s := NewSplitter(inputFile, outputDir)
	if err := s.Split(); err != nil {
		t.Fatalf("split failed: %v", err)
	}

	// Verify my_table.sql was created
	if _, err := os.Stat(filepath.Join(outputDir, "my_table.sql")); os.IsNotExist(err) {
		t.Error("my_table.sql was not created")
	}
}

func TestSplitter_MultilineStatements(t *testing.T) {
	tmpDir := t.TempDir()
	inputFile := filepath.Join(tmpDir, "input.sql")
	outputDir := filepath.Join(tmpDir, "output")

	content := `CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255)
);
INSERT INTO users VALUES (
    1,
    'Alice',
    'alice@example.com'
);
`

	if err := os.WriteFile(inputFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write input file: %v", err)
	}

	s := NewSplitter(inputFile, outputDir)
	if err := s.Split(); err != nil {
		t.Fatalf("split failed: %v", err)
	}

	// Verify users.sql contains both statements
	usersContent, err := os.ReadFile(filepath.Join(outputDir, "users.sql"))
	if err != nil {
		t.Fatalf("failed to read users.sql: %v", err)
	}

	if !strings.Contains(string(usersContent), "CREATE TABLE users") {
		t.Error("users.sql should contain CREATE TABLE")
	}
	if !strings.Contains(string(usersContent), "INSERT INTO users") {
		t.Error("users.sql should contain INSERT INTO")
	}
}

func TestSplitter_StringsWithSemicolons(t *testing.T) {
	tmpDir := t.TempDir()
	inputFile := filepath.Join(tmpDir, "input.sql")
	outputDir := filepath.Join(tmpDir, "output")

	content := `CREATE TABLE logs (id INT, message TEXT);
INSERT INTO logs VALUES (1, 'Error: semicolon; in message');
INSERT INTO logs VALUES (2, 'Another; test; message');
`

	if err := os.WriteFile(inputFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write input file: %v", err)
	}

	s := NewSplitter(inputFile, outputDir)
	if err := s.Split(); err != nil {
		t.Fatalf("split failed: %v", err)
	}

	logsContent, err := os.ReadFile(filepath.Join(outputDir, "logs.sql"))
	if err != nil {
		t.Fatalf("failed to read logs.sql: %v", err)
	}

	// Verify the semicolons in strings didn't break parsing
	if !strings.Contains(string(logsContent), "Error: semicolon; in message") {
		t.Error("logs.sql should contain the message with semicolon")
	}

	stats := s.GetStats()
	if stats.StatementsProcessed != 3 {
		t.Errorf("expected 3 statements, got %d", stats.StatementsProcessed)
	}
}

func TestSplitter_GetStats(t *testing.T) {
	tmpDir := t.TempDir()
	inputFile := filepath.Join(tmpDir, "input.sql")
	outputDir := filepath.Join(tmpDir, "output")

	content := `CREATE TABLE users (id INT);
INSERT INTO users VALUES (1);
INSERT INTO users VALUES (2);
`

	if err := os.WriteFile(inputFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write input file: %v", err)
	}

	s := NewSplitter(inputFile, outputDir)

	// Stats should be zero before split
	stats := s.GetStats()
	if stats.StatementsProcessed != 0 {
		t.Errorf("expected 0 statements before split, got %d", stats.StatementsProcessed)
	}

	if err := s.Split(); err != nil {
		t.Fatalf("split failed: %v", err)
	}

	// Stats should be updated after split
	stats = s.GetStats()
	if stats.StatementsProcessed != 3 {
		t.Errorf("expected 3 statements after split, got %d", stats.StatementsProcessed)
	}
	if stats.BytesProcessed == 0 {
		t.Error("expected bytes processed to be > 0")
	}
}

func BenchmarkSplitter_Split(b *testing.B) {
	tmpDir := b.TempDir()
	inputFile := filepath.Join(tmpDir, "bench.sql")

	// Create file with many statements
	var content strings.Builder
	content.WriteString("CREATE TABLE users (id INT);\n")
	for i := 0; i < 1000; i++ {
		content.WriteString("INSERT INTO users VALUES (1);\n")
	}

	if err := os.WriteFile(inputFile, []byte(content.String()), 0644); err != nil {
		b.Fatalf("failed to write test file: %v", err)
	}

	b.ResetTimer()
	b.SetBytes(int64(content.Len()))

	for i := 0; i < b.N; i++ {
		outputDir := filepath.Join(tmpDir, "output", string(rune(i)))
		s := NewSplitter(inputFile, outputDir)
		if err := s.Split(); err != nil {
			b.Fatal(err)
		}
	}
}
