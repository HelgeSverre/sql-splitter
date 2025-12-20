package analyzer

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAnalyzer_Analyze(t *testing.T) {
	// Create temp file with SQL content
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.sql")

	content := `CREATE TABLE users (id INT, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
CREATE TABLE posts (id INT, user_id INT, title VARCHAR(255));
INSERT INTO posts VALUES (1, 1, 'Hello World');
`

	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	// Create analyzer and run
	a := NewAnalyzer(testFile)
	stats, err := a.Analyze()
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}

	// Verify results
	if len(stats) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(stats))
	}

	// Results should be sorted by insert count (descending)
	// users has 2 inserts, posts has 1
	if stats[0].TableName != "users" {
		t.Errorf("expected first table to be 'users', got %q", stats[0].TableName)
	}
	if stats[0].InsertCount != 2 {
		t.Errorf("expected users to have 2 inserts, got %d", stats[0].InsertCount)
	}
	if stats[0].CreateCount != 1 {
		t.Errorf("expected users to have 1 create, got %d", stats[0].CreateCount)
	}

	if stats[1].TableName != "posts" {
		t.Errorf("expected second table to be 'posts', got %q", stats[1].TableName)
	}
	if stats[1].InsertCount != 1 {
		t.Errorf("expected posts to have 1 insert, got %d", stats[1].InsertCount)
	}
}

func TestAnalyzer_AnalyzeWithProgress(t *testing.T) {
	// Create temp file with SQL content
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.sql")

	content := `CREATE TABLE users (id INT);
INSERT INTO users VALUES (1);
INSERT INTO users VALUES (2);
`

	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	// Track progress calls
	var progressCalls []int64
	progressFn := func(bytesRead int64) {
		progressCalls = append(progressCalls, bytesRead)
	}

	// Create analyzer and run with progress
	a := NewAnalyzer(testFile)
	stats, err := a.AnalyzeWithProgress(progressFn)
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}

	// Verify stats
	if len(stats) != 1 {
		t.Fatalf("expected 1 table, got %d", len(stats))
	}
	if stats[0].TableName != "users" {
		t.Errorf("expected 'users', got %q", stats[0].TableName)
	}

	// Verify progress was called
	if len(progressCalls) == 0 {
		t.Error("expected progress callback to be called")
	}
}

func TestAnalyzer_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "empty.sql")

	if err := os.WriteFile(testFile, []byte(""), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	a := NewAnalyzer(testFile)
	stats, err := a.Analyze()
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}

	if len(stats) != 0 {
		t.Errorf("expected 0 tables, got %d", len(stats))
	}
}

func TestAnalyzer_FileNotFound(t *testing.T) {
	a := NewAnalyzer("/nonexistent/file.sql")
	_, err := a.Analyze()
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestAnalyzer_AllStatementTypes(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.sql")

	content := `CREATE TABLE users (id INT);
INSERT INTO users VALUES (1);
CREATE INDEX idx_users ON users (id);
ALTER TABLE users ADD COLUMN email VARCHAR(255);
DROP TABLE old_users;
`

	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	a := NewAnalyzer(testFile)
	stats, err := a.Analyze()
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}

	// Should find 2 tables: users (from CREATE, INSERT, CREATE INDEX, ALTER) and old_users (from DROP)
	if len(stats) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(stats))
	}

	// Find users table
	var usersStats *TableStats
	for _, s := range stats {
		if s.TableName == "users" {
			usersStats = s
			break
		}
	}

	if usersStats == nil {
		t.Fatal("expected to find 'users' table")
	}

	if usersStats.CreateCount != 1 {
		t.Errorf("expected 1 create, got %d", usersStats.CreateCount)
	}
	if usersStats.InsertCount != 1 {
		t.Errorf("expected 1 insert, got %d", usersStats.InsertCount)
	}
	// Total statements for users: CREATE TABLE, INSERT, CREATE INDEX, ALTER TABLE = 4
	if usersStats.StatementCount != 4 {
		t.Errorf("expected 4 statements, got %d", usersStats.StatementCount)
	}
}

func TestAnalyzer_TableWithBackticks(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.sql")

	content := "CREATE TABLE `my_table` (id INT);\nINSERT INTO `my_table` VALUES (1);\n"

	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	a := NewAnalyzer(testFile)
	stats, err := a.Analyze()
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}

	if len(stats) != 1 {
		t.Fatalf("expected 1 table, got %d", len(stats))
	}
	if stats[0].TableName != "my_table" {
		t.Errorf("expected 'my_table', got %q", stats[0].TableName)
	}
}

func BenchmarkAnalyzer_Analyze(b *testing.B) {
	tmpDir := b.TempDir()
	testFile := filepath.Join(tmpDir, "bench.sql")

	// Create file with many statements
	var content string
	content += "CREATE TABLE users (id INT);\n"
	for i := 0; i < 1000; i++ {
		content += "INSERT INTO users VALUES (1);\n"
	}

	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		b.Fatalf("failed to write test file: %v", err)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(content)))

	for i := 0; i < b.N; i++ {
		a := NewAnalyzer(testFile)
		_, err := a.Analyze()
		if err != nil {
			b.Fatal(err)
		}
	}
}
