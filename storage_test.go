package main

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/tejasprabhu/GopherStore/p2p"
)

func TestStorageService(t *testing.T) {
	service := NewStorageService()

	// Test data to store
	testData := &p2p.Data{
		ID:        "unique-id-1",
		Filename:  "testfile",
		Extension: ".txt",
	}

	// Creating a reader from the byte slice to simulate file content
	content := bytes.NewReader([]byte("Hello World"))

	// Store data using reader
	if err := service.StoreData(testData, content); err != nil {
		t.Fatalf("StoreData failed: %v", err)
	}

	// Generate file path to verify file existence
	path, err := service.generateFilePath(testData)
	if err != nil {
		t.Fatalf("generateFilePath failed: %v", err)
	}

	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("file %s does not exist", path)
	}

	// Read data
	readCloser, err := service.ReadData(testData)
	if err != nil {
		t.Fatalf("ReadData failed: %v", err)
	}
	defer readCloser.Close()

	// Compare the content read back from the file
	readBuffer := new(bytes.Buffer)
	if _, err := io.Copy(readBuffer, readCloser); err != nil {
		t.Fatalf("Failed to read data back: %v", err)
	}
	if !bytes.Equal(readBuffer.Bytes(), []byte("Hello World")) {
		t.Errorf("ReadData content mismatch: got %v, want %v", readBuffer.Bytes(), []byte("Hello World"))
	}

	// Delete data
	if err := service.DeleteData(testData); err != nil {
		t.Fatalf("DeleteData failed: %v", err)
	}

	// Verify deletion
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("file %s still exists after delete", path)
	}
}
