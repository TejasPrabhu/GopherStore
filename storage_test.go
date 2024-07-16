package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/tejasprabhu/GopherStore/datamgmt"
)

func TestStorageService_GenerateFilePath(t *testing.T) {
    service := NewStorageService("test_address")
    data := &datamgmt.Data{
        ID:        "1",
        Filename:  "testfile",
        Extension: "txt",
    }
    expectedPath := filepath.Join(service.rootPath, "6b86b2", "testfile.txt")
    path, err := service.generateFilePath(data)
    if err != nil {
        t.Errorf("GenerateFilePath() error = %v", err)
    }
    if path != expectedPath {
        t.Errorf("Expected %s, got %s", expectedPath, path)
    }
}

func TestStorageService_StoreData(t *testing.T) {
    service := NewStorageService("test_address")
    defer os.RemoveAll(service.rootPath) // Clean up after test

    data := &datamgmt.Data{
        ID:        "1",
        Filename:  "testfile",
        Extension: "txt",
    }
    content := bytes.NewReader([]byte("Hello, world!"))

    // Test storing data
    if err := service.StoreData(data, content); err != nil {
        t.Errorf("StoreData() error = %v", err)
    }

    // Verify file exists
    filePath, _ := service.generateFilePath(data)
    if _, err := os.Stat(filePath); os.IsNotExist(err) {
        t.Errorf("File does not exist: %s", filePath)
    }
}

func TestStorageService_ReadData(t *testing.T) {
    service := NewStorageService("test_address")
    defer os.RemoveAll(service.rootPath) // Clean up after test

    data := &datamgmt.Data{
        ID:        "1",
        Filename:  "testfile",
        Extension: "txt",
    }
    expectedContent := "Hello, world!"
    content := bytes.NewReader([]byte(expectedContent))
    if err := service.StoreData(data, content); err != nil {
        t.Errorf("StoreData() error = %v", err)
    }

    // Test reading data
    reader, err := service.ReadData(data)
    if err != nil {
        t.Errorf("ReadData() error = %v", err)
    }
    result, _ := io.ReadAll(reader)
    reader.Close()

    if string(result) != expectedContent {
        t.Errorf("Expected %s, got %s", expectedContent, string(result))
    }
}

func TestStorageService_DeleteData(t *testing.T) {
    service := NewStorageService("test_address")
    defer os.RemoveAll(service.rootPath) // Clean up after test

    data := &datamgmt.Data{
        ID:        "1",
        Filename:  "testfile",
        Extension: "txt",
    }
    content := bytes.NewReader([]byte("Hello, world!"))
    if err := service.StoreData(data, content); err != nil {
        t.Errorf("StoreData() error = %v", err)
    }

    // Test deleting data
    if err := service.DeleteData(data); err != nil {
        t.Errorf("DeleteData() error = %v", err)
    }

    // Verify file does not exist
    filePath, _ := service.generateFilePath(data)
    if _, err := os.Stat(filePath); !os.IsNotExist(err) {
        t.Errorf("File still exists after delete: %s", filePath)
    }
}