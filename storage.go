package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/tejasprabhu/GopherStore/datamgmt"
)

const storageRootDir = "data_storage"

type StorageService struct {
    rootPath string
    lock     sync.Mutex
}

func NewStorageService(address string) *StorageService {
    root := fmt.Sprintf("%s_%s", storageRootDir, address)
    if err := os.MkdirAll(root, 0755); err != nil {
        log.Fatalf("Unable to create root storage directory: %v", err)
    }
    return &StorageService{rootPath: root}
}

func (s *StorageService) StoreData(data *datamgmt.Data, r io.Reader) error {
    log.Println("storing content")
    s.lock.Lock()
    defer s.lock.Unlock()

    path, err := s.generateFilePath(data)
    if err != nil {
        log.Printf("Error generating file path: %v", err)
        return err
    }

    log.Printf("store path: %s", path)

    if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
        log.Printf("Error creating directories for file: %v", err)
        return err
    }

    file, err := os.Create(path)
    if err != nil {
        log.Printf("Error creating file: %v", err)
        return err
    }
    defer file.Close()

    // Write directly from the reader to the file
    written, err := io.Copy(file, r)
    if err != nil {
        log.Printf("Error writing stream to file: %v", err)
        return err
    }
    if written == 0 {
        log.Printf("No data written to file, check stream content.")
    } else {
        log.Printf("Data streamed and stored successfully: %s, bytes written: %d", file.Name(), written)
    }

    return nil
}


func (s *StorageService) ReadData(data *datamgmt.Data) (io.ReadCloser, error) {
    s.lock.Lock()
    defer s.lock.Unlock()

    path, _ := s.generateFilePath(data)
    fmt.Println(path)
    file, err := os.Open(path)
    if err != nil {
        log.Printf("Error opening data file: %v", err)
        return nil, err
    }

    log.Printf("Data stream opened successfully: %s", path)
    return file, nil
}


func (s *StorageService) DeleteData(data *datamgmt.Data) error {
    s.lock.Lock()
    defer s.lock.Unlock()

    path, _ := s.generateFilePath(data)
    if err := os.Remove(path); err != nil {
        log.Printf("Error deleting file: %v", err)
        return err
    }

    log.Printf("Data deleted successfully: %s", path)
    return nil
}

func (s *StorageService) generateFilePath(data *datamgmt.Data) (string, error) {
    hash := sha256.Sum256([]byte(data.ID))
    subfolder := hex.EncodeToString(hash[:3]) // Uses the first 3 bytes of the hash for subfolder
    filename := fmt.Sprintf("%s.%s", data.Filename, data.Extension)
    return filepath.Join(s.rootPath, subfolder, filename), nil
}