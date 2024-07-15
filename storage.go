package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tejasprabhu/GopherStore/datamgmt"
	"github.com/tejasprabhu/GopherStore/logger" // Assuming logger is set up correctly for structured logging
)

// StorageService handles the storage operations for data objects.
type StorageService struct {
    rootPath string
    mutex    sync.Mutex
}

const storageRootDir = "data_storage"

// NewStorageService initializes a new storage service with a dedicated storage directory.
func NewStorageService(address string) *StorageService {
    root := filepath.Join(storageRootDir, address)
    if err := os.MkdirAll(root, 0740); err != nil {
        logger.Log.WithError(err).Fatal("Unable to create root storage directory")
    }
    return &StorageService{rootPath: root}
}

// StoreData writes data from a reader into a file determined by the datamgmt.Data object.
func (s *StorageService) StoreData(data *datamgmt.Data, reader io.Reader) error {
    s.mutex.Lock()
    defer s.mutex.Unlock()

    path, err := s.generateFilePath(data)
    if err != nil {
        logger.Log.WithError(err).Error("Error generating file path")
        return err
    }

    if err := os.MkdirAll(filepath.Dir(path), 0740); err != nil {
        logger.Log.WithError(err).Error("Error creating directories for file")
        return err
    }

    file, err := createFile(path)
    if err != nil {
        logger.Log.WithError(err).Error("Error creating file")
        return err
    }
    defer file.Close()

    if written, err := io.Copy(file, reader); err != nil {
        logger.Log.WithError(err).Error("Error writing data to file")
        return err
    } else if written == 0 {
        logger.Log.Warn("No data written to file, check input stream")
    } else {
        logger.Log.WithField("path", file.Name()).WithField("bytes_written", written).Info("Data stored successfully")
    }

    return nil
}

// ReadData opens a file for reading based on the provided datamgmt.Data object.
func (s *StorageService) ReadData(data *datamgmt.Data) (io.ReadCloser, error) {
    s.mutex.Lock()
    defer s.mutex.Unlock()

    path, err := s.generateFilePath(data)
    if err != nil {
        logger.Log.WithError(err).Error("Error generating file path")
        return nil, err
    }

    file, err := openFile(path)
    if err != nil {
        logger.Log.WithError(err).Error("Error opening data file")
        return nil, err
    }

    logger.Log.WithField("path", path).Info("Data file opened successfully")
    return file, nil
}

// DeleteData removes a file based on the provided datamgmt.Data object.
func (s *StorageService) DeleteData(data *datamgmt.Data) error {
    s.mutex.Lock()
    defer s.mutex.Unlock()

    path, err := s.generateFilePath(data)
    if err != nil {
        logger.Log.WithError(err).Error("Error generating file path")
        return err
    }

    if err := os.Remove(path); err != nil {
        logger.Log.WithError(err).Error("Error deleting file")
        return err
    }

    logger.Log.WithField("path", path).Info("Data deleted successfully")
    return nil
}

// generateFilePath creates a filepath for storing data using a hash of the data ID.
func (s *StorageService) generateFilePath(data *datamgmt.Data) (string, error) {
    hash := sha256.Sum256([]byte(data.ID))
    subfolder := hex.EncodeToString(hash[:3]) // Use first 3 bytes of hash for subfolder
    filename := fmt.Sprintf("%s.%s", data.Filename, data.Extension)
    return filepath.Join(s.rootPath, subfolder, filename), nil
}


func sanitizeFilePath(filePath string) (string, error) {
    cleanPath := filepath.Clean(filePath)
    if strings.Contains(cleanPath, "..") || filepath.IsAbs(cleanPath) {
        return "", errors.New("invalid file path")
    }
    return cleanPath, nil
}

func openFile(filePath string) (*os.File, error) {
    cleanPath, err := sanitizeFilePath(filePath)
    if err != nil {
        return nil, err
    }
    file, err := os.Open(cleanPath)
    if err != nil {
        logger.Log.WithError(err).Error("Failed to open file")
        return nil, err
    }
    return file, nil
}

func createFile(filePath string) (*os.File, error) {
    cleanPath, err := sanitizeFilePath(filePath)
    if err != nil {
        return nil, err
    }
    file, err := os.Create(cleanPath)
    if err != nil {
        logger.Log.WithError(err).Error("Failed to create file")
        return nil, err
    }
    return file, nil
}