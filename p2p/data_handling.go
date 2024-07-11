package p2p

import (
    "compress/gzip"
    "encoding/gob"
    "io"
    "log"
    "net"
)

func compressAndEncode(conn net.Conn) (*gzip.Writer, *gob.Encoder) {
    gzipWriter := gzip.NewWriter(conn)
    encoder := gob.NewEncoder(gzipWriter)
    return gzipWriter, encoder
}

func decompressAndDecode(conn net.Conn) (*gzip.Reader, *gob.Decoder, error) {
    gzipReader, err := gzip.NewReader(conn)
    if err != nil {
        return nil, nil, err
    }
    decoder := gob.NewDecoder(gzipReader)
    return gzipReader, decoder, nil
}

func handleClient(conn net.Conn) {
    gzipWriter, encoder := compressAndEncode(conn)
    defer gzipWriter.Close()

    gzipReader, decoder, err := decompressAndDecode(conn)
    if err != nil {
        log.Printf("error: setting up gzip reader, err: %v", err)
        return
    }
    defer gzipReader.Close()

    for {
        var data Data // Assuming Data is a struct that represents your data model
        if err := decoder.Decode(&data); err != nil {
            if err != io.EOF {
                log.Printf("error: decoding data from %s, err: %v", conn.RemoteAddr(), err)
            }
            break
        }

        log.Printf("Received data: %+v", data)

        if err := encoder.Encode(data); err != nil {
            log.Printf("error: encoding response to %s, err: %v", conn.RemoteAddr(), err)
            break
        }
    }
}
