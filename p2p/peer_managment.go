package p2p

import (
	"log"
	"sync"
	"time"

	"github.com/tejasprabhu/GopherStore/logger"
)
type Peer struct {
    ID        string
    Address   string
    Connected bool
    LastSeen  time.Time
}

type PeerManager struct {
    peers map[string]*Peer  // Maps peer IDs to Peer structs
    mu    sync.RWMutex      // Protects the peers map
}

func NewPeerManager() *PeerManager {
    return &PeerManager{
        peers: make(map[string]*Peer),
    }
}

func (pm *PeerManager) AddPeer(peer *Peer) {
    pm.mu.Lock()
    defer pm.mu.Unlock()
    pm.peers[peer.ID] = peer
    log.Printf("Added new peer: %s", peer.ID)
}

func (pm *PeerManager) RemovePeer(peerID string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.peers, peerID)
	log.Printf("Removed peer: %s", peerID)
}

func (pm *PeerManager) CheckPeers(transport *TCPTransport) {
    pm.mu.Lock()
    defer pm.mu.Unlock()
    for id, peer := range pm.peers {
        if time.Since(peer.LastSeen) > 5*time.Minute {
            if tryReconnect(peer, transport) {
                log.Printf("Peer %s reconnected", id)
                peer.Connected = true
            } else {
                log.Printf("Peer %s is inactive", id)
                peer.Connected = false
            }
        }
    }
}


func tryReconnect(peer *Peer, transport *TCPTransport) bool {
    conn, err := transport.Dial(peer.Address)
    if err != nil {
        log.Printf("Failed to reconnect to %s: %v", peer.Address, err)
        return false
    }
    if err := conn.Close(); err != nil {
        logger.Log.WithError(err).Error("Failed to close connection")
    }
    log.Printf("Successfully reconnected to %s", peer.Address)
    return true
}

// Example of method to update the LastSeen timestamp
func (pm *PeerManager) UpdateLastSeen(peerID string) {
    pm.mu.Lock()
    defer pm.mu.Unlock()
    if peer, exists := pm.peers[peerID]; exists {
        peer.LastSeen = time.Now()
        peer.Connected = true
    }
}
