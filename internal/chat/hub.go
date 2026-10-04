package chat

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"messageGO/internal/models"
	"messageGO/internal/storage/postgres"
	"messageGO/internal/storage/redis"
)

type Hub struct {
	// UserID -> Set of active client socket connections (multi-device/tabs)
	clients map[string]map[*Client]struct{}

	// Aliases map: lowercase handle / email / ID -> primary UserID
	aliases map[string]string

	register   chan *Client
	unregister chan *Client

	db    *postgres.DB
	redis *redis.Client

	mu sync.RWMutex
}

func NewHub(db *postgres.DB, redisClient *redis.Client) *Hub {
	return &Hub{
		clients:    make(map[string]map[*Client]struct{}),
		aliases:    make(map[string]string),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		db:         db,
		redis:      redisClient,
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.registerClient(client)

		case client := <-h.unregister:
			h.unregisterClient(client)
		}
	}
}

func (h *Hub) registerClient(client *Client) {
	h.mu.Lock()
	if _, ok := h.clients[client.UserID]; !ok {
		h.clients[client.UserID] = make(map[*Client]struct{})
	}
	h.clients[client.UserID][client] = struct{}{}

	// Register aliases (case-insensitive)
	h.aliases[strings.ToLower(client.UserID)] = client.UserID
	if client.Email != "" {
		h.aliases[strings.ToLower(client.Email)] = client.UserID
	}
	if client.Username != "" {
		h.aliases[strings.ToLower(client.Username)] = client.UserID
	}
	activeSockets := len(h.clients[client.UserID])
	h.mu.Unlock()

	log.Printf("[HUB] Connected: ID=%s (email=%s, handle=%s) | active sockets=%d",
		client.UserID, client.Email, client.Username, activeSockets)

	h.RenewPresence(client.UserID)

	// Fetch & deliver all undelivered messages from PostgreSQL
	go h.flushOfflineMessages(client)
}

func (h *Hub) unregisterClient(client *Client) {
	h.mu.Lock()
	if conns, ok := h.clients[client.UserID]; ok {
		delete(conns, client)
		close(client.Send)
		if len(conns) == 0 {
			delete(h.clients, client.UserID)
			go func(uid string) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = h.redis.RemovePresence(ctx, uid)
			}(client.UserID)
			log.Printf("[HUB] User completely disconnected: %s", client.UserID)
		} else {
			log.Printf("[HUB] User socket closed: %s (remaining sockets: %d)", client.UserID, len(conns))
		}
	}
	h.mu.Unlock()
}

func (h *Hub) Register(client *Client) {
	h.register <- client
}

func (h *Hub) Unregister(client *Client) {
	h.unregister <- client
}

func (h *Hub) RenewPresence(userID string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := h.redis.SetPresence(ctx, userID, 60*time.Second); err != nil {
			log.Printf("[HUB] Failed to update presence for %s: %v", userID, err)
		}
	}()
}

// resolveUserID converts username/email alias into the canonical primary UserID
func (h *Hub) resolveUserID(target string) string {
	target = strings.TrimSpace(strings.ToLower(target))
	if target == "" {
		return ""
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if id, ok := h.aliases[target]; ok {
		return id
	}
	return target
}

// HandleIncomingEnvelope processes frames dispatched from client ReadPump
func (h *Hub) HandleIncomingEnvelope(client *Client, env *models.Envelope) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch env.Type {
	case "chat":
		h.handleChatMessage(ctx, client, env.Data)

	case "delivery_ack":
		h.handleDeliveryACK(ctx, client, env.Data)

	case "read_ack":
		h.handleReadACK(ctx, client, env.Data)

	case "sync_req":
		h.handleSyncRequest(ctx, client, env.Data)

	case "presence_query":
		h.handlePresenceQuery(ctx, client, env.Data)

	default:
		log.Printf("[HUB] Unknown envelope type received: %s from %s", env.Type, client.UserID)
		client.sendError("UNKNOWN_TYPE", "Unrecognized envelope type: "+env.Type)
	}
}

func (h *Hub) handleChatMessage(ctx context.Context, sender *Client, data json.RawMessage) {
	var inbound models.InboundChatMessage
	if err := json.Unmarshal(data, &inbound); err != nil {
		sender.sendError("BAD_PAYLOAD", "Invalid chat payload JSON")
		return
	}

	convID := strings.TrimSpace(inbound.ConversationID)
	rawRecipient := strings.TrimSpace(inbound.RecipientID)

	var recipientID string
	if rawRecipient != "" {
		recipientID = h.resolveUserID(rawRecipient)
	}

	// Validation 1: Prevent messaging self
	if recipientID != "" && (recipientID == sender.UserID || (sender.Username != "" && strings.EqualFold(rawRecipient, sender.Username))) {
		sender.sendError("CANNOT_MESSAGE_SELF", "Cannot send direct message to yourself")
		return
	}

	// Validation 2: If conversation ID is missing, resolve or create 1:1 conversation
	if convID == "" {
		if recipientID == "" {
			sender.sendError("MISSING_TARGET", "Either conversation_id or valid recipient_id (handle/email/id) is required")
			return
		}

		var err error
		convID, err = h.db.EnsureDirectConversation(ctx, sender.UserID, recipientID)
		if err != nil {
			log.Printf("[HUB] Failed to ensure conversation between %s and %s: %v", sender.UserID, recipientID, err)
			sender.sendError("DB_ERROR", "Failed to resolve conversation")
			return
		}
	}

	// Validation 3: Prevent empty or whitespace-only messages
	cleanContent := strings.TrimSpace(inbound.Content)
	if cleanContent == "" {
		sender.sendError("EMPTY_MESSAGE", "Message content cannot be empty or whitespace only")
		return
	}
	if len(cleanContent) > 10000 {
		sender.sendError("MESSAGE_TOO_LARGE", "Message content exceeds limit of 10000 characters")
		return
	}

	clientMsgID := strings.TrimSpace(inbound.ClientMsgID)
	if clientMsgID == "" {
		clientMsgID = uuid.New().String()
	}

	contentType := inbound.ContentType
	if contentType == "" {
		contentType = "text"
	}

	// 1. Atomic sequence assignment and database insertion
	msg, err := h.db.SaveMessageAtomic(ctx, convID, sender.UserID, clientMsgID, contentType, cleanContent)
	if err != nil {
		log.Printf("[HUB] Failed to save message atomically: %v", err)
		sender.sendError("PERSIST_ERROR", "Failed to persist message")
		return
	}

	log.Printf("[HUB] Message saved [conv=%s, seq=%d, sender=%s (%s)]", convID, msg.SeqID, sender.UserID, sender.Username)

	// 2. Resolve conversation participants and route envelope
	participants, err := h.db.GetConversationParticipants(ctx, convID)
	if err != nil {
		log.Printf("[HUB] Error fetching participants for %s: %v", convID, err)
		sender.sendError("ROUTING_ERROR", "Failed to resolve conversation participants")
		return
	}

	msgData, _ := json.Marshal(msg)
	outboundEnv := &models.Envelope{Type: "chat", Data: msgData}
	anyDelivered := false
	recipientOnline := false

	for _, participantID := range participants {
		if participantID == sender.UserID {
			continue // Do not route chat payload to sender
		}
		// Check presence
		online, _ := h.redis.IsOnline(ctx, participantID)
		if online {
			recipientOnline = true
		}
		if h.sendToUser(participantID, outboundEnv) {
			anyDelivered = true
			_ = h.db.UpdateDeliveryCursor(ctx, convID, participantID, msg.SeqID)
		}
	}

	// 3. Deliver enhanced Server ACK back to sender with live status
	recipStatus := "offline"
	if recipientOnline || anyDelivered {
		recipStatus = "online"
	}
	serverAckData, _ := json.Marshal(models.ServerACK{
		ClientMsgID:     clientMsgID,
		MessageID:       msg.ID,
		ConversationID:  convID,
		SeqID:           msg.SeqID,
		Timestamp:       msg.CreatedAt,
		RecipientStatus: recipStatus,
		Delivered:       anyDelivered,
	})
	h.sendToUser(sender.UserID, &models.Envelope{Type: "server_ack", Data: serverAckData})
}

func (h *Hub) handleDeliveryACK(ctx context.Context, client *Client, data json.RawMessage) {
	var ack models.DeliveryACK
	if err := json.Unmarshal(data, &ack); err != nil {
		return
	}

	ack.RecipientID = client.UserID
	ack.Timestamp = time.Now()

	_ = h.db.UpdateDeliveryCursor(ctx, ack.ConversationID, client.UserID, ack.SeqID)

	targetSenderID := h.resolveUserID(ack.SenderID)
	if targetSenderID != "" {
		ackData, _ := json.Marshal(ack)
		h.sendToUser(targetSenderID, &models.Envelope{Type: "delivery_ack", Data: ackData})
	}
}

func (h *Hub) handleReadACK(ctx context.Context, client *Client, data json.RawMessage) {
	var ack models.ReadACK
	if err := json.Unmarshal(data, &ack); err != nil {
		return
	}

	ack.ReaderID = client.UserID
	ack.Timestamp = time.Now()

	_ = h.db.UpdateReadCursor(ctx, ack.ConversationID, client.UserID, ack.SeqID)

	targetSenderID := h.resolveUserID(ack.SenderID)
	if targetSenderID != "" {
		ackData, _ := json.Marshal(ack)
		h.sendToUser(targetSenderID, &models.Envelope{Type: "read_ack", Data: ackData})
	}
}

func (h *Hub) handleSyncRequest(ctx context.Context, client *Client, data json.RawMessage) {
	var req models.SyncRequest
	if err := json.Unmarshal(data, &req); err != nil {
		client.sendError("BAD_SYNC_REQUEST", "Invalid sync request format")
		return
	}

	convID := strings.TrimSpace(req.ConversationID)
	if convID == "" && req.RecipientID != "" {
		targetID := h.resolveUserID(req.RecipientID)
		if targetID != "" {
			var err error
			convID, err = h.db.EnsureDirectConversation(ctx, client.UserID, targetID)
			if err != nil {
				log.Printf("[HUB] Sync conversation resolution failed: %v", err)
			}
		}
	}

	if convID == "" {
		client.sendError("BAD_SYNC_REQUEST", "conversation_id or recipient_id is required")
		return
	}

	messages, err := h.db.GetMessagesSince(ctx, convID, req.SinceSeqID, req.Limit)
	if err != nil {
		log.Printf("[HUB] Sync query failed for %s: %v", convID, err)
		client.sendError("SYNC_FAILED", "Failed to retrieve missed messages")
		return
	}

	var latestSeq int64 = req.SinceSeqID
	if len(messages) > 0 {
		latestSeq = messages[len(messages)-1].SeqID
	}

	hasMore := len(messages) == req.Limit && req.Limit > 0

	resData, _ := json.Marshal(models.SyncResponse{
		ConversationID: convID,
		Messages:       messages,
		LatestSeqID:    latestSeq,
		HasMore:        hasMore,
	})

	select {
	case client.Send <- &models.Envelope{Type: "sync_res", Data: resData}:
	default:
		log.Printf("[HUB] Could not send sync response to %s: socket buffer full", client.UserID)
	}
}

func (h *Hub) handlePresenceQuery(ctx context.Context, client *Client, data json.RawMessage) {
	var query models.PresenceQuery
	if err := json.Unmarshal(data, &query); err != nil {
		return
	}

	targetID := h.resolveUserID(query.TargetUserID)
	online, err := h.redis.IsOnline(ctx, targetID)
	status := "offline"
	if err == nil && online {
		status = "online"
	}

	resData, _ := json.Marshal(models.PresenceResponse{
		UserID: query.TargetUserID,
		Status: status,
	})

	select {
	case client.Send <- &models.Envelope{Type: "presence_res", Data: resData}:
	default:
	}
}

// flushOfflineMessages restores complete conversation history and missed messages from Postgres upon connection
func (h *Hub) flushOfflineMessages(client *Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Stream complete conversation history from Postgres so reconnecting clients see previous + queued messages
	history, err := h.db.GetUserConversationHistory(ctx, client.UserID, 100)
	if err != nil {
		log.Printf("[HUB] Failed to fetch conversation history from Postgres for %s: %v", client.UserID, err)
	} else if len(history) > 0 {
		log.Printf("[HUB] Streaming %d history messages from Postgres to %s", len(history), client.UserID)
		for _, msg := range history {
			msgData, _ := json.Marshal(msg)
			select {
			case client.Send <- &models.Envelope{Type: "chat", Data: msgData}:
				_ = h.db.UpdateDeliveryCursor(ctx, msg.ConversationID, client.UserID, msg.SeqID)
			default:
				log.Printf("[HUB] Buffer full streaming history message %d to %s", msg.SeqID, client.UserID)
			}
		}
	}

	// 2. Drain any transient Redis queue items
	_, _ = h.redis.GetAndFlushOfflineQueue(ctx, client.UserID)
}

// sendToUser delivers an envelope to active sockets, or enqueues to Redis if offline. Returns true if delivered.
func (h *Hub) sendToUser(userID string, env *models.Envelope) bool {
	h.mu.RLock()
	conns, exists := h.clients[userID]
	if !exists || len(conns) == 0 {
		h.mu.RUnlock()
		// Recipient is offline -> persist in Redis offline queue
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := h.redis.EnqueueOffline(ctx, userID, env); err != nil {
				log.Printf("[HUB] Failed to enqueue offline envelope for %s: %v", userID, err)
			}
		}()
		return false
	}

	delivered := false
	for client := range conns {
		select {
		case client.Send <- env:
			delivered = true
		default:
			log.Printf("[HUB] Buffer full for user %s. Evicting slow socket.", userID)
			go func(c *Client) {
				h.Unregister(c)
			}(client)
		}
	}
	h.mu.RUnlock()
	return delivered
}
