package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"messageGO/internal/auth"
	"messageGO/internal/chat"
	"messageGO/internal/config"
	"messageGO/internal/models"
	"messageGO/internal/storage/postgres"
	"messageGO/internal/storage/redis"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Permissive origin check for development
	},
}

func main() {
	log.Println("[SERVER] Initializing messageGO Chat Gateway...")

	// 1. Load configuration
	cfg := config.LoadConfig()

	// 2. Initialize PostgreSQL connection
	db, err := postgres.Connect(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("[SERVER] PostgreSQL connection failed: %v", err)
	}
	defer db.Close()

	// Auto-apply schema migrations if file exists
	schemaSQL, err := os.ReadFile("internal/storage/postgres/schema.sql")
	if err == nil {
		if err := db.InitSchema(string(schemaSQL)); err != nil {
			log.Printf("[SERVER] Warning: Failed to apply schema SQL: %v", err)
		}
	}

	// 3. Initialize Redis connection
	redisClient, err := redis.Connect(cfg.RedisAddr, cfg.RedisPassword)
	if err != nil {
		log.Fatalf("[SERVER] Redis connection failed: %v", err)
	}
	defer redisClient.Close()

	// 4. Instantiate central message hub
	hub := chat.NewHub(db, redisClient)
	go hub.Run()

	// 5. Register HTTP & WebSocket routes
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"healthy","service":"messageGO"}`))
	})

	// Ticket generation endpoint (secure handshake token)
	http.HandleFunc("/api/v1/ws/ticket", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodOptions {
			return
		}
		handleCreateWSTicket(w, r, cfg, redisClient)
	})

	// Conversation REST APIs
	http.HandleFunc("/api/v1/conversations", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodOptions {
			return
		}
		handleConversations(w, r, cfg, db, redisClient)
	})

	http.HandleFunc("/api/v1/conversations/", func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		if r.Method == http.MethodOptions {
			return
		}
		handleConversationItem(w, r, cfg, db, redisClient)
	})

	// WebSocket Gateway
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(cfg, hub, redisClient, w, r)
	})

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: nil,
	}

	// 6. Graceful shutdown handler
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
		<-quit
		log.Println("[SERVER] Shutting down gracefully...")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Fatalf("[SERVER] Server forced to shutdown: %v", err)
		}
	}()

	log.Printf("[SERVER] messageGO Chat Gateway listening on :%s", cfg.Port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[SERVER] Server crashed: %v", err)
	}
}

func enableCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
}

func authenticate(r *http.Request, cfg *config.Config, rdb *redis.Client) (*auth.JWTCustomClaims, error) {
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return nil, errors.New("missing or malformed Authorization header")
	}
	tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
	return auth.ValidateAccessTokenWithRevocation(r.Context(), tokenStr, cfg.JWTAccessSecret, rdb)
}

func handleCreateWSTicket(w http.ResponseWriter, r *http.Request, cfg *config.Config, rdb *redis.Client) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	claims, err := authenticate(r, cfg, rdb)
	if err != nil {
		http.Error(w, "Unauthorized: "+err.Error(), http.StatusUnauthorized)
		return
	}

	username := claims.Email
	if strings.Contains(claims.Email, "@") {
		username = strings.Split(claims.Email, "@")[0]
	}

	ticket := uuid.New().String()
	ticketPayload, _ := json.Marshal(map[string]string{
		"user_id":  claims.UserID,
		"email":    claims.Email,
		"username": username,
	})

	if err := rdb.CreateWSTicket(r.Context(), ticket, ticketPayload, 30*time.Second); err != nil {
		http.Error(w, "Failed to create ticket", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(models.WSTicketResponse{
		Ticket:    ticket,
		ExpiresIn: 30,
	})
}

func handleConversations(w http.ResponseWriter, r *http.Request, cfg *config.Config, db *postgres.DB, rdb *redis.Client) {
	claims, err := authenticate(r, cfg, rdb)
	if err != nil {
		http.Error(w, "Unauthorized: "+err.Error(), http.StatusUnauthorized)
		return
	}

	switch r.Method {
	case http.MethodGet:
		summaries, err := db.GetUserConversations(r.Context(), claims.UserID)
		if err != nil {
			log.Printf("[SERVER] GetUserConversations error: %v", err)
			http.Error(w, "Failed to fetch conversations", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(summaries)

	case http.MethodPost:
		var req models.CreateConversationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.RecipientID) == "" {
			http.Error(w, "Invalid request body: recipient_id is required", http.StatusBadRequest)
			return
		}
		if req.RecipientID == claims.UserID {
			http.Error(w, "Cannot create conversation with yourself", http.StatusBadRequest)
			return
		}

		convID, err := db.EnsureDirectConversation(r.Context(), claims.UserID, strings.TrimSpace(req.RecipientID))
		if err != nil {
			log.Printf("[SERVER] EnsureDirectConversation error: %v", err)
			http.Error(w, "Failed to create conversation", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"conversation_id": convID,
			"recipient_id":    req.RecipientID,
		})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleConversationItem(w http.ResponseWriter, r *http.Request, cfg *config.Config, db *postgres.DB, rdb *redis.Client) {
	claims, err := authenticate(r, cfg, rdb)
	if err != nil {
		http.Error(w, "Unauthorized: "+err.Error(), http.StatusUnauthorized)
		return
	}

	pathRemainder := strings.TrimPrefix(r.URL.Path, "/api/v1/conversations/")
	parts := strings.Split(strings.Trim(pathRemainder, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "Missing conversation ID", http.StatusBadRequest)
		return
	}

	convID := parts[0]

	// 1. DELETE /api/v1/conversations/{id}
	if len(parts) == 1 && r.Method == http.MethodDelete {
		if err := db.DeleteConversation(r.Context(), convID, claims.UserID); err != nil {
			http.Error(w, "Failed to delete conversation", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "success", "message": "Conversation removed"})
		return
	}

	// 2. GET /api/v1/conversations/{id}/messages
	if len(parts) == 2 && parts[1] == "messages" && r.Method == http.MethodGet {
		sinceSeqID, _ := strconv.ParseInt(r.URL.Query().Get("since_seq_id"), 10, 64)
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 || limit > 100 {
			limit = 50
		}

		msgs, err := db.GetConversationMessages(r.Context(), convID, claims.UserID, sinceSeqID, limit)
		if err != nil {
			http.Error(w, "Failed to retrieve messages", http.StatusInternalServerError)
			return
		}

		var latestSeq int64 = sinceSeqID
		if len(msgs) > 0 {
			latestSeq = msgs[len(msgs)-1].SeqID
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(models.SyncResponse{
			ConversationID: convID,
			Messages:       msgs,
			LatestSeqID:    latestSeq,
			HasMore:        len(msgs) == limit,
		})
		return
	}

	// 3. POST /api/v1/conversations/{id}/clear
	if len(parts) == 2 && parts[1] == "clear" && r.Method == http.MethodPost {
		if err := db.ClearConversation(r.Context(), convID, claims.UserID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				http.Error(w, "Conversation not found or not a participant", http.StatusNotFound)
				return
			}
			http.Error(w, "Failed to clear chat history", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":          "success",
			"conversation_id": convID,
			"message":         "Chat history cleared successfully for user",
		})
		return
	}

	http.Error(w, "Endpoint not found", http.StatusNotFound)
}

func serveWS(cfg *config.Config, hub *chat.Hub, rdb *redis.Client, w http.ResponseWriter, r *http.Request) {
	var userID, email, username string

	// 1. Primary auth: Single-use short-lived ticket
	ticket := r.URL.Query().Get("ticket")
	if ticket != "" {
		data, err := rdb.ConsumeWSTicket(r.Context(), ticket)
		if err != nil || len(data) == 0 {
			log.Printf("[SERVER] Handshake rejected: invalid or expired ticket")
			http.Error(w, "Unauthorized: Invalid or expired ticket", http.StatusUnauthorized)
			return
		}
		var parsed map[string]string
		if err := json.Unmarshal(data, &parsed); err == nil {
			userID = parsed["user_id"]
			email = parsed["email"]
			username = parsed["username"]
		}
	}

	// 2. Secondary auth: Authorization header / query token with Redis revocation validation
	if userID == "" {
		tokenStr := r.URL.Query().Get("token")
		if tokenStr == "" {
			authHeader := r.Header.Get("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ") {
				tokenStr = strings.TrimPrefix(authHeader, "Bearer ")
			}
		}

		if tokenStr != "" {
			claims, err := auth.ValidateAccessTokenWithRevocation(r.Context(), tokenStr, cfg.JWTAccessSecret, rdb)
			if err != nil {
				log.Printf("[SERVER] Handshake rejected: invalid or revoked token: %v", err)
				http.Error(w, "Unauthorized: "+err.Error(), http.StatusUnauthorized)
				return
			}
			userID = claims.UserID
			email = claims.Email
			if strings.Contains(email, "@") {
				username = strings.Split(email, "@")[0]
			} else {
				username = email
			}
		} else if cfg.Env == "development" && r.URL.Query().Get("user_id") != "" {
			// Dev fallback for quick manual testing without JWT
			userID = r.URL.Query().Get("user_id")
			email = userID
			username = userID
			log.Printf("[SERVER] Handshake in dev mode for user_id=%s without token", userID)
		} else {
			log.Println("[SERVER] Handshake rejected: missing authentication credentials")
			http.Error(w, "Unauthorized: Missing authentication ticket or token", http.StatusUnauthorized)
			return
		}
	}

	// 3. Upgrade HTTP connection to WebSocket protocol
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[SERVER] Upgrade error for user %s: %v", userID, err)
		return
	}

	// 4. Create client wrapper and register with Hub
	client := chat.NewClient(userID, email, username, hub, conn)
	hub.Register(client)

	// 5. Spawn read/write pumps
	go client.WritePump()
	go client.ReadPump()
}
