package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"messageGO/internal/models"
)

type DB struct {
	pool *sql.DB
}

func Connect(databaseURL string) (*DB, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres database: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping postgres database: %w", err)
	}

	log.Println("[POSTGRES] Connected successfully to database")
	return &DB{pool: db}, nil
}

func (d *DB) Close() error {
	return d.pool.Close()
}

// InitSchema applies the initial tables if they don't exist
func (d *DB) InitSchema(schemaSQL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := d.pool.ExecContext(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("failed to execute schema initialization: %w", err)
	}
	// Ensure cleared_seq_id column exists for zero-downtime upgrades
	_, _ = d.pool.ExecContext(ctx, `ALTER TABLE conversation_participants ADD COLUMN IF NOT EXISTS cleared_seq_id BIGINT NOT NULL DEFAULT 0;`)
	log.Println("[POSTGRES] Schema initialized successfully")
	return nil
}

// EnsureDirectConversation retrieves or creates a 1:1 direct conversation between two users
func (d *DB) EnsureDirectConversation(ctx context.Context, userA, userB string) (string, error) {
	// Query to find if a direct conversation already contains both participants
	query := `
		SELECT cp1.conversation_id
		FROM conversation_participants cp1
		JOIN conversation_participants cp2 ON cp1.conversation_id = cp2.conversation_id
		JOIN conversations c ON c.id = cp1.conversation_id
		WHERE c.type = 'direct' AND cp1.user_id = $1 AND cp2.user_id = $2
		LIMIT 1;
	`
	var convID string
	err := d.pool.QueryRowContext(ctx, query, userA, userB).Scan(&convID)
	if err == nil {
		return convID, nil
	}

	// Create new direct conversation inside a transaction
	tx, err := d.pool.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	insertConv := `INSERT INTO conversations (type) VALUES ('direct') RETURNING id;`
	if err := tx.QueryRowContext(ctx, insertConv).Scan(&convID); err != nil {
		return "", fmt.Errorf("failed to create conversation: %w", err)
	}

	insertPart := `INSERT INTO conversation_participants (conversation_id, user_id) VALUES ($1, $2), ($1, $3);`
	if _, err := tx.ExecContext(ctx, insertPart, convID, userA, userB); err != nil {
		return "", fmt.Errorf("failed to insert participants: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", err
	}

	return convID, nil
}

// IsParticipant checks if a user is an active participant in a conversation
func (d *DB) IsParticipant(ctx context.Context, convID, userID string) (bool, error) {
	query := `SELECT 1 FROM conversation_participants WHERE conversation_id = $1 AND user_id = $2 LIMIT 1;`
	var dummy int
	err := d.pool.QueryRowContext(ctx, query, convID, userID).Scan(&dummy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// SaveMessageAtomic writes a message with an atomic monotonic sequence number.
// Supports idempotency: if a client_msg_id was already saved for the conversation, the existing message is returned.
func (d *DB) SaveMessageAtomic(ctx context.Context, convID, senderID, clientMsgID, contentType, content string) (*models.Message, error) {
	if contentType == "" {
		contentType = "text"
	}

	// Single atomic CTE query that locks the conversation row, increments last_seq_id, and writes message
	atomicQuery := `
		WITH next_seq AS (
			UPDATE conversations 
			SET last_seq_id = last_seq_id + 1, updated_at = NOW() 
			WHERE id = $1 
			RETURNING last_seq_id
		)
		INSERT INTO messages (conversation_id, seq_id, sender_id, client_msg_id, content_type, content)
		SELECT $1, next_seq.last_seq_id, $2, $3, $4, $5 FROM next_seq
		RETURNING id, conversation_id, seq_id, sender_id, client_msg_id, content_type, content, created_at;
	`

	var msg models.Message
	err := d.pool.QueryRowContext(ctx, atomicQuery, convID, senderID, clientMsgID, contentType, content).Scan(
		&msg.ID,
		&msg.ConversationID,
		&msg.SeqID,
		&msg.SenderID,
		&msg.ClientMsgID,
		&msg.ContentType,
		&msg.Content,
		&msg.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("conversation not found: %s: %w", convID, err)
		}

		// Handle duplicate client_msg_id for idempotency (SQLSTATE 23505 / uq_conversation_client_msg)
		errStr := err.Error()
		if strings.Contains(errStr, "uq_conversation_client_msg") || strings.Contains(errStr, "23505") || strings.Contains(errStr, "duplicate key") {
			var existing models.Message
			fetchQuery := `
				SELECT id, conversation_id, seq_id, sender_id, client_msg_id, content_type, content, created_at
				FROM messages
				WHERE conversation_id = $1 AND client_msg_id = $2
				LIMIT 1;
			`
			fetchErr := d.pool.QueryRowContext(ctx, fetchQuery, convID, clientMsgID).Scan(
				&existing.ID,
				&existing.ConversationID,
				&existing.SeqID,
				&existing.SenderID,
				&existing.ClientMsgID,
				&existing.ContentType,
				&existing.Content,
				&existing.CreatedAt,
			)
			if fetchErr == nil {
				return &existing, nil
			}
		}

		return nil, fmt.Errorf("failed atomic message insert: %w", err)
	}

	return &msg, nil
}

// GetMessagesSince fetches paginated messages with seq_id > sinceSeqID
func (d *DB) GetMessagesSince(ctx context.Context, convID string, sinceSeqID int64, limit int) ([]*models.Message, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}

	query := `
		SELECT id, conversation_id, seq_id, sender_id, client_msg_id, content_type, content, created_at
		FROM messages
		WHERE conversation_id = $1 AND seq_id > $2
		ORDER BY seq_id ASC
		LIMIT $3;
	`

	rows, err := d.pool.QueryContext(ctx, query, convID, sinceSeqID, limit)
	if err != nil {
		return nil, fmt.Errorf("query failed for messages since seq: %w", err)
	}
	defer rows.Close()

	messages := make([]*models.Message, 0)
	for rows.Next() {
		var m models.Message
		if err := rows.Scan(
			&m.ID,
			&m.ConversationID,
			&m.SeqID,
			&m.SenderID,
			&m.ClientMsgID,
			&m.ContentType,
			&m.Content,
			&m.CreatedAt,
		); err != nil {
			return nil, err
		}
		messages = append(messages, &m)
	}

	return messages, nil
}

// GetConversationMessages fetches messages respecting the requesting user's cleared_seq_id
func (d *DB) GetConversationMessages(ctx context.Context, convID, userID string, sinceSeqID int64, limit int) ([]*models.Message, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}

	query := `
		SELECT m.id, m.conversation_id, m.seq_id, m.sender_id, m.client_msg_id, m.content_type, m.content, m.created_at
		FROM messages m
		JOIN conversation_participants cp ON cp.conversation_id = m.conversation_id
		WHERE m.conversation_id = $1 
		  AND cp.user_id = $2
		  AND m.seq_id > GREATEST($3, cp.cleared_seq_id)
		ORDER BY m.seq_id ASC
		LIMIT $4;
	`

	rows, err := d.pool.QueryContext(ctx, query, convID, userID, sinceSeqID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch conversation messages: %w", err)
	}
	defer rows.Close()

	messages := make([]*models.Message, 0)
	for rows.Next() {
		var m models.Message
		if err := rows.Scan(
			&m.ID,
			&m.ConversationID,
			&m.SeqID,
			&m.SenderID,
			&m.ClientMsgID,
			&m.ContentType,
			&m.Content,
			&m.CreatedAt,
		); err != nil {
			return nil, err
		}
		messages = append(messages, &m)
	}

	return messages, nil
}

// UpdateReadCursor updates the watermark read cursor for a user in a conversation
func (d *DB) UpdateReadCursor(ctx context.Context, convID, userID string, seqID int64) error {
	query := `
		UPDATE conversation_participants
		SET last_read_seq_id = GREATEST(last_read_seq_id, $3)
		WHERE conversation_id = $1 AND user_id = $2;
	`
	_, err := d.pool.ExecContext(ctx, query, convID, userID, seqID)
	return err
}

// UpdateDeliveryCursor updates the watermark delivery cursor for a user
func (d *DB) UpdateDeliveryCursor(ctx context.Context, convID, userID string, seqID int64) error {
	query := `
		UPDATE conversation_participants
		SET last_delivered_seq_id = GREATEST(last_delivered_seq_id, $3)
		WHERE conversation_id = $1 AND user_id = $2;
	`
	_, err := d.pool.ExecContext(ctx, query, convID, userID, seqID)
	return err
}

// ClearConversation sets cleared_seq_id to last_seq_id for the user in this conversation (per-user soft clear)
func (d *DB) ClearConversation(ctx context.Context, convID, userID string) error {
	query := `
		UPDATE conversation_participants cp
		SET cleared_seq_id = c.last_seq_id
		FROM conversations c
		WHERE cp.conversation_id = $1 AND cp.user_id = $2 AND c.id = cp.conversation_id;
	`
	res, err := d.pool.ExecContext(ctx, query, convID, userID)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// GetConversationParticipants returns all user IDs in a conversation
func (d *DB) GetConversationParticipants(ctx context.Context, convID string) ([]string, error) {
	query := `SELECT user_id FROM conversation_participants WHERE conversation_id = $1;`
	rows, err := d.pool.QueryContext(ctx, query, convID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var participants []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, err
		}
		participants = append(participants, uid)
	}
	return participants, nil
}

// GetUndeliveredMessages returns all messages across conversations with seq_id > last_delivered_seq_id
func (d *DB) GetUndeliveredMessages(ctx context.Context, userID string) ([]*models.Message, error) {
	query := `
		SELECT m.id, m.conversation_id, m.seq_id, m.sender_id, m.client_msg_id, m.content_type, m.content, m.created_at
		FROM messages m
		JOIN conversation_participants cp ON cp.conversation_id = m.conversation_id
		WHERE cp.user_id = $1 
		  AND m.seq_id > GREATEST(cp.last_delivered_seq_id, cp.cleared_seq_id)
		  AND m.sender_id != $1
		ORDER BY m.seq_id ASC;
	`

	rows, err := d.pool.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to query undelivered messages: %w", err)
	}
	defer rows.Close()

	messages := make([]*models.Message, 0)
	for rows.Next() {
		var m models.Message
		if err := rows.Scan(
			&m.ID,
			&m.ConversationID,
			&m.SeqID,
			&m.SenderID,
			&m.ClientMsgID,
			&m.ContentType,
			&m.Content,
			&m.CreatedAt,
		); err != nil {
			return nil, err
		}
		messages = append(messages, &m)
	}

	return messages, nil
}

// GetUserConversationHistory returns recent messages across all conversations for a user
func (d *DB) GetUserConversationHistory(ctx context.Context, userID string, limit int) ([]*models.Message, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	query := `
		SELECT m.id, m.conversation_id, m.seq_id, m.sender_id, m.client_msg_id, m.content_type, m.content, m.created_at
		FROM messages m
		JOIN conversation_participants cp ON cp.conversation_id = m.conversation_id
		WHERE cp.user_id = $1 AND m.seq_id > cp.cleared_seq_id
		ORDER BY m.created_at ASC, m.seq_id ASC
		LIMIT $2;
	`

	rows, err := d.pool.QueryContext(ctx, query, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query conversation history: %w", err)
	}
	defer rows.Close()

	messages := make([]*models.Message, 0)
	for rows.Next() {
		var m models.Message
		if err := rows.Scan(
			&m.ID,
			&m.ConversationID,
			&m.SeqID,
			&m.SenderID,
			&m.ClientMsgID,
			&m.ContentType,
			&m.Content,
			&m.CreatedAt,
		); err != nil {
			return nil, err
		}
		messages = append(messages, &m)
	}

	return messages, nil
}

// GetUserConversations retrieves all conversations for a user with unread counts & last message preview
func (d *DB) GetUserConversations(ctx context.Context, userID string) ([]*models.ConversationSummary, error) {
	query := `
		SELECT 
			c.id, 
			c.type, 
			COALESCE(c.title, ''), 
			c.last_seq_id, 
			cp.last_read_seq_id, 
			c.updated_at,
			COALESCE((
				SELECT cp2.user_id 
				FROM conversation_participants cp2 
				WHERE cp2.conversation_id = c.id AND cp2.user_id != $1 
				LIMIT 1
			), '') AS peer_id,
			(
				SELECT COUNT(*) 
				FROM messages m 
				WHERE m.conversation_id = c.id 
				  AND m.seq_id > cp.last_read_seq_id 
				  AND m.seq_id > cp.cleared_seq_id 
				  AND m.sender_id != $1
			) AS unread_count
		FROM conversations c
		JOIN conversation_participants cp ON cp.conversation_id = c.id
		WHERE cp.user_id = $1
		ORDER BY c.updated_at DESC;
	`

	rows, err := d.pool.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to query conversations: %w", err)
	}
	defer rows.Close()

	summaries := make([]*models.ConversationSummary, 0)
	for rows.Next() {
		var s models.ConversationSummary
		if err := rows.Scan(
			&s.ID,
			&s.Type,
			&s.Title,
			&s.LastSeqID,
			&s.LastReadSeqID,
			&s.UpdatedAt,
			&s.RecipientID,
			&s.UnreadCount,
		); err != nil {
			return nil, err
		}

		// Fetch last message respecting cleared_seq_id
		lastMsgQuery := `
			SELECT m.id, m.conversation_id, m.seq_id, m.sender_id, m.client_msg_id, m.content_type, m.content, m.created_at
			FROM messages m
			JOIN conversation_participants cp ON cp.conversation_id = m.conversation_id
			WHERE m.conversation_id = $1 AND cp.user_id = $2 AND m.seq_id > cp.cleared_seq_id
			ORDER BY m.seq_id DESC
			LIMIT 1;
		`
		var msg models.Message
		err := d.pool.QueryRowContext(ctx, lastMsgQuery, s.ID, userID).Scan(
			&msg.ID, &msg.ConversationID, &msg.SeqID, &msg.SenderID, &msg.ClientMsgID, &msg.ContentType, &msg.Content, &msg.CreatedAt,
		)
		if err == nil {
			s.LastMessage = &msg
		}

		summaries = append(summaries, &s)
	}

	return summaries, nil
}

// DeleteConversation leaves or deletes a conversation for the user
func (d *DB) DeleteConversation(ctx context.Context, convID, userID string) error {
	query := `DELETE FROM conversation_participants WHERE conversation_id = $1 AND user_id = $2;`
	_, err := d.pool.ExecContext(ctx, query, convID, userID)
	if err != nil {
		return err
	}

	// Delete orphan conversation if no participants left
	orphanQuery := `DELETE FROM conversations WHERE id = $1 AND NOT EXISTS (SELECT 1 FROM conversation_participants WHERE conversation_id = $1);`
	_, _ = d.pool.ExecContext(ctx, orphanQuery, convID)
	return nil
}
