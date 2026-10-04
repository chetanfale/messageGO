package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"messageGO/internal/models"
)

type Client struct {
	rdb *redis.Client
}

func Connect(addr, password string) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to ping redis: %w", err)
	}

	log.Println("[REDIS] Connected successfully to Redis")
	return &Client{rdb: rdb}, nil
}

func (c *Client) Close() error {
	return c.rdb.Close()
}

// SetPresence marks a user online with a specified TTL (e.g. 60 seconds)
func (c *Client) SetPresence(ctx context.Context, userID string, ttl time.Duration) error {
	key := fmt.Sprintf("presence:%s", userID)
	return c.rdb.Set(ctx, key, "online", ttl).Err()
}

// RemovePresence immediately marks a user offline
func (c *Client) RemovePresence(ctx context.Context, userID string) error {
	key := fmt.Sprintf("presence:%s", userID)
	return c.rdb.Del(ctx, key).Err()
}

// IsOnline checks if the presence key exists and is still valid
func (c *Client) IsOnline(ctx context.Context, userID string) (bool, error) {
	key := fmt.Sprintf("presence:%s", userID)
	val, err := c.rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return val > 0, nil
}

// EnqueueOffline stores an undelivered envelope in Redis list for offline recipient
func (c *Client) EnqueueOffline(ctx context.Context, userID string, env *models.Envelope) error {
	bytes, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("failed to marshal envelope for offline queue: %w", err)
	}
	key := fmt.Sprintf("offline:%s", userID)
	return c.rdb.RPush(ctx, key, bytes).Err()
}

// GetAndFlushOfflineQueue atomically retrieves and drains all queued offline envelopes for a user
func (c *Client) GetAndFlushOfflineQueue(ctx context.Context, userID string) ([]*models.Envelope, error) {
	key := fmt.Sprintf("offline:%s", userID)
	pipe := c.rdb.TxPipeline()
	lrangeCmd := pipe.LRange(ctx, key, 0, -1)
	pipe.Del(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	rawItems := lrangeCmd.Val()
	if len(rawItems) == 0 {
		return nil, nil
	}

	envelopes := make([]*models.Envelope, 0, len(rawItems))
	for _, item := range rawItems {
		var env models.Envelope
		if err := json.Unmarshal([]byte(item), &env); err == nil {
			envelopes = append(envelopes, &env)
		}
	}
	return envelopes, nil
}

// PublishEnvelope broadcasts an envelope over a Redis Pub/Sub channel
func (c *Client) PublishEnvelope(ctx context.Context, channel string, env *models.Envelope) error {
	bytes, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("failed to marshal envelope for redis pubsub: %w", err)
	}
	return c.rdb.Publish(ctx, channel, bytes).Err()
}

// Subscribe returns a Redis PubSub listener for a given channel
func (c *Client) Subscribe(ctx context.Context, channel string) *redis.PubSub {
	return c.rdb.Subscribe(ctx, channel)
}

// IsTokenBlacklisted checks if an access token JTI is in the Redis blacklist
func (c *Client) IsTokenBlacklisted(ctx context.Context, tokenID string) (bool, error) {
	if tokenID == "" {
		return false, nil
	}
	key := fmt.Sprintf("blacklist:token:%s", tokenID)
	val, err := c.rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return val > 0, nil
}

// IsUserBlacklisted checks if user sessions before issuedAt were revoked
func (c *Client) IsUserBlacklisted(ctx context.Context, userID string, issuedAt time.Time) (bool, error) {
	if userID == "" {
		return false, nil
	}
	key := fmt.Sprintf("blacklist:user:%s", userID)
	val, err := c.rdb.Get(ctx, key).Int64()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// If token was issued before or at the revocation timestamp, it is invalid
	return issuedAt.Unix() <= val, nil
}

// CreateWSTicket stores a short-lived single-use ticket for WebSocket handshake
func (c *Client) CreateWSTicket(ctx context.Context, ticket string, userData []byte, ttl time.Duration) error {
	key := fmt.Sprintf("ws_ticket:%s", ticket)
	return c.rdb.Set(ctx, key, userData, ttl).Err()
}

// ConsumeWSTicket atomically retrieves and deletes a WebSocket handshake ticket
func (c *Client) ConsumeWSTicket(ctx context.Context, ticket string) ([]byte, error) {
	key := fmt.Sprintf("ws_ticket:%s", ticket)
	pipe := c.rdb.TxPipeline()
	getCmd := pipe.Get(ctx, key)
	pipe.Del(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	data, err := getCmd.Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return data, err
}
