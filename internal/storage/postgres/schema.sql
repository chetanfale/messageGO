-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- 1. Conversations Table
CREATE TABLE IF NOT EXISTS conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type VARCHAR(16) NOT NULL DEFAULT 'direct', -- 'direct' or 'group'
    title VARCHAR(128),
    last_seq_id BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Conversation Participants & Sync Cursors
CREATE TABLE IF NOT EXISTS conversation_participants (
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    user_id VARCHAR(64) NOT NULL,
    last_read_seq_id BIGINT NOT NULL DEFAULT 0,
    last_delivered_seq_id BIGINT NOT NULL DEFAULT 0,
    cleared_seq_id BIGINT NOT NULL DEFAULT 0,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (conversation_id, user_id)
);

-- 3. Messages Table (Append-only log with monotonic seq_id per conversation)
CREATE TABLE IF NOT EXISTS messages (
    id BIGSERIAL PRIMARY KEY,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    seq_id BIGINT NOT NULL,
    sender_id VARCHAR(64) NOT NULL,
    client_msg_id VARCHAR(64) NOT NULL,
    content_type VARCHAR(32) NOT NULL DEFAULT 'text', -- 'text', 'image', 'file', 'system'
    content TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_conversation_seq UNIQUE (conversation_id, seq_id),
    CONSTRAINT uq_conversation_client_msg UNIQUE (conversation_id, client_msg_id)
);

-- Indexes for ultra-fast delta synchronization & participant lookups
CREATE INDEX IF NOT EXISTS idx_messages_conv_seq ON messages (conversation_id, seq_id ASC);
CREATE INDEX IF NOT EXISTS idx_participants_user ON conversation_participants (user_id);
CREATE INDEX IF NOT EXISTS idx_conversations_updated ON conversations (updated_at DESC);
