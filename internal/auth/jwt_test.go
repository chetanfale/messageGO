package auth

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestValidateAccessToken_InvalidTokens(t *testing.T) {
	secret := "test_secret_key"

	// 1. Empty token
	if _, err := ValidateAccessToken("", secret); err == nil {
		t.Error("expected error for empty token, got nil")
	}

	// 2. Ticket UUID passed as JWT
	ticketUUID := "c1a2b3c4-d5e0-7f8a-9b0c-1d2e3f4a5b6c"
	if _, err := ValidateAccessToken(ticketUUID, secret); err == nil {
		t.Error("expected error for UUID string passed as JWT, got nil")
	}

	// 3. Expired token
	expiredClaims := &JWTCustomClaims{
		UserID: "user123",
		Email:  "user@example.com",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-10 * time.Minute)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, expiredClaims)
	tokenStr, _ := token.SignedString([]byte(secret))
	if _, err := ValidateAccessToken(tokenStr, secret); err == nil {
		t.Error("expected error for expired token, got nil")
	}

	// 4. Token without user_id
	emptyUserClaims := &JWTCustomClaims{
		UserID: "",
		Email:  "user@example.com",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(10 * time.Minute)),
		},
	}
	token2 := jwt.NewWithClaims(jwt.SigningMethodHS256, emptyUserClaims)
	tokenStr2, _ := token2.SignedString([]byte(secret))
	if _, err := ValidateAccessToken(tokenStr2, secret); err == nil {
		t.Error("expected error for token with empty user_id, got nil")
	}

	// 5. Valid token
	validClaims := &JWTCustomClaims{
		UserID: "user_valid_123",
		Email:  "user@example.com",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(10 * time.Minute)),
		},
	}
	token3 := jwt.NewWithClaims(jwt.SigningMethodHS256, validClaims)
	tokenStr3, _ := token3.SignedString([]byte(secret))
	claims, err := ValidateAccessTokenWithRevocation(context.Background(), tokenStr3, secret, nil)
	if err != nil {
		t.Fatalf("unexpected error for valid token: %v", err)
	}
	if claims.UserID != "user_valid_123" {
		t.Fatalf("expected user_id 'user_valid_123', got '%s'", claims.UserID)
	}
}
