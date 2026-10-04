package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"messageGO/internal/storage/redis"
)

// ErrTokenRevoked is returned when token or user has been revoked
var ErrTokenRevoked = errors.New("token has been revoked")

// JWTCustomClaims matches the token structure emitted by auth_service
type JWTCustomClaims struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	Role   string `json:"role"`
	jwt.RegisteredClaims
}

// ValidateAccessToken parses and validates a JWT token string against the shared secret.
func ValidateAccessToken(tokenString, secret string) (*JWTCustomClaims, error) {
	if tokenString == "" {
		return nil, errors.New("empty token provided")
	}

	token, err := jwt.ParseWithClaims(tokenString, &JWTCustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		// Ensure signing method is HMAC (HS256)
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(secret), nil
	})

	if err != nil {
		return nil, fmt.Errorf("invalid or expired token: %w", err)
	}

	claims, ok := token.Claims.(*JWTCustomClaims)
	if !ok || !token.Valid || claims.UserID == "" {
		return nil, errors.New("invalid token claims: missing user identification")
	}

	return claims, nil
}

// ValidateAccessTokenWithRevocation parses and validates the token and checks Redis for revocation.
func ValidateAccessTokenWithRevocation(ctx context.Context, tokenString, secret string, rdb *redis.Client) (*JWTCustomClaims, error) {
	claims, err := ValidateAccessToken(tokenString, secret)
	if err != nil {
		return nil, err
	}

	if rdb != nil {
		// 1. Check if token JTI is blacklisted
		if claims.ID != "" {
			blacklisted, err := rdb.IsTokenBlacklisted(ctx, claims.ID)
			if err == nil && blacklisted {
				return nil, ErrTokenRevoked
			}
		}

		// 2. Check if user sessions before token issued_at were revoked
		issuedAt := time.Time{}
		if claims.IssuedAt != nil {
			issuedAt = claims.IssuedAt.Time
		}
		userRevoked, err := rdb.IsUserBlacklisted(ctx, claims.UserID, issuedAt)
		if err == nil && userRevoked {
			return nil, ErrTokenRevoked
		}
	}

	return claims, nil
}
