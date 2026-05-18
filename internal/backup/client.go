package backup

import (
	"context"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func Open(ctx context.Context, cfg ClientConfig) (*milvusclient.Client, error) {
	return milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address:       cfg.Address,
		Username:      cfg.Username,
		Password:      cfg.Password,
		APIKey:        cfg.APIKey,
		DBName:        cfg.DBName,
		EnableTLSAuth: cfg.TLS,
	})
}
