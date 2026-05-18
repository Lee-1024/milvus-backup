package backup

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func Open(ctx context.Context, cfg ClientConfig) (*milvusclient.Client, error) {
	client, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address:       cfg.Address,
		Username:      cfg.Username,
		Password:      cfg.Password,
		APIKey:        cfg.APIKey,
		DBName:        cfg.DBName,
		EnableTLSAuth: cfg.TLS,
	})
	if err != nil {
		return nil, err
	}
	if cfg.DBName != "" {
		if err := client.UseDatabase(ctx, milvusclient.NewUseDatabaseOption(cfg.DBName)); err != nil {
			_ = client.Close(ctx)
			return nil, fmt.Errorf("use database %s: %w", cfg.DBName, err)
		}
	}
	fmt.Printf("connected to Milvus: address=%s database=%s tls=%v\n", cfg.Address, displayDatabase(cfg.DBName), cfg.TLS)
	return client, nil
}
