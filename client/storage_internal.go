package client

import (
	"context"
	"strings"
)

type storageClient struct {
	Client
}

func newStorageClient(addrs string) (*storageClient, error) {
	return newStorageClientWithContext(context.Background(), addrs)
}

func newStorageClientWithContext(ctx context.Context, addrs string) (c *storageClient, err error) {
	c = &storageClient{}

	err = c.Init(ctx, addrs)

	return
}

func (c *storageClient) Close() {
}

func (c *storageClient) Addrs() string {
	return strings.Join(c.addrs, ",")
}
