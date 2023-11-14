package issues

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/zhangfane/clickhouse-go/v2"
	"testing"
)

func Test592(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{})
	assert.NoError(t, err)

	ctx := context.Background()
	err = conn.Exec(ctx, "DROP TABLE test_connection")
	assert.Error(t, err)
}
