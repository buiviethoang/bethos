package processors

import (
	"bufio"
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/warpstreamlabs/bento/public/service"
)

type CSVGenerator struct {
	FilePath string
	Count    int
}

func (c *CSVGenerator) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (c *CSVGenerator) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	f, _ := os.OpenFile(c.FilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()

	w := bufio.NewWriterSize(f, 32*1024*1024)
	defer w.Flush()

	now := time.Now().UnixMilli()
	ns := time.Now().UnixNano()

	for i := 0; i < c.Count; i++ {
		line := strings.Join([]string{
			"tel_001",
			"VF37ARFZE12345678",
			"content.34183.1.2",
			"vehicle_speed",
			"85.5",
			strconv.FormatInt(now, 10),
			strconv.FormatInt(now+1000, 10),
			"bk",
			strconv.FormatInt(ns, 10),
		}, ",")
		w.WriteString(line + "\n")
	}
	return service.MessageBatch{msg}, nil
}
