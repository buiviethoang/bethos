package processors

import (
	"bethos/internal/model"
	"bufio"
	"context"
	"os"
	"strconv"
	"strings"

	"github.com/warpstreamlabs/bento/public/service"
)

const defaultMaxLines = 2_000_000

type CSVReader struct {
	FilePath string
	Batch    int // I/O chunk size (lines per read loop); also used as max per batch if we ever split
	MaxLines int // cap total lines read per file (0 = defaultMaxLines)
}

func (r *CSVReader) Close(ctx context.Context) error {
	return nil
}

func (r *CSVReader) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	f, err := os.Open(r.FilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	maxLines := r.MaxLines
	if maxLines <= 0 {
		maxLines = defaultMaxLines
	}

	sc := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 1024*1024)

	out := service.MessageBatch{}
	linesRead := 0

	for sc.Scan() && linesRead < maxLines {
		line := sc.Text()
		cols := strings.Split(line, ",")
		if len(cols) < 9 {
			continue
		}
		row := parseCSVRow(cols)
		m := service.NewMessage(nil)
		m.SetStructured(row)
		out = append(out, m)
		linesRead++
	}

	if err := sc.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func parseCSVRow(cols []string) model.CSVRow {
	capturedTS, _ := strconv.ParseInt(cols[5], 10, 64)
	ts, _ := strconv.ParseInt(cols[6], 10, 64)
	nsTS, _ := strconv.ParseInt(cols[8], 10, 64)
	return model.CSVRow{
		ID:           cols[0],
		Vincode:      cols[1],
		ResourceID:   cols[2],
		ResourceName: cols[3],
		Value:        cols[4],
		CapturedTS:   capturedTS,
		TS:           ts,
		Source:       cols[7],
		NsTS:         nsTS,
	}
}
