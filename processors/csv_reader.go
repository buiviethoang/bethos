package processors

import (
	"bethos/internal/model"
	"bufio"
	"context"
	"os"
	"strings"

	"github.com/warpstreamlabs/bento/public/service"
)

type CSVReader struct {
	FilePath string
	Batch    int
}

func (r *CSVReader) Close(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (r *CSVReader) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	f, _ := os.Open(r.FilePath)
	defer f.Close()

	sc := bufio.NewScanner(f)
	out := service.MessageBatch{}

	for sc.Scan() {
		cols := strings.Split(sc.Text(), ",")
		row := model.CSVRow{
			Vincode:      cols[1],
			ResourceName: cols[3],
			Value:        cols[4],
		}
		m := service.NewMessage(nil)
		m.SetStructured(row)
		out = append(out, m)

		if len(out) >= r.Batch {
			return out, nil
		}
	}
	return out, nil
}
