// Copyright 2017-2018 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package file implements "file" surfacer. This surfacer type is in
// experimental phase right now.
package file

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/cloudprober/logger"
	"github.com/google/cloudprober/metrics"

	configpb "github.com/google/cloudprober/surfacers/datadog/proto"
)

type DataDogSurfacer struct {
	// Configuration
	c *configpb.SurfacerConf

	// Channel for incoming data.
	inChan chan *metrics.EventMetrics

	// Output file for serializing to
	outf *os.File

	// Cloud logger
	l *logger.Logger

	// Each output message has a unique id. This field keeps the record of
	// that.
	id int64
}

func New(config *configpb.SurfacerConf, l *logger.Logger) (*DataDogSurfacer, error) {
	ctx := context.TODO()

	s := &DataDogSurfacer{
		c: config,
		l: l,
	}

	id := time.Now().UnixNano()

	return s, s.init(ctx, id)
}

func (s *DataDogSurfacer) processInput(ctx context.Context) {
	for {
		select {
		case em := <-s.inChan:
			emStr := fmt.Sprintf("%s %d %s", s.c.GetPrefix(), s.id, em.String())
			s.id++

			if !s.c.GetCompressionEnabled() {
				if _, err := fmt.Fprintln(s.outf, emStr); err != nil {
					s.l.Errorf("Unable to write data to %s. Err: %v", s.c.GetFilePath(), err)
				}
				continue
			}
			s.compressionBuffer.writeLine(emStr)

		case <-ctx.Done():
			return
		}
	}
}

func (s *DataDogSurfacer) init(ctx context.Context, id int64) error {
	s.inChan = make(chan *metrics.EventMetrics, 1000)
	s.id = id

	if s.c.GetFilePath() == "" {
		s.outf = os.Stdout
	} else {
		outf, err := os.Create(s.c.GetFilePath())
		if err != nil {
			return fmt.Errorf("failed to create file for writing: %v", err)
		}
		s.outf = outf
	}

	go s.processInput(ctx)

	return nil
}

func (s *DataDogSurfacer) Write(ctx context.Context, em *metrics.EventMetrics) {
	select {
	case s.inChan <- em:
	default:
		s.l.Errorf("DataDogSurfacer's write channel is full, dropping new data.")
	}
}
