/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package puller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	exportTokenHeader = "x-kubevirt-export-token"
	mapPageSize       = 4096
	maxErrorBody      = 512
)

// DataSource reads raw disk content. It is the part of the export endpoint
// the transfer needs, split out so tests can substitute it.
type DataSource interface {
	// Data returns length bytes of the disk starting at offset. The stream
	// carries no length, so the caller must count what it receives.
	Data(ctx context.Context, offset, length int64) (io.ReadCloser, error)
}

// ExportClient talks to the export endpoints of one volume of a pull-mode backup.
type ExportClient struct {
	httpClient *http.Client
	token      string
	dataURL    string
	mapURL     string
}

// NewExportClient builds a client for one volume. caPEM is the certificate
// published in the backup's status, token the content of the token secret.
func NewExportClient(caPEM []byte, token, dataURL, mapURL string) (*ExportClient, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("the export certificate is not valid PEM")
	}
	for _, u := range []string{dataURL, mapURL} {
		parsed, err := url.Parse(u)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
			return nil, fmt.Errorf("invalid export endpoint %q", u)
		}
	}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		// One TCP connection per reader: HTTP/2 would multiplex all readers
		// over a single connection.
		TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 2 * time.Minute,
		DisableCompression:    true,
	}
	return &ExportClient{
		httpClient: &http.Client{Transport: transport},
		token:      token,
		dataURL:    dataURL,
		mapURL:     mapURL,
	}, nil
}

func (c *ExportClient) get(ctx context.Context, base string, query url.Values) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"?"+query.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(exportTokenHeader, c.token)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("export endpoint returned HTTP %d: %s", resp.StatusCode, string(body))
	}
	return resp, nil
}

// Data implements DataSource.
func (c *ExportClient) Data(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	resp, err := c.get(ctx, c.dataURL, url.Values{
		"offset": {strconv.FormatInt(offset, 10)},
		"length": {strconv.FormatInt(length, 10)},
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

type mapPage struct {
	Extents    []Extent `json:"extents"`
	NextOffset *int64   `json:"next_offset"`
}

// Map reads the whole block map of the volume, following the paging.
func (c *ExportClient) Map(ctx context.Context) ([]Extent, error) {
	return readMap(ctx, func(ctx context.Context, offset int64) ([]byte, error) {
		resp, err := c.get(ctx, c.mapURL, url.Values{
			"offset":    {strconv.FormatInt(offset, 10)},
			"length":    {"0"},
			"page_size": {strconv.Itoa(mapPageSize)},
		})
		if err != nil {
			return nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		return io.ReadAll(resp.Body)
	})
}

// readMap collects the pages of a block map. fetch returns the raw page
// that starts at the given offset.
func readMap(ctx context.Context, fetch func(ctx context.Context, offset int64) ([]byte, error)) ([]Extent, error) {
	var extents []Extent
	var offset int64
	for {
		raw, err := fetch(ctx, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to read block map at offset %d: %w", offset, err)
		}
		page := mapPage{}
		if err := json.Unmarshal(raw, &page); err != nil {
			return nil, fmt.Errorf("failed to parse block map page at offset %d: %w", offset, err)
		}
		if len(page.Extents) == 0 {
			return nil, fmt.Errorf("block map page at offset %d is empty", offset)
		}
		extents = append(extents, page.Extents...)
		if page.NextOffset == nil {
			return extents, nil
		}
		if *page.NextOffset <= offset {
			return nil, fmt.Errorf("block map paging does not advance: next offset %d after %d", *page.NextOffset, offset)
		}
		offset = *page.NextOffset
	}
}
