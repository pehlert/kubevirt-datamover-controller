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
	"encoding/json"
	"strings"
)

// ParseProgress finds the most recent progress line in a pod log and returns
// its counters. ok is false if the log holds no progress line.
func ParseProgress(logs string) (done, total int64, ok bool) {
	lines := strings.Split(logs, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if !strings.Contains(line, ProgressMessage) || !strings.HasPrefix(line, "{") {
			continue
		}
		var entry struct {
			Msg        string `json:"msg"`
			BytesDone  *int64 `json:"bytesDone"`
			TotalBytes *int64 `json:"totalBytes"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if entry.Msg != ProgressMessage || entry.BytesDone == nil || entry.TotalBytes == nil {
			continue
		}
		if *entry.BytesDone < 0 || *entry.TotalBytes < *entry.BytesDone {
			continue
		}
		return *entry.BytesDone, *entry.TotalBytes, true
	}
	return 0, 0, false
}
