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

package downloader

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/migtools/kubevirt-datamover-controller/pkg/uploader"
)

func TestChainFormat(t *testing.T) {
	index := uploader.VMIndex{Checkpoints: []uploader.CheckpointEntry{
		{ID: "q1"}, {ID: "q2"},
		{ID: "b1", Format: uploader.CheckpointFormatBlocks}, {ID: "b2", Format: uploader.CheckpointFormatBlocks},
		{ID: "x", Format: "tape"},
	}}

	format, err := chainFormat([]string{"q1", "q2"}, index)
	require.NoError(t, err)
	assert.Equal(t, uploader.CheckpointFormatQcow2, format, "entries without a format are qcow2 backups made before the field existed")

	format, err = chainFormat([]string{"b1", "b2"}, index)
	require.NoError(t, err)
	assert.Equal(t, uploader.CheckpointFormatBlocks, format)

	_, err = chainFormat([]string{"q1", "b2"}, index)
	require.ErrorContains(t, err, "another format")
	_, err = chainFormat([]string{"b1", "q2"}, index)
	require.ErrorContains(t, err, "another format")
	_, err = chainFormat([]string{"b1", "gone"}, index)
	require.ErrorContains(t, err, "not found")
	_, err = chainFormat([]string{"x"}, index)
	require.ErrorContains(t, err, "unknown storage format")
}
