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
	"os"
	"syscall"
)

const (
	fallocKeepSize  = 0x01
	fallocPunchHole = 0x02
)

// punchHole deallocates a range so it reads as zeros. On a block device the
// kernel turns this into a zero-out without a fallback write.
func punchHole(f *os.File, offset, length int64) error {
	return syscall.Fallocate(int(f.Fd()), fallocPunchHole|fallocKeepSize, offset, length)
}
