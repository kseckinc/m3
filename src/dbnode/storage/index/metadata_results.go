// Copyright (c) 2021 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package index

import (
	"sync"

	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

type metadataResults struct {
	sync.RWMutex

	nsID   ident.ID
	hashFn sharding.HashFn

	results map[xtime.UnixNano]QueryMetadataBlockResults
}

// NewQueryMetadataResults returns a new query metadata results object.
func NewQueryMetadataResults(namespaceID ident.ID, hashFn sharding.HashFn) QueryMetadataResults {
	return &metadataResults{
		nsID:    namespaceID,
		hashFn:  hashFn,
		results: make(map[xtime.UnixNano]QueryMetadataBlockResults),
	}
}

func (r *metadataResults) Namespace() ident.ID {
	r.RLock()
	v := r.nsID
	r.RUnlock()
	return v
}

func (r *metadataResults) HashFn() sharding.HashFn {
	return r.hashFn
}

func (r *metadataResults) AddQueryMetadataResult(
	blockStart xtime.UnixNano,
	result QueryMetadataAggregateResult,
) {
	r.Lock()
	defer r.Unlock()

	entry, ok := r.results[blockStart]
	if !ok {
		entry = QueryMetadataBlockResults{
			BlockStart: blockStart,
		}
	}

	entry.Results = append(entry.Results, result)
	r.results[blockStart] = entry
}

func (r *metadataResults) Map() map[xtime.UnixNano]QueryMetadataBlockResults {
	r.RLock()
	defer r.RUnlock()
	return r.results
}

func (r *metadataResults) Finalize() {
	// nop
}
