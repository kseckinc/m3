// +build integration

// Copyright (c) 2020 Uber Technologies, Inc.
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

package integration

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestQueryCardinality(t *testing.T) {
	testOpts, ns := newTestOptionsWithIndexedNamespace(t)
	testSetup, err := NewTestSetup(t, testOpts, nil, nil)
	require.NoError(t, err)
	defer testSetup.Close()

	require.NoError(t, testSetup.StartServer())
	defer func() {
		require.NoError(t, testSetup.StopServer())
	}()

	var (
		nowFn = testSetup.StorageOpts().ClockOptions().NowFn()
		end   = xtime.ToUnixNano(nowFn().Truncate(time.Hour))
		start = end.Add(-time.Hour)
		query = index.Query{
			Query: idx.NewConjunctionQuery(
				idx.NewTermQuery([]byte("tag"), []byte("value")),
				idx.NewTermQuery([]byte("__metadata__"), []byte("cardinality")),
			),
		}
		queryOpts = index.QueryOptions{StartInclusive: start, EndExclusive: end}
	)

	session, err := testSetup.M3DBClient().DefaultSession()
	require.NoError(t, err)
	expectedCardinality := 100
	for i := 0; i < expectedCardinality; i++ {
		var (
			metricName  = fmt.Sprintf("metric_%v", i)
			tag         = ident.StringTag("tag", "value")
			tagChanging = ident.StringTag("ix", strconv.Itoa(i))
			timestamp   = xtime.ToUnixNano(nowFn()).Add(-time.Second * time.Duration(i+1))
		)
		err := session.WriteTagged(
			ns.ID(), ident.StringID(metricName),
			ident.NewTagsIterator(ident.NewTags(tag, tagChanging)), timestamp, 0.0, xtime.Second, nil,
		)
		require.NoError(t, err)
	}

	seriesIters, _, err := session.FetchTagged(
		ContextWithDefaultTimeout(),
		ns.ID(), query, queryOpts,
	)
	require.NoError(t, err)
	defer seriesIters.Close()

	actualCardinality := 0
	actualTimeSeries := 0
	for _, iterator := range seriesIters.Iters() {
		require.NoError(t, iterator.Err())
		for iterator.Next() {
			dp, _, _ := iterator.Current()
			actualCardinality += int(dp.Value)
			actualTimeSeries++
			t.Logf("{%s} (%s=%g) \n", iterator.ID(), dp.TimestampNanos, dp.Value)
		}
	}

	require.Equal(t, expectedCardinality, actualCardinality)
	require.Equal(t, testOpts.NumShards(), actualTimeSeries)
}
