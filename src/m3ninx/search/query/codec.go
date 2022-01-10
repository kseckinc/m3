// Copyright (c) 2018 Uber Technologies, Inc.
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

package query

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/m3db/m3/src/m3ninx/generated/proto/querypb"
	"github.com/m3db/m3/src/m3ninx/search"
)

var errNilQuery = errors.New("query is nil")

// Marshal encodes a query into a byte slice.
func Marshal(q search.Query) ([]byte, error) {
	if q == nil {
		return nil, errNilQuery
	}
	return q.ToProto().Marshal()
}

// Unmarshal decodes a query from a byte slice.
func Unmarshal(data []byte) (search.Query, error) {
	var pb querypb.Query
	if err := pb.Unmarshal(data); err != nil {
		return nil, err
	}

	return UnmarshalProto(&pb)
}

// UnmarshalProto will unmarshal a proto query.
func UnmarshalProto(q *querypb.Query) (search.Query, error) {
	switch q := q.Query.(type) {

	case *querypb.Query_All:
		return NewAllQuery(), nil

	case *querypb.Query_Field:
		return NewFieldQuery(q.Field.Field), nil

	case *querypb.Query_Term:
		return NewTermQuery(q.Term.Field, q.Term.Term), nil

	case *querypb.Query_Regexp:
		return NewRegexpQuery(q.Regexp.Field, q.Regexp.Regexp)

	case *querypb.Query_Negation:
		inner, err := UnmarshalProto(q.Negation.Query)
		if err != nil {
			return nil, err
		}
		return NewNegationQuery(inner), nil

	case *querypb.Query_Conjunction:
		qs := make([]search.Query, 0, len(q.Conjunction.Queries))
		for _, qry := range q.Conjunction.Queries {
			if term := qry.GetTerm(); term != nil {
				if bytes.Equal(term.Field, []byte("__metadata__")) {
					continue
				}
			}
			sqry, err := UnmarshalProto(qry)
			if err != nil {
				return nil, err
			}
			qs = append(qs, sqry)
		}
		return NewConjunctionQuery(qs), nil

	case *querypb.Query_Disjunction:
		qs := make([]search.Query, 0, len(q.Disjunction.Queries))
		for _, qry := range q.Disjunction.Queries {
			sqry, err := UnmarshalProto(qry)
			if err != nil {
				return nil, err
			}
			qs = append(qs, sqry)
		}
		return NewDisjunctionQuery(qs), nil
	}

	return nil, fmt.Errorf("unknown query: %+v", q)
}

const fldMetadata = "__metadata__"

// UnmarshalMetadata decodes a query from a byte slice.
func UnmarshalMetadata(data []byte) (search.Query, []string, error) {
	var pb querypb.Query
	if err := pb.Unmarshal(data); err != nil {
		return nil, nil, err
	}

	var metadata []string
	query, md, err := UnmarshalProtoMetadata(&pb, metadata)
	return query, md, err
}

// UnmarshalProtoMetadata will unmarshal a proto query (will return metadata values if found).
func UnmarshalProtoMetadata(q *querypb.Query, metadata []string) (search.Query, []string, error) {
	switch q := q.Query.(type) {

	case *querypb.Query_All:
		return NewAllQuery(), metadata, nil

	case *querypb.Query_Field:
		return NewFieldQuery(q.Field.Field), metadata, nil

	case *querypb.Query_Term:
		if bytes.Equal([]byte(fldMetadata), q.Term.Field) {
			metadata = append(metadata, string(q.Term.Term))
			return NewAllQuery(), metadata, nil
		}
		return NewTermQuery(q.Term.Field, q.Term.Term), metadata, nil

	case *querypb.Query_Regexp:
		query, err := NewRegexpQuery(q.Regexp.Field, q.Regexp.Regexp)
		return query, metadata, err

	case *querypb.Query_Negation:
		inner, md, err := UnmarshalProtoMetadata(q.Negation.Query, metadata)
		if err != nil {
			return nil, nil, err
		}
		return NewNegationQuery(inner), md, nil

	case *querypb.Query_Conjunction:
		qs := make([]search.Query, 0, len(q.Conjunction.Queries))
		for _, qry := range q.Conjunction.Queries {
			if term := qry.GetTerm(); term != nil {
				if bytes.Equal([]byte(fldMetadata), term.Field) {
					metadata = append(metadata, string(term.Term))
					continue
				}
			}
			sqry, md, err := UnmarshalProtoMetadata(qry, metadata)
			if err != nil {
				return nil, nil, err
			}
			metadata = md
			qs = append(qs, sqry)
		}
		if len(qs) == 0 {
			return NewAllQuery(), metadata, nil
		}
		return NewConjunctionQuery(qs), metadata, nil

	case *querypb.Query_Disjunction:
		qs := make([]search.Query, 0, len(q.Disjunction.Queries))
		for _, qry := range q.Disjunction.Queries {
			if term := qry.GetTerm(); term != nil {
				if bytes.Equal([]byte(fldMetadata), term.Field) {
					metadata = append(metadata, string(term.Term))
					continue
				}
			}
			sqry, md, err := UnmarshalProtoMetadata(qry, metadata)
			if err != nil {
				return nil, nil, err
			}
			metadata = md
			qs = append(qs, sqry)
		}
		if len(qs) == 0 {
			return NewAllQuery(), metadata, nil
		}
		return NewDisjunctionQuery(qs), metadata, nil
	}

	return nil, metadata, fmt.Errorf("unknown query: %+v", q)
}
