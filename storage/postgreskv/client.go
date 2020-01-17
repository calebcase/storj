// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package postgreskv

import (
	"context"
	"database/sql"

	"github.com/lib/pq"
	"github.com/zeebo/errs"
	"gopkg.in/spacemonkeygo/monkit.v2"

	"storj.io/storj/private/dbutil"
	"storj.io/storj/private/dbutil/pgutil"
	"storj.io/storj/storage"
	"storj.io/storj/storage/postgreskv/schema"
)

const (
	defaultBatchSize          = 128
	defaultRecursiveBatchSize = 10000
)

var (
	mon = monkit.Package()
)

// Client is the entrypoint into a postgreskv data store
type Client struct {
	db *sql.DB
}

var _ storage.KeyValueStore = (*Client)(nil)

// New instantiates a new postgreskv client given db URL
func New(dbURL string) (*Client, error) {
	dbURL = pgutil.CheckApplicationName(dbURL)

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, err
	}

	dbutil.Configure(db, mon)
	//TODO: Fix the parameters!!
	err = schema.PrepareDB(context.TODO(), db, dbURL)
	if err != nil {
		return nil, err
	}

	return NewWith(db), nil
}

// NewWith instantiates a new postgreskv client given db.
func NewWith(db *sql.DB) *Client {
	return &Client{db: db}
}

// Close closes the client
func (client *Client) Close() error {
	return client.db.Close()
}

// Has returns true if the key exists.
func (client *Client) Has(ctx context.Context, key storage.Key) (_ bool, err error) {
	defer mon.Task()(&ctx)(&err)

	if key.IsZero() {
		return false, storage.ErrEmptyKey.New("")
	}

	q := "SELECT 1 FROM pathdata WHERE fullpath = $1::BYTEA"
	result, err := client.db.ExecContext(ctx, q, []byte(key))
	if err != nil {
		return false, Error.Wrap(err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return false, Error.Wrap(err)
	}

	if affected == 0 {
		return false, nil
	}

	return true, nil
}

// Create the provided key if and only it doesn't exist and set the value.
func (client *Client) Create(ctx context.Context, key storage.Key, value storage.Value) (err error) {
	defer mon.Task()(&ctx)(&err)

	if key.IsZero() {
		return storage.ErrEmptyKey.New("")
	}

	q := `INSERT INTO pathdata (fullpath, metadata)
		VALUES ($1::BYTEA, $2::BYTEA)
		ON CONFLICT DO NOTHING
		RETURNING 1`
	result, err := client.db.ExecContext(ctx, q, []byte(key), []byte(value))
	if err != nil {
		return Error.Wrap(err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return Error.Wrap(err)
	}

	if affected == 0 {
		return storage.ErrKeyExists.New("%q", key)
	}

	return nil
}

// Put sets the value for the provided key.
func (client *Client) Put(ctx context.Context, key storage.Key, value storage.Value) (err error) {
	defer mon.Task()(&ctx)(&err)

	if key.IsZero() {
		return storage.ErrEmptyKey.New("")
	}

	q := `INSERT INTO pathdata (fullpath, metadata)
		VALUES ($1::BYTEA, $2::BYTEA)
		ON CONFLICT (fullpath) DO
		  UPDATE SET metadata = EXCLUDED.metadata`
	_, err = client.db.Exec(q, []byte(key), []byte(value))

	return Error.Wrap(err)
}

// Get looks up the provided key and returns its value (or an error).
func (client *Client) Get(ctx context.Context, key storage.Key) (_ storage.Value, err error) {
	defer mon.Task()(&ctx)(&err)

	if key.IsZero() {
		return nil, storage.ErrEmptyKey.New("")
	}

	q := "SELECT metadata FROM pathdata WHERE fullpath = $1::BYTEA"
	row := client.db.QueryRow(q, []byte(key))

	var val []byte
	err = row.Scan(&val)
	if err == sql.ErrNoRows {
		return nil, storage.ErrKeyNotFound.New("%q", key)
	}

	return val, Error.Wrap(err)
}

// GetAll finds all values for the provided keys (up to storage.LookupLimit).
// If more keys are provided than the maximum, an error will be returned.
func (client *Client) GetAll(ctx context.Context, keys storage.Keys) (_ storage.Values, err error) {
	defer mon.Task()(&ctx)(&err)

	if len(keys) > storage.LookupLimit {
		return nil, storage.ErrLimitExceeded
	}

	q := `SELECT metadata
		FROM pathdata pd
		  RIGHT JOIN
		    unnest($1::BYTEA[]) WITH ORDINALITY pk(request, ord)
		  ON (pd.fullpath = pk.request)
		ORDER BY pk.ord`
	rows, err := client.db.Query(q, pq.ByteaArray(keys.ByteSlices()))
	if err != nil {
		return nil, errs.Wrap(err)
	}
	defer func() { err = errs.Combine(err, Error.Wrap(rows.Close())) }()

	values := make([]storage.Value, 0, len(keys))
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			return nil, Error.Wrap(err)
		}

		values = append(values, storage.Value(value))
	}

	return values, Error.Wrap(rows.Err())
}

// Delete deletes the given key and its associated value.
func (client *Client) Delete(ctx context.Context, key storage.Key) (err error) {
	defer mon.Task()(&ctx)(&err)

	if key.IsZero() {
		return storage.ErrEmptyKey.New("")
	}

	q := "DELETE FROM pathdata WHERE fullpath = $1::BYTEA"
	result, err := client.db.Exec(q, []byte(key))
	if err != nil {
		return Error.Wrap(err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return Error.Wrap(err)
	}

	if affected == 0 {
		return storage.ErrKeyNotFound.New("%q", key)
	}

	return nil
}

// List returns either a list of known keys, in order, or an error.
func (client *Client) List(ctx context.Context, first storage.Key, limit int) (_ storage.Keys, err error) {
	defer mon.Task()(&ctx)(&err)

	return storage.ListKeys(ctx, client, first, limit)
}

// Iterate calls the callback with an iterator over the keys.
func (client *Client) Iterate(ctx context.Context, opts storage.IterateOptions, fn func(context.Context, storage.Iterator) error) (err error) {
	defer mon.Task()(&ctx)(&err)

	batchSize := defaultBatchSize

	if opts.Recurse {
		batchSize = defaultRecursiveBatchSize
	}

	opi, err := newOrderedPostgresIterator(ctx, client, opts, batchSize)
	if err != nil {
		return err
	}
	defer func() { err = errs.Combine(err, opi.Close()) }()

	return fn(ctx, opi)
}

// CompareAndSwap atomically compares and swaps oldValue with newValue.
func (client *Client) CompareAndSwap(ctx context.Context, key storage.Key, oldValue, newValue storage.Value) (err error) {
	defer mon.Task()(&ctx)(&err)

	if key.IsZero() {
		return storage.ErrEmptyKey.New("")
	}

	// Semantically equivalent to an non-existence check.
	if oldValue == nil && newValue == nil {
		var exists bool

		exists, err = client.Has(ctx, key)
		if err != nil {
			return Error.Wrap(err)
		}

		if !exists {
			return nil
		}

		// FIXME: Well that's unexpected. When the key exists when it
		// shouldn't the error is 'value changed'???
		//return storage.ErrKeyNotFound.New("%q", key)
		return storage.ErrValueChanged.New("%q", key)
	}

	// Semantically equivalent to a create.
	if oldValue == nil {
		err = client.Create(ctx, key, newValue)

		if storage.ErrKeyExists.Has(err) {
			return storage.ErrValueChanged.New("%q", key)
		}

		if err != nil {
			return Error.Wrap(err)
		}

		return nil
	}

	// Semantically similar to a delete, but guarded on the value being
	// equal to oldValue. This is subtely different from a normal delete
	// using the key.
	if newValue == nil {
		q := `DELETE FROM pathdata
			WHERE fullpath = $1::BYTEA
			  AND metadata = $2::BYTEA`
		result, err := client.db.ExecContext(ctx, q, []byte(key), []byte(oldValue))
		if err != nil {
			return Error.Wrap(err)
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return Error.Wrap(err)
		}

		if affected == 0 {
			// Attempt to decide if it was because the key didn't exist or
			// the value changed.
			//
			// TODO: Remove this once all the backends have been moved to
			// using the stricter sense of CAS where neither value can be
			// nil.
			var exists bool

			exists, err = client.Has(ctx, key)
			if err != nil {
				return Error.Wrap(err)
			}

			if !exists {
				return storage.ErrKeyNotFound.New("%q", key)
			}

			return storage.ErrValueChanged.New("%q", key)
		}

		return nil
	}

	// Compare the value with the oldValue and swap it for the newValue if
	// and only if the key exists.
	q := `UPDATE pathdata
		SET metadata = $3::BYTEA
		WHERE fullpath = $1::BYTEA
		  AND metadata = $2::BYTEA`
	result, err := client.db.ExecContext(ctx, q, []byte(key), []byte(oldValue), []byte(newValue))
	if err != nil {
		return Error.Wrap(err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return Error.Wrap(err)
	}

	if affected == 0 {
		// Attempt to decide if it was because the key didn't exist or
		// the value changed.
		//
		// TODO: Remove this once all the backends have been moved to
		// using the stricter sense of CAS where neither value can be
		// nil.
		var exists bool

		exists, err = client.Has(ctx, key)
		if err != nil {
			return Error.Wrap(err)
		}

		if !exists {
			return storage.ErrKeyNotFound.New("%q", key)
		}

		return storage.ErrValueChanged.New("%q", key)
	}

	return nil
}
