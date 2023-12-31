package minibitcask

import (
	"fmt"
	"os"
	"testing"
	"github.com/stretchr/testify/require"
	"time"
)

// TestDB_Delete tests the Delete function of the DB.
func TestDB_Delete(t *testing.T) {
	dir := "./test-delete"
	defer func() {
		os.RemoveAll(dir)
	}()
	// Open a DB with the given options and a directory for the test.
	db, err := Open(DefaultOptions, WithDir(dir), WithSyncEnable(false), WithMaxActiveFileSize(1024 * 1))
	require.NoError(t, err)
	// Set the number of keys to be stored.
	n := 10000
	// Store the keys and values in the DB.
	for i  := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		err = db.Put(key, value)
		require.NoError(t, err)
	}

	// Delete half of the keys.
	for i  := 0; i < n / 2; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		err := db.Delete(key)
		require.NoError(t, err)
	}

	// Check that the deleted keys are no longer present.
	for i := 0; i < n / 2; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		_, err := db.Get(key)
		require.Equal(t, ErrKeyNotFound, err)
	}

	// Close the DB.
	err = db.Close()
	require.NoError(t, err)

	// Re-open the DB.
	db, err = Open(DefaultOptions, WithDir(dir), WithSyncEnable(false), WithMaxActiveFileSize(1024 * 1))
	require.NoError(t, err)

	// Check that the deleted keys are no longer present.
	for i := 0; i < n / 2; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		_, err := db.Get(key)
		require.Equal(t, ErrKeyNotFound, err)
	}

	// Check that the remaining keys are present.
	for i  := n / 2; i < n; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		dbValue, err := db.Get(key)
		require.NoError(t, err)
		require.Equal(t, value, dbValue)
	}

	// Close the DB.
	err = db.Close()
	require.NoError(t, err)
}

// TestDB_Get tests the Get function of the DB
func TestDB_Get(t *testing.T) {
	dir := "./test-get"
	defer func() {
		os.RemoveAll(dir)
	}()

	// Open a DB with the given options
	db, err := Open(DefaultOptions, WithDir(dir), WithSyncEnable(false), WithMaxActiveFileSize(1024 * 1024 * 1))
	require.NoError(t, err)

	// Create a test key and value
	n := 1000
	for i  := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		// Put the key and value into the DB
		err = db.Put(key, value)
		require.NoError(t, err)
	}

	// Retrieve the values from the DB
	for i  := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		// Retrieve the value from the DB
		dbValue, err := db.Get(key)
		require.NoError(t, err)
		// Compare the retrieved value to the expected value
		require.Equal(t, value, dbValue)
	}

	// Close the DB
	err = db.Close()
	require.NoError(t, err)

	// reopen db
	db, err = Open(DefaultOptions, WithDir(dir), WithSyncEnable(false), WithMaxActiveFileSize(1024 * 1024 * 1))
	require.NoError(t, err)

	// Retrieve the values from the DB
	for i  := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		// Retrieve the value from the DB
		dbValue, err := db.Get(key)
		require.NoError(t, err)
		// Compare the retrieved value to the expected value
		require.Equal(t, value, dbValue)
	}

	// Close the DB
	err = db.Close()
	require.NoError(t, err)
}

func TestDB_Merge(t *testing.T) {
	dir := "./test-merge"
	defer func() {
		os.RemoveAll(dir)
	}()

	// Open a DB with the given options
	db, err := Open(DefaultOptions, WithDir(dir), WithSyncEnable(false), WithMaxActiveFileSize( 1024 * 1), WithMergeInteval(time.Second * 6000))
	require.NoError(t, err)

	// Create a test key and value
	n := 10000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		// Put the key and value into the DB
		err = db.Put(key, value)
		require.NoError(t, err)
	}

	// Retrieve the values from the DB
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		// Retrieve the value from the DB
		dbValue, err := db.Get(key)
		require.NoError(t, err)
		// Compare the retrieved value to the expected value
		require.Equal(t, value, dbValue)
	}

	for i := 0; i < n - 100; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		// Retrieve the value from the DB
		err := db.Delete(key)
		require.NoError(t, err)
	}

	for i := 0; i < n - 100; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		// Retrieve the value from the DB
		_, err := db.Get(key)
		require.Equal(t, err, ErrKeyNotFound)
	}

	require.NoError(t, db.Merge())

	// Close the DB
	err = db.Close()
	require.NoError(t, err)

	db, err = Open(DefaultOptions, WithDir(dir), WithSyncEnable(false), WithMaxActiveFileSize( 1024 * 1), WithMergeInteval(time.Second * 6000))
	require.NoError(t, err)

	for i := 0; i < n - 100; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		// Retrieve the value from the DB
		_, err := db.Get(key)
		require.Equal(t, err, ErrKeyNotFound)
	}

	for i := n - 100; i < n; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		// Retrieve the value from the DB
		dbValue, err := db.Get(key)
		require.NoError(t, err)
		// Compare the retrieved value to the expected value
		require.Equal(t, value, dbValue)
	}

	// Close the DB
	err = db.Close()
	require.NoError(t, err)
}

// TestOpen is a function to test the Open function
func TestOpen(t *testing.T) {
	dir := "./test-open"
	defer func() {
		os.RemoveAll(dir)
	}()
	// Create a new database with the given options
	db, err := Open(DefaultOptions, WithDir(dir), WithSyncEnable(false), WithMaxActiveFileSize(1024 * 1))
	// Check if there is an error when opening the database
	require.NoError(t, err)

	// Create a variable n to store the number of entries
	n := 1000
	// Loop through the number of entries
	for i  := 0; i < n; i++ {
		// Create a key and value to store the entry
		key := []byte(fmt.Sprintf("test%d", i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		// Put the entry into the database
		err = db.Put(key, value)
		// Check if there is an error when putting the entry
		require.NoError(t, err)
	}
	// Close the database
	err = db.Close()
	// Check if there is an error when closing the database
	require.NoError(t, err)

	// Open the database again with different options
	db, err = Open(&Options{}, WithDir(dir), WithSyncEnable(false), WithMaxActiveFileSize(1024 * 1024 * 1))
	// Check if there is an error when opening the database
	require.NoError(t, err)
	// Loop through the number of entries
	for i  := 0; i < n; i++ {
		// Create a key and value to store the entry
		key := []byte(fmt.Sprintf("test%d", i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		// Get the entry from the database
		dbValue, err := db.Get(key)
		// Check if there is an error when getting the entry
		require.NoError(t, err)
		// Check if the entry is equal to the value stored in the database
		require.Equal(t, value, dbValue)
	}
}
