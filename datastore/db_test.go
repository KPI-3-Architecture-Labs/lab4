package datastore

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

type Data struct {
	key   string
	value string
}

func TestPutAndGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, 333)

	if err != nil {
		t.Fatal(err)
	}

	defer db.out.Close()

	data := []Data{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	outPath := filepath.Join(dir, outFileName+"0")
	outFile, err := os.Open(outPath)

	if err != nil {
		t.Fatal(err)
	}

	t.Run("put&get check", func(t *testing.T) {
		for i := 0; i < len(data); i++ {

			key := data[i].key
			value := data[i].value

			err := db.Put(key, value)
			if err != nil {
				t.Errorf("Cannot put %s: %s", key, err)
			}

			result, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}

			if result != value {
				t.Errorf("Bad value returned expected %s, got %s", value, result)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("test size", func(t *testing.T) {
		for i := 0; i < len(data); i++ {
			key := data[i].key

			value := data[i].value

			err := db.Put(key, value)

			if err != nil {
				t.Errorf("Cannot put %s: %s", key, err)
			}

		}

		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}

		if size1*2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d != %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.out.Close(); err != nil {
			t.Fatal(err)
		}

		db, err = NewDb(dir, 100)
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < len(data); i++ {
			key := data[i].key
			value := data[i].value

			res, err := db.Get(key)
			if err != nil {
				t.Errorf("Unable to get %s: %s", key, err)
			}

			if res != value {
				t.Errorf("Expected value: %s, but received: %s", value, res)
			}
		}
	})
}
