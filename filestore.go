package filestore

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/rest"
	"github.com/rs/rest-layer/schema"
	"golang.org/x/net/context"
)

type FileStoreHandler struct {
	sync.RWMutex
	// If latency is set, the handler will introduce an artificial latency on
	// all operations
	Latency       time.Duration
	items         map[interface{}][]byte
	ids           []interface{}
	directory     string
	collection    string
	database_file string
	UniqueFields  []string
}

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register(time.Time{})
}

// NewHandler creates an empty memory handler
func NewHandler(directory string, collection string, uniqueFields []string) *FileStoreHandler {
	os.MkdirAll(directory, 0664)
	f := &FileStoreHandler{
		items:         map[interface{}][]byte{},
		ids:           []interface{}{},
		directory:     directory,
		collection:    collection,
		database_file: directory + "/" + collection,
		UniqueFields:  uniqueFields,
	}
	f.readDatafile()
	return f
}

// NewSlowHandler creates an empty memory handler with specified latency
func NewSlowHandler(latency time.Duration) *FileStoreHandler {
	return &FileStoreHandler{
		Latency: latency,
		items:   map[interface{}][]byte{},
		ids:     []interface{}{},
	}
}

func (self *FileStoreHandler) readDatafile() {
	if _, err := os.Stat(self.database_file); os.IsNotExist(err) {
		log.Println("Database " + self.database_file + " doesn't exist for collection " + self.collection)
		return
	}

	data, err := ioutil.ReadFile(self.database_file)

	if err != nil {
		log.Println("Error reading database file " + self.database_file)
		panic(err)
	}

	dec := gob.NewDecoder(bytes.NewBuffer(data))

	var items map[interface{}][]byte
	if err := dec.Decode(&items); err != nil {
		log.Println("Error reading database file " + self.database_file)
		panic(err)
	}

	for k := range self.items {
		delete(self.items, k)
	}

	self.ids = nil

	for k, v := range items {
		self.items[k] = v
		self.ids = append(self.ids, k)
	}
	log.Println("Read database " + self.database_file)
}

func (self *FileStoreHandler) saveDatafile() {

	encoded_items, err := self.serialize(&self.items)

	if err != nil {
		panic(err)
	}

	err = ioutil.WriteFile(self.database_file, encoded_items, 0644)

	if err != nil {
		panic(err)
	}

	log.Println("Saved database " + self.database_file)

}

func (self *FileStoreHandler) persistData() {
	self.saveDatafile()
	self.readDatafile()
}

// store serialize the item using gob and store it in the handler's items map
func (self *FileStoreHandler) store(item *resource.Item) error {
	encoded_item, err := self.serialize(&item)

	if err != nil {
		return err
	}
	self.items[item.ID] = encoded_item

	self.persistData()

	return nil
}

func (self *FileStoreHandler) serialize(item interface{}) ([]byte, error) {
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	if err := enc.Encode(item); err != nil {
		return nil, err
	}
	return data.Bytes(), nil
}

// fetch unserialize item's data and return a new item
func (self *FileStoreHandler) fetch(id interface{}) (*resource.Item, bool, error) {
	data, found := self.items[id]
	if !found {
		return nil, false, nil
	}
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	var item resource.Item
	if err := dec.Decode(&item); err != nil {
		return nil, true, err
	}
	return &item, true, nil
}

// delete removes an item by this id with no look
func (self *FileStoreHandler) delete(id interface{}) {
	delete(self.items, id)
	// Remove id from id list
	for i, _id := range self.ids {
		if _id == id {
			if i >= len(self.ids)-1 {
				self.ids = self.ids[:i]
			} else {
				self.ids = append(self.ids[:i], self.ids[i+1:]...)
			}
			break
		}
	}
	self.persistData()
}

// Insert inserts new items in memory
func (self *FileStoreHandler) Insert(ctx context.Context, items []*resource.Item) (err error) {
	self.Lock()
	defer self.Unlock()
	err = handleWithLatency(self.Latency, ctx, func() error {

		for _, item := range items {
			if _, found := self.items[item.ID]; found {
				return resource.ErrConflict
			}

			for _, uniqueField := range self.UniqueFields {
				lookup := resource.NewLookup()
				queries := schema.Query{}
				queries = append(queries, schema.Equal{Field: uniqueField, Value: item.Payload[uniqueField]})
				lookup.AddQuery(queries)
				res, err := self.findNoLock(ctx, lookup, 1, -1)
				if err != nil {
					return err
				}

				if len(res.Items) > 0 {
					return &rest.Error{422, "Unique precondition failed on field '" + uniqueField + "'", nil}
				}
			}

		}
		for _, item := range items {
			// Store ids in ordered slice for sorting
			self.ids = append(self.ids, item.ID)

			if err := self.store(item); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Update replace an item by a new one in memory
func (self *FileStoreHandler) Update(ctx context.Context, item *resource.Item, original *resource.Item) (err error) {
	self.Lock()
	defer self.Unlock()
	err = handleWithLatency(self.Latency, ctx, func() error {
		o, found, err := self.fetch(original.ID)
		if !found {
			return resource.ErrNotFound
		}
		if err != nil {
			return err
		}
		if original.ETag != o.ETag {
			return resource.ErrConflict
		}
		if err := self.store(item); err != nil {
			return err
		}
		return nil
	})
	return err
}

// Delete deletes an item from memory
func (self *FileStoreHandler) Delete(ctx context.Context, item *resource.Item) (err error) {
	self.Lock()
	defer self.Unlock()
	err = handleWithLatency(self.Latency, ctx, func() error {
		o, found, err := self.fetch(item.ID)
		if !found {
			return resource.ErrNotFound
		}
		if err != nil {
			return err
		}
		if item.ETag != o.ETag {
			return resource.ErrConflict
		}
		self.delete(item.ID)
		return nil
	})
	return err
}

// Clear clears all items from the memory store matching the lookup
func (self *FileStoreHandler) Clear(ctx context.Context, lookup *resource.Lookup) (total int, err error) {
	self.Lock()
	defer self.Unlock()
	err = handleWithLatency(self.Latency, ctx, func() error {
		ids := make([]interface{}, len(self.ids))
		copy(ids, self.ids)
		for _, id := range ids {
			item, _, err := self.fetch(id)
			if err != nil {
				return err
			}
			if !lookup.Filter().Match(item.Payload) {
				continue
			}
			self.delete(item.ID)
			total++
		}
		return nil
	})
	self.persistData()
	return total, err
}

// Find items from memory matching the provided lookup
func (self *FileStoreHandler) Find(ctx context.Context, lookup *resource.Lookup, page, perPage int) (list *resource.ItemList, err error) {
	self.RLock()
	defer self.RUnlock()
	return self.findNoLock(ctx, lookup, page, perPage)
}

func (self *FileStoreHandler) findNoLock(ctx context.Context, lookup *resource.Lookup, page, perPage int) (list *resource.ItemList, err error) {
	err = handleWithLatency(self.Latency, ctx, func() error {
		items := []*resource.Item{}
		// Apply filter
		for _, id := range self.ids {
			item, _, err := self.fetch(id)
			if err != nil {
				return err
			}
			if !lookup.Filter().Match(item.Payload) {
				continue
			}
			items = append(items, item)
		}
		// Apply sort
		if len(lookup.Sort()) > 0 {
			s := sortableItems{lookup.Sort(), items}
			sort.Sort(s)
		}
		// Apply pagination
		total := len(items)
		start := (page - 1) * perPage
		end := total
		if perPage > 0 {
			end = start + perPage
			if start > total-1 {
				start = 0
				end = 0
			} else if end > total-1 {
				end = total
			}
		}
		list = &resource.ItemList{total, page, items[start:end]}
		return nil
	})
	return list, err
}
