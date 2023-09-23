package indexer

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/zeebo/assert"
	"testing"

	ipld "github.com/ipld/go-ipld-prime"
	datamodel "github.com/ipld/go-ipld-prime/datamodel"
	basicnode "github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/printer"
)

func TestInit(t *testing.T) {
	db, err := NewMemoryDatabase()

	assert.NoError(t, err)
	if db == nil {
		t.Fail()
	}

	reader := strings.NewReader(`{"name":"Alice"}
{"name":"Bob"}
{"name":"Albert"}
{"name":"Clearance and Steve"}`)

	// collection of users indexed by  name
	collection, err := db.Collection("users", "name")

	assert.NoError(t, err)

	ctx := context.Background()
	err = collection.IndexNDJSON(ctx, reader)
	assert.NoError(t, err)

	records, err := collection.Iterate(ctx)
	assert.NoError(t, err)

	for record := range records {
		fmt.Println(record.Id, printer.Sprint(record.Data))
	}

	err = db.ExportToFile(ctx, "../fixtures/init.car")

	assert.NoError(t, err)

	query := Query{
		Equal: map[string]ipld.Node{
			"name": basicnode.NewString("Bob"),
		},
	}

	results, err := collection.Search(ctx, query)

	assert.NoError(t, err)

	record := <-results

	name, err := record.Data.LookupByString("name")

	assert.NoError(t, err)

	assert.True(t, datamodel.DeepEqual(name, basicnode.NewString("Bob")))

	proof, err := collection.GetProof(record.Id)

	assert.NoError(t, err)

	fmt.Println(proof)

	query = Query{
		Limit: 2,
	}

	count := 0
	results, err = collection.Search(ctx, query)

	for _ = range results {
		count++
	}

	assert.Equal(t, count, query.Limit)
}

func TestSampleData(t *testing.T) {
	db, err := NewMemoryDatabase()
	assert.NoError(t, err)
	if db == nil {
		t.Fail()
	}

	dmi, err := db.GetDBMetaInfo()
	assert.NoError(t, err)
	assert.Equal(t, dmi.Format, "database")
	assert.Equal(t, dmi.Version, CURRENT_DB_VERSION)

	ctx := context.Background()

	reader, err := os.Open("../fixtures/sample.ndjson")
	assert.NoError(t, err)

	// collection of logs, indexed by their ID field
	collection, err := db.Collection("logs", "id")

	assert.NoError(t, err)

	err = db.StartMutating(ctx)
	assert.NoError(t, err)

	_, err = collection.CreateIndex(ctx, "created")
	assert.NoError(t, err)

	//_, err = collection.CreateIndex(ctx, "model", "created")
	//assert.NoError(t, err)

	index := &Index{
		collection: collection,
		fields:     []string{"created"},
	}
	assert.True(t, index.Exists())

	err = db.Flush(ctx)
	assert.NoError(t, err)

	err = collection.IndexNDJSON(ctx, reader)

	assert.NoError(t, err)

	err = db.ExportToFile(ctx, "../fixtures/sample.car")

	assert.NoError(t, err)

	query := Query{
		Equal: map[string]ipld.Node{
			"created": basicnode.NewInt(1688405691),
		},
	}

	expectedId := "chatcmpl-1056144062448104093141073783165392307"

	results, err := collection.Search(ctx, query)

	assert.NoError(t, err)

	record, ok := <-results

	assert.True(t, ok)

	fmt.Println(record.Id, printer.Sprint(record.Data))

	id, err := record.Data.LookupByString("id")

	assert.NoError(t, err)

	assert.True(t, datamodel.DeepEqual(id, basicnode.NewString(expectedId)))

	proof, err := collection.GetProof(record.Id)

	assert.NoError(t, err)

	fmt.Println(proof)

	loaded, err := ImportFromFile("../fixtures/sample.car")

	assert.NoError(t, err)

	loadedCollection, err := loaded.Collection("logs", "id")

	loadedNode, err := loadedCollection.Get(ctx, record.Id)

	assert.NoError(t, err)
	fmt.Println(loadedNode)

	treeCid, err := loaded.tree.TreeCid()
	assert.NoError(t, err)
	fmt.Println("Tree", treeCid)

	proof, err = loadedCollection.GetProof(record.Id)

	assert.NoError(t, err)

	fmt.Println(proof)
}

func TestMergeDB(t *testing.T) {
	db, err := NewMemoryDatabase()
	assert.NoError(t, err)

	reader := strings.NewReader(`{"name":"Alice"}
									{"name":"Bob"}
									{"name":"Albert"}
									{"name":"Clearance and Steve"}`)

	// collection of users indexed by name
	collection, err := db.Collection("users", "name")
	assert.NoError(t, err)

	ctx := context.Background()
	err = collection.IndexNDJSON(ctx, reader)
	assert.NoError(t, err)

	records, err := collection.Iterate(ctx)
	assert.NoError(t, err)

	for record := range records {
		fmt.Println(record.Id, printer.Sprint(record.Data))
	}

	fmt.Println("#####")

	dbTwo, err := NewMemoryDatabase()
	assert.NoError(t, err)

	reader = strings.NewReader(`{"name":"William"}
									{"name":"Tom"}
									{"name":"Smith"}`)
	collection, err = dbTwo.Collection("users", "name")
	assert.NoError(t, err)

	err = collection.IndexNDJSON(ctx, reader)
	assert.NoError(t, err)

	records, err = collection.Iterate(ctx)
	assert.NoError(t, err)

	for record := range records {
		fmt.Println(record.Id, printer.Sprint(record.Data))
	}

	newDB, err := Merge(ctx, dbTwo, db)
	assert.NoError(t, err)

	//firstKey, _ := newDB.tree.FirstKey()
	//lastKey, _ := newDB.tree.LastKey()
	//iter, err := newDB.tree.Search(ctx, firstKey, lastKey)
	//assert.NoError(t, err)
	//for !iter.Done() {
	//	k, v, err := iter.NextPair()
	//	assert.NoError(t, err)
	//	vLink, err := v.AsLink()
	//	assert.NoError(t, err)
	//	t.Logf("%s\n", k)
	//	t.Logf("%v : %s", k, vLink.String())
	//}

	collection, err = newDB.Collection("users", "name")
	assert.NoError(t, err)

	records, err = collection.Iterate(ctx)
	assert.NoError(t, err)

	for record := range records {
		fmt.Println(record.Id, printer.Sprint(record.Data))
	}

	querys := []struct {
		name string
		q    Query
	}{
		{
			"Tom",
			Query{
				Equal: map[string]ipld.Node{
					"name": basicnode.NewString("Tom"),
				},
			},
		},
		{
			"William",
			Query{
				Equal: map[string]ipld.Node{
					"name": basicnode.NewString("William"),
				},
			},
		},
		{
			"Smith",
			Query{
				Equal: map[string]ipld.Node{
					"name": basicnode.NewString("Smith"),
				},
			},
		},
		{
			"Alice",
			Query{
				Equal: map[string]ipld.Node{
					"name": basicnode.NewString("Alice"),
				},
			},
		},
		{
			"Bob",
			Query{
				Equal: map[string]ipld.Node{
					"name": basicnode.NewString("Bob"),
				},
			},
		},
		{
			"Albert",
			Query{
				Equal: map[string]ipld.Node{
					"name": basicnode.NewString("Albert"),
				},
			},
		},
		{
			"Clearance and Steve",
			Query{
				Equal: map[string]ipld.Node{
					"name": basicnode.NewString("Clearance and Steve"),
				},
			},
		},
	}

	for _, query := range querys {
		results, err := collection.Search(ctx, query.q)
		assert.NoError(t, err)

		record := <-results
		name, err := record.Data.LookupByString("name")
		assert.NoError(t, err)
		assert.True(t, datamodel.DeepEqual(name, basicnode.NewString(query.name)))
	}

}
