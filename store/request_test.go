package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddKey(t *testing.T) {
	assert := assert.New(t)
	r := &Request{}

	r.AddKey("test", "testing")

	s, ok := r.Key["test"]

	assert.True(ok, "Key not added")
	assert.Equal(s, "testing", "Key value not correct")

	r.AddKey("test2", 2)

	assert.Len(r.Key, 2, "Key not added")

	i, ok := r.Key["test2"]
	assert.True(ok, "Key not added")
	assert.Equal(i, 2, "Key value not correct")

	r.AddKey("test3", "testing").AddKey("test4", "1")

	assert.Len(r.Key, 4, "AddKey Chaining Not working")
}

func TestAdd(t *testing.T) {
	assert := assert.New(t)

	r := &Request{}

	r.AddItem("s1k", "s1v")
	r.AddItem("s2k", "s2v")
	s, ok := r.Item["s1k"]

	assert.True(ok)
	assert.Equal("s1v", s)

	s, ok = r.Item["s2k"]

	assert.True(ok)
	assert.Equal("s2v", s)

	r.AddItem("n1k", 1)
	i, ok := r.Item["n1k"]

	assert.True(ok)
	assert.Equal(1, i)

}
