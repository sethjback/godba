package godba

import (
	"errors"

	"github.com/sethjback/godba/config"
	"github.com/sethjback/godba/store"
)

type Store int32

const (
	Dynamodb Store = iota
)

func New(kind Store, config config.Store) (store.Storer, error) {
	switch kind {
	case Dynamodb:
		return store.NewDynamodb(config), nil
	}
	return nil, errors.New("unknown store")
}
