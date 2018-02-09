package store

type Storer interface {
	Run(request Request) (Result, error)
	StartTransaction()
	FinishTransaction()
	Rollback() []error
	ClearCache()
	CacheOff()
	CacheOn()
}
