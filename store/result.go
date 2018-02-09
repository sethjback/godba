package store

// Result represents the output of a DB operation
type Result interface {
	// GetStringItem returns a field as a string, second argument indicates if the item was found
	GetStringItem(int, string) (string, bool)

	// GetNumberItem returns a field as an int, second argument indicates if the item was found
	GetNumberItem(int, string) (int, bool)

	// GetStringSetItem returns a string set from the db, second argument indicates if the item was found
	GetStringListItem(int, string) ([]string, bool)

	// GetBoolItem returns a bool field, second arguemt indicates if the item was found
	GetBoolItem(int, string) (bool, bool)

	// Getitem returns a generic item type of nil if not found, second argument indicates if the item was found
	GetItem(int, string) (interface{}, bool)

	// UnmarshalItem attemps to translate item into the interface, second argument indicates if the item was found
	UnmarshalItem(int, string, interface{}) (error, bool)

	// GetItemCount returns the item count
	GetItemCount() int

	// GetLastEvaluatedKey returns the last evaluated key from a request
	GetLastEvaluatedKey() map[string]interface{}

	// PageCount returns the total number of pages for a query pages result
	PageCount() int
}
