package config

type Option int32

type Store map[Option]interface{}

func (s Store) Get(option Option) (interface{}, bool) {
	k, ok := s[option]
	return k, ok
}

func (s Store) GetString(option Option) string {
	st, ok := s[option]
	if !ok {
		return ""
	}
	return st.(string)
}

func (s Store) GetNum(option Option) int {
	i, ok := s[option]
	if !ok {
		return -1
	}
	return i.(int)
}
