package quickbolt

type Size interface {
	Megabytes() int
}

type sizeStore struct {
	mb int
}

func newSizeStore(mb int) sizeStore {
	return sizeStore{
		mb: mb,
	}
}

func (s sizeStore) Megabytes() int {
	return s.mb
}
