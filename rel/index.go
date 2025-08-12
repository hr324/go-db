package rel

type KeyFunc func(row []byte) [][]byte

type Index struct {
	Table string
	Name  string
	fn    KeyFunc
}
