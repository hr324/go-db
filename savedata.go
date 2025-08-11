package savedata

import (
	"fmt"
	"math/rand/v2"
	"os"
)

func SaveData1(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)

	if err != nil {
		return err
	}

	defer fp.Close()

	_, err = fp.Write(data)

	if err != nil {
		return err
	}

	return fp.Sync()
}

func SaveData2(path string, data []byte) error {
	temp := fmt.Sprintf("%s.temp.%d", path, rand.IntN(999999))

	fp, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}

	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(temp)
		}
	}()

	if _, err = fp.Write(data); err != nil {
		return err
	}

	if err = fp.Sync(); err != nil {
		return err
	}

	err = os.Rename(temp, path)
	return err
}
