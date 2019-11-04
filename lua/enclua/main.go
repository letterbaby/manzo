package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/xxtea/xxtea-go/xxtea"
)

func main() {
	indir := os.Args[1]
	outdir := os.Args[2]

	files := make(map[string]string, 0)
	filepath.Walk(indir, func(fp string, fi os.FileInfo, err error) error {
		if fi.IsDir() { // 忽略目录
			return nil
		}
		if strings.HasSuffix(strings.ToUpper(fi.Name()), "LUA") {
			files[fi.Name()] = fp
		}
		return nil
	})

	for k, v := range files {
		rs, er := ioutil.ReadFile(v)
		if er != nil {
			fmt.Printf("encode -----f:%v,e:%v", v, er)
			continue
		}

		outbytes := xxtea.Encrypt(rs, []byte("xx"))
		ioutil.WriteFile(outdir+k, outbytes, os.ModePerm)

		fmt.Printf("encode -----f:%v\n", k)
	}
}
