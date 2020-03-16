package pcscommand

import (
	"fmt"
)
// convert bool to int
func Bool2int(b bool) int {
	if b {
		return 1
	}
	return 0
}
func Bool2int64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// RunGetMeta 执行 获取文件/目录的元信息
func RunGetMetaData(targetPaths ...string) {
	targetPaths, err := matchPathByShellPattern(targetPaths...)
	if err != nil {
		fmt.Println(err)
		return
	}

		var pcs = GetBaiduPCS()
		data, err := pcs.FilesDirectoriesMeta(targetPaths[0])
		if err != nil {
			fmt.Println(err)
			return
		} else { // no error
						//   dir,sd,id,m5,sz,"fdn","path",ct,mtime
			fmt.Printf("%d,%d,%d,%s,%d,\"%s\",\"%s\",%d,%d", Bool2int(data.Isdir), Bool2int(data.Ifhassubdir), data.FsID, data.MD5, data.Size, data.Filename, data.Path, data.Ctime, data.Mtime)
			if data.Isdir { // if dir
				fmt.Println() // enough, don't print sliceMD5 and CRC32
			} else { 				// if file, print extra meta md5/crc32
				rinfo, pcsError := pcs.ExportByFileInfo(data)
					if pcsError != nil { // if error, cannot get extra meta
						fmt.Printf(",\"")
						fmt.Print(pcsError)
						fmt.Printf("\"\n")
						return
					} else { // no error, print extra meta, add suffix \n
						fmt.Printf(",%s,%d,%s,%s,%s\n", rinfo.Filename,  rinfo.ContentLength, rinfo.ContentMD5, rinfo.SliceMD5, rinfo.ContentCrc32)
					}
			}
		}
}
