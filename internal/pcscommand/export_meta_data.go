package pcscommand

import (
	"container/list"
	"fmt"
	"github.com/iikira/BaiduPCS-Go/baidupcs"
	"github.com/iikira/BaiduPCS-Go/baidupcs/pcserror"
	"github.com/iikira/BaiduPCS-Go/pcsutil/converter"
	"github.com/iikira/BaiduPCS-Go/pcsutil/pcstime"
	"os"
	"path"
	"strings"
	"time"
)

type (
	etaskEMD struct {
		*ListTask
		path     string
		rootPath string
		fd       *baidupcs.FileDirectory
		err      pcserror.Error
	}

	// ExportMDOptions 导出可选项
	ExportMDOptions struct {
		RootPath  string // 根路径
		SavePathData  string // 输出路径
		SavePathLog  string // 输出路径
		MaxRerty  int
		Recursive bool
	}
)
// global var for main func and err_handler func
var ExportSavePathData 	string
var ExportSavePathLog 	string
var fpExportData 	*os.File
var fpExportLog 	*os.File

func (task *etaskEMD) handleExportTaskErrorEMD(l *list.List, failedList *list.List) {
	if task.err == nil {
		return
	}

	var wrErrExpErrEMD error // local var err
	// 不重试
	switch task.err.GetError() {
	case baidupcs.ErrGetRapidUploadInfoMD5NotFound, baidupcs.ErrGetRapidUploadInfoCrc32NotFound:
		// stdout error
		fmt.Printf("[%d] - [%d] - [%d] - [%s] file meta 导出成功, slice/crc 导出失败, 可能是服务器未刷新文件的md5, 请过一段时间再试一试\n", task.ID, Bool2int(task.fd.Isdir), task.fd.FsID, task.path)
		// log error
		_, wrErrExpErrEMD = fpExportLog.Write(converter.ToBytes(fmt.Sprintf("[%d] - [%d] - [%d] - [%s] file meta 导出成功, slice/crc 导出失败, 可能是服务器未刷新文件的md5, 请过一段时间再试一试\n", task.ID, Bool2int(task.fd.Isdir), task.fd.FsID, task.path)))
			if wrErrExpErrEMD != nil {
				fmt.Printf("写入文件失败: %s\n", wrErrExpErrEMD)
				return // 直接返回
			} 
		failedList.PushBack(task)
		return
	case baidupcs.ErrFileTooLarge:
		// stdout err
		fmt.Printf("[%d] - [%d] - [%d] - [%s] file meta 导出成功, slice/crc 导出失败, 文件大于20GB, 无法导出\n", task.ID,  Bool2int(task.fd.Isdir), task.fd.FsID, task.path)
		// log err
		_, wrErrExpErrEMD = fpExportLog.Write(converter.ToBytes(fmt.Sprintf("[%d] - [%d] - [%d] - [%s] file meta 导出成功, slice/crc 导出失败, 文件大于20GB, 无法导出\n", task.ID,  Bool2int(task.fd.Isdir), task.fd.FsID, task.path)))
			if wrErrExpErrEMD != nil {
				fmt.Printf("写入文件失败: %s\n", wrErrExpErrEMD)
				return // 直接返回
			}
		failedList.PushBack(task)
		return
	}

	// 未达到失败重试最大次数, 将任务推送到队列末尾
	if task.retry < task.MaxRetry { // if --retry > 0 (not default)
		task.retry++
		// stdout err
		fmt.Printf("[%d] - [%d] - [%d] - [%s] file meta 导出错误, %s, 重试 %d/%d\n", task.ID, Bool2int(task.fd.Isdir), task.fd.FsID, task.path, task.err, task.retry, task.MaxRetry)
		// log err
		_, wrErrExpErrEMD = fpExportLog.Write(converter.ToBytes(fmt.Sprintf("[%d] - [%d] - [%d] - [%s] file meta 导出错误, %s, 重试 %d/%d\n", task.ID, Bool2int(task.fd.Isdir), task.fd.FsID, task.path, task.err, task.retry, task.MaxRetry)))
			if wrErrExpErrEMD != nil {
				fmt.Printf("写入文件失败: %s\n", wrErrExpErrEMD)
				return // 直接返回
			}
		l.PushBack(task)
		time.Sleep(3 * time.Duration(task.retry) * time.Second)
	} else { // skip retry (default)
		// stdout err
		fmt.Printf("[%d] - [%d] - [%d] - [%s] file meta 导出成功, slice/crc 导出错误: %s\n", task.ID, Bool2int(task.fd.Isdir), task.fd.FsID, task.path, task.err)
		// log err
		_, wrErrExpErrEMD = fpExportLog.Write(converter.ToBytes(fmt.Sprintf("[%d] - [%d] - [%d] - [%s] file meta 导出成功, slice/crc 导出错误: %s\n", task.ID, Bool2int(task.fd.Isdir), task.fd.FsID, task.path, task.err)))
			if wrErrExpErrEMD != nil {
				fmt.Printf("写入文件失败: %s\n", wrErrExpErrEMD)
				return // 直接返回
			}
		failedList.PushBack(task)
	}
}
/* re-use export.go
func changeRootPath(dstRootPath, dstPath, srcRootPath string) string {
	if srcRootPath == "" {
		return dstPath
	}
	return path.Join(srcRootPath, strings.TrimPrefix(dstPath, dstRootPath))
}
*/
// SetExportFilenameData 获取导出路径
func SetExportFilenameData(opt *ExportMDOptions) {
	if opt.SavePathData != "" {
		ExportSavePathData = opt.SavePathData
	} else {
		ExportSavePathData = "BaiduPCS-Go_export_" + pcstime.BeijingTimeOption("") + "_data.txt"
	}
	var err error
	fpExportData, err = os.OpenFile(ExportSavePathData, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil { // 不可写
		fmt.Printf("%s\n", err)
		return
	}
	fmt.Printf("导出的信息Data将保存在: %s\n", ExportSavePathData)
}
// SetExportFilenameLog 获取导出路径
func SetExportFilenameLog(opt *ExportMDOptions) {
	if opt.SavePathLog != "" {
		ExportSavePathLog = opt.SavePathLog
	} else {
		ExportSavePathLog = "BaiduPCS-Go_export_" + pcstime.BeijingTimeOption("") + "_log.txt"
	}
	var err error
	fpExportLog, err = os.OpenFile(ExportSavePathLog, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil { // 不可写
		fmt.Printf("%s\n", err)
		return
	}
	fmt.Printf("导出过程Log将保存在: %s\n", ExportSavePathLog)
}

// RunExport 执行导出文件和目录
func RunExportMetaData(pcspaths []string, opt *ExportMDOptions) {
	if opt == nil {
		opt = &ExportMDOptions{}
	}
	SetExportFilenameData(opt)
	SetExportFilenameLog(opt)

	pcspaths, err := matchPathByShellPattern(pcspaths...)
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		au         = GetActiveUser()
		pcs        = GetBaiduPCS()
		l          = list.New()
		failedList = list.New()
		writeErr   error
		id         int
	)

	for id = range pcspaths {
		var rootPath string
		if pcspaths[id] == au.Workdir {
			rootPath = pcspaths[id]
		} else {
			rootPath = path.Dir(pcspaths[id])
		}
		// 加入队列
		l.PushBack(&etaskEMD{
			ListTask: &ListTask{
				ID:       id,
				MaxRetry: opt.MaxRerty,
			},
			path:     pcspaths[id],
			rootPath: rootPath,
		})
	}

	for {
		e := l.Front()
		if e == nil { // 结束
			break
		}

		l.Remove(e) // 载入任务后, 移除队列

		task := e.Value.(*etaskEMD)
		root := task.fd == nil

		// 获取文件信息
		if task.fd == nil { // 第一次初始化
			fd, pcsError := pcs.FilesDirectoriesMeta(task.path)
			if pcsError != nil {
				task.err = pcsError
				task.handleExportTaskErrorEMD(l, failedList)
				continue
			}
			task.fd = fd
		}

		if task.fd.Isdir { // 导出目录
			if !root && !opt.Recursive { 
			// 非递归 if (-r not set) and (root not set), skip this dir
				continue
			}

			// get all file+dir item into fds list in current dir
			fds, pcsError := pcs.FilesDirectoriesList(task.path, baidupcs.DefaultOrderOptions)
			if pcsError != nil {
				task.err = pcsError
				task.handleExportTaskErrorEMD(l, failedList)
				continue
			}

			if len(fds) == 0 { // if dir empty, get dir meta
				// write data
				_, writeErr = fpExportData.Write(converter.ToBytes(fmt.Sprintf("%d,%d,%d,%s,%d,\"%s\",\"%s\",%d,%d,1,,,,,\n", Bool2int(task.fd.Isdir), Bool2int(task.fd.Ifhassubdir), task.fd.FsID, task.fd.MD5, task.fd.Size, task.fd.Filename, task.fd.Path, task.fd.Ctime, task.fd.Mtime)))
					if writeErr != nil {
						fmt.Printf("写入文件失败: %s\n", writeErr)
						return // 直接返回
					}
				// write log
				_, writeErr = fpExportLog.Write(converter.ToBytes(fmt.Sprintf("[%d] - [%d] - [%d] - [%s] dir meta 导出成功\n", task.ID, Bool2int(task.fd.Isdir), task.fd.FsID, task.path)))
					if writeErr != nil {
						fmt.Printf("写入文件失败: %s\n", writeErr)
						return // 直接返回
					}
				// stdout log
				fmt.Printf("[%d] - [%d] - [%d] - [%s] dir meta 导出成功\n", task.ID, Bool2int(task.fd.Isdir), task.fd.FsID, task.path)
				// if dir empty, skip next and back to loop to check next file or dir
				continue
			} else {    // if dir not empty, get dir meta
				// write data
				_, writeErr = fpExportData.Write(converter.ToBytes(fmt.Sprintf("%d,%d,%d,%s,%d,\"%s\",\"%s\",%d,%d,1,,,,,\n", Bool2int(task.fd.Isdir), Bool2int(task.fd.Ifhassubdir), task.fd.FsID, task.fd.MD5, task.fd.Size, task.fd.Filename, task.fd.Path, task.fd.Ctime, task.fd.Mtime)))
					if writeErr != nil {
						fmt.Printf("写入文件失败: %s\n", writeErr)
						return // 直接返回
					}
				// write log
				_, writeErr = fpExportLog.Write(converter.ToBytes(fmt.Sprintf("[%d] - [%d] - [%d] - [%s] dir meta 导出成功\n", task.ID, Bool2int(task.fd.Isdir), task.fd.FsID, task.path)))
					if writeErr != nil {
						fmt.Printf("写入文件失败: %s\n", writeErr)
						return // 直接返回
					}
				// stdout log
				fmt.Printf("[%d] - [%d] - [%d] - [%s] dir meta 导出成功\n", task.ID, Bool2int(task.fd.Isdir), task.fd.FsID, task.path)
			} // then go next to add item in queue

			// add all items into queue in current dir
			for _, fd := range fds {
				// 加入队列
				id++
				l.PushBack(&etaskEMD{
					ListTask: &ListTask{
						ID:       id,
						MaxRetry: opt.MaxRerty,
					},
					path:     fd.Path,
					fd:       fd,
					rootPath: task.rootPath,
				})
			}
			continue // scan this dir layer done, back to loop and handle next fd
		}
		// if not dir, this is one file, get file meta info
		rinfo, pcsError := pcs.ExportByFileInfo(task.fd)
		if pcsError != nil { // cannot get rapid upload info, assume can get meta
			task.err = pcsError
			task.handleExportTaskErrorEMD(l, failedList)
			// write data
			_, writeErr = fpExportData.Write(converter.ToBytes(fmt.Sprintf("%d,%d,%d,%s,%d,\"%s\",\"%s\",%d,%d,,,,,\n", Bool2int(task.fd.Isdir), Bool2int(task.fd.Ifhassubdir), task.fd.FsID, task.fd.MD5, task.fd.Size, task.fd.Filename, task.fd.Path, task.fd.Ctime, task.fd.Mtime)))
					if writeErr != nil {
						fmt.Printf("写入文件失败: %s\n", writeErr)
						return // 直接返回 
					}
			// write log
			fmt.Printf("[%d] - [%d] - [%d] - [%s] meta 导出成功, slice/crc 导出失败\n", task.ID, Bool2int(task.fd.Isdir), task.fd.FsID, task.path)
					if writeErr != nil {
						fmt.Printf("写入文件失败: %s\n", writeErr)
						return // 直接返回 
					}
			continue // skip next (fail report) and handle next fd
		} else { // get rapid upload info + meta
			// write data
			_, writeErr = fpExportData.Write(converter.ToBytes(fmt.Sprintf("%d,%d,%d,%s,%d,\"%s\",\"%s\",%d,%d,\"%s\",%d,%s,%s,%s\n", Bool2int(task.fd.Isdir), Bool2int(task.fd.Ifhassubdir), task.fd.FsID, task.fd.MD5, task.fd.Size, task.fd.Filename, task.fd.Path, task.fd.Ctime, task.fd.Mtime, rinfo.Filename,  rinfo.ContentLength, rinfo.ContentMD5, rinfo.SliceMD5, rinfo.ContentCrc32)))
					if writeErr != nil {
						fmt.Printf("写入文件失败: %s\n", writeErr)
						return // 直接返回
					}
			// write log
			_, writeErr = fpExportLog.Write(converter.ToBytes(fmt.Sprintf("[%d] - [%d] - [%d] - [%s] file meta + slice/crc 导出成功\n", task.ID, Bool2int(task.fd.Isdir), task.fd.FsID, task.path)))
					if writeErr != nil {
						fmt.Printf("写入文件失败: %s\n", writeErr)
						return // 直接返回
					}
			// write stdout log
			fmt.Printf("[%d] - [%d] - [%d] - [%s] file meta + slice/crc 导出成功\n", task.ID, Bool2int(task.fd.Isdir), task.fd.FsID, task.path)
			continue // skip next fail report
		}
	} // if queue nil empty, go to next fail report
	// the failure report
	if failedList.Len() > 0 {
		fmt.Printf("\n以下目录/文件导出失败: \n")
		fmt.Printf("%s\n", strings.Repeat("-", 100))
		// write summary header to list
		_, writeErr = fpExportLog.Write(converter.ToBytes(fmt.Sprintf("\n以下目录/文件导出失败: \n%s\n", strings.Repeat("-", 100))))
				if writeErr != nil {
					fmt.Printf("写入文件失败: %s\n", writeErr)
					return // 直接返回
				}
		var CntFailFileDir int64 = 1
		for e := failedList.Front(); e != nil; e = e.Next() {
			et := e.Value.(*etaskEMD)
			// stdout fail list
			fmt.Printf("[%d] - [%d] - [%d] - [%d] - [%s]\n", CntFailFileDir, et.ID, Bool2int(et.fd.Isdir), et.fd.FsID, et.path)
			// write fail list to log
			_, writeErr = fpExportLog.Write(converter.ToBytes(fmt.Sprintf("[%d] - [%d] - [%d] - [%d] - [%s]\n", CntFailFileDir, et.ID, Bool2int(et.fd.Isdir), et.fd.FsID, et.path)))
			CntFailFileDir++
		}
	}
	defer fpExportData.Close() // close file
	defer fpExportLog.Close()
}
