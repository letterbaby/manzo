// +build windows

package shm

import (
	"syscall"
	"unsafe"
)

//Not suport window!
func AttachShm(ppShmRoot *unsafe.Pointer, key int, size int) (int, bool) {
	name := uint16(key)

	h, err := syscall.CreateFileMapping(
		syscall.InvalidHandle, nil,
		syscall.PAGE_READWRITE, 0, uint32(size), &name)
	if 0 == h {
		return -1, false
	}

	v, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_WRITE, 0, 0, 0)
	if err != nil {
		return -1, false
	}

	*ppShmRoot = unsafe.Pointer(v)

	//没法区分是New,始终返回true
	return 0, true
}

//Not suport window!
func DetachShm(ppShmRoot *unsafe.Pointer) int {
	return -1
}
