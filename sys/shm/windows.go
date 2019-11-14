// +build windows

package shm

import "unsafe"

//Not suport window!
func AttachShm(ppShmRoot *unsafe.Pointer, key int, size int) (int, bool) {
	return -1, false
}

//Not suport window!
func DetachShm(ppShmRoot *unsafe.Pointer) int {
	return -1
}
