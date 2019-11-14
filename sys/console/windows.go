// +build windows

package console

import (
	"syscall"
	"unsafe"
)

func SetConsoleTitle(title string) {
	kernel32, err := syscall.LoadLibrary("kernel32.dll")
	if err != nil {
		return
	}
	defer syscall.FreeLibrary(kernel32)
	api, _ := syscall.GetProcAddress(kernel32, "SetConsoleTitleW")

	syscall.Syscall(api, 1, uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(title))), 0, 0)
}
