// +build linux,cgo

package shm

/*
#cgo LDFLAGS: -lrt

#include    <stdio.h>
#include    <unistd.h>
#include    <errno.h>
#include    <string.h>
#include    <sys/types.h>
#include    <sys/ipc.h>
#include    <sys/shm.h>
#include    <sys/sem.h>

int AttachShm(void **ppShmRoot, key_t tShmKey, size_t iShmSize, void *bNew)
{
    int	iShmId = -1;
	*ppShmRoot = NULL;

	iShmId = shmget(tShmKey, iShmSize, IPC_CREAT|IPC_EXCL|0666);
	if (iShmId < 0)
	{
		if (errno != EEXIST)
			return -1;

		*(int*)bNew = 0;
	}
	else
	{
		*(int*)bNew = 1;
	}

    if (iShmId < 0)
    {
		iShmId = shmget(tShmKey, iShmSize, 0666);
        if (iShmId < 0)
            return -2;
    }

    *ppShmRoot = shmat(iShmId, NULL, 0);
    if (*ppShmRoot == NULL || (int64_t)(*ppShmRoot) == -1)
        return -3;

    return iShmId;
}

int DetachShm(void **ppShmRoot)
{
    if (!(*ppShmRoot))
        return -1;

    shmdt(*ppShmRoot);
    *ppShmRoot = NULL;

    return 0;
}
*/
import "C"

import (
	"unsafe"
)

func AttachShm(ppShmRoot *unsafe.Pointer, key int, size int) (int, bool) {
	bNew := 0
	iShmId := C.AttachShm(ppShmRoot, C.int(key), C.ulong(size), unsafe.Pointer(&bNew))
	return int(iShmId), bNew == 1
}

func DetachShm(ppShmRoot *unsafe.Pointer) int {
	return int(C.DetachShm(ppShmRoot))
}
