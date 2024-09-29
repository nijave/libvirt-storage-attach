package internal

import (
	"bufio"
	"context"
	"fmt"
	"github.com/c2h5oh/datasize"
	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
	"os"
	"path/filepath"
)

type VolumeInfo struct {
	Id       string
	Capacity uint64
	Owners   []string
}

type VolumeManager interface {
	Attach() error
	Detach() error
	ListVolumes() ([]*VolumeInfo, error)
	CreateVolume(size datasize.ByteSize) (string, error)
	DeleteVolume() error
}

type LockingVmContext struct {
	Ctx    context.Context
	Cfg    *Config
	VmName string
	PvId   string
	PvLock *os.File
	VmLock *os.File
}

func (c *LockingVmContext) WithLock(lockVm bool, fn func() error) error {
	var vmLock *os.File
	if lockVm {
		vmLock, _ := os.Create(filepath.Join(c.Cfg.LockPath, c.VmName))
		if err := unix.Flock(int(vmLock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			return err
		}
		c.VmLock = vmLock
	}

	pvLock, _ := os.OpenFile(filepath.Join(c.Cfg.LockPath, c.PvId), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err := unix.Flock(int(pvLock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return err
	}
	c.PvLock = pvLock

	pvOwner := bufio.NewScanner(pvLock).Text()
	klog.InfoS("device ownership", "pv-id", c.PvId, "vm-name", pvOwner)

	if len(pvOwner) > 0 && pvOwner != c.VmName {
		return fmt.Errorf("pv %s is in use can't be modified by '%s' since it's locked by '%s'", c.PvId, c.VmName, pvOwner)
	}

	if len(pvOwner) == 0 {
		_, err := c.PvLock.WriteString(c.VmName)
		if err != nil {
			return err
		}
	}

	innerError := fn()

	if vmLock != nil {
		if err := unix.Flock(int(vmLock.Fd()), unix.LOCK_UN); err != nil {
			return err
		}
		c.VmLock = nil
	}

	if err := unix.Flock(int(pvLock.Fd()), unix.LOCK_UN); err != nil {
		return err
	}
	c.PvLock = nil

	return innerError
}
