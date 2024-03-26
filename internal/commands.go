package internal

import (
	"bytes"
	"context"
	"fmt"
	"github.com/c2h5oh/datasize"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
	"io"
	"k8s.io/klog/v2"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

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
			log.Fatal(err)
		}
		c.VmLock = vmLock
	}

	pvLock, _ := os.OpenFile(filepath.Join(c.Cfg.LockPath, c.PvId), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err := unix.Flock(int(pvLock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		log.Fatal(err)
	}
	c.PvLock = pvLock

	pvOwner := bytes.NewBuffer(nil)
	io.Copy(pvOwner, pvLock)
	klog.InfoS("device ownership", "pv-id", c.PvId, "vm-name", pvOwner.Bytes())

	if pvOwner.Len() > 0 && pvOwner.String() != c.VmName {
		return fmt.Errorf("pv %s is in use can't be modified by '%s' since it's locked by '%s'", c.PvId, c.VmName, pvOwner.String())
	}

	if pvOwner.Len() == 0 {
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

func (c *LockingVmContext) Attach() error {
	summary, err := detectBlockDevices(c.Cfg, c.VmName)
	if err != nil {
		return err
	}
	if _, attached := summary.AttachedDevices[c.PvId]; !attached {
		klog.InfoS("attaching device", "pv-id", c.PvId, "vm-name", c.VmName, "target", summary.NextTarget)

		// attach code (after writing state update to lock)
		devicePath := fmt.Sprintf("/dev/%s/%s", strings.Split(c.Cfg.LvmVolumeGroup, "/")[0], c.PvId)
		deviceXmlTmpl := `
<disk type='block' device='disk'>
  <driver name='qemu' type='raw' cache='writeback' discard='unmap'/>
  <source dev='%s'/>
  <target dev="%s" bus="scsi" />
  <serial>%s</serial>
</disk>
`
		deviceXml := fmt.Sprintf(deviceXmlTmpl, devicePath, summary.NextTarget, c.PvId)
		xmlFilePath, err := writeTempFile(c.PvId, deviceXml)
		if err != nil {
			return err
		}

		timeout, _ := context.WithTimeout(c.Ctx, c.Cfg.AttachTimeout)
		stdout, stderr, err := processOutput(exec.CommandContext(
			timeout,
			"virsh",
			fmt.Sprintf("--connect=%s", c.Cfg.QemuUrl),
			"attach-device",
			c.VmName,
			xmlFilePath,
		))

		os.Remove(xmlFilePath)

		if err != nil {
			klog.InfoS("command output", "stdout", stdout, "stderr", stderr, "err", err)
			return err
		}

		// TODO maybe exit code = 0 is sufficient
		if stdout != "Device attached successfully" {
			return fmt.Errorf("unexpected output '%s' from virsh", stdout)
		}
	} else {
		klog.Warningf("%s already attached to %s", c.PvId, c.VmName)
	}

	return nil
}

func (c *LockingVmContext) Detach() error {
	var err error

	timeout, _ := context.WithTimeout(c.Ctx, c.Cfg.DetachTimeout)
	summary, err := detectBlockDevices(c.Cfg, c.VmName)
	if err != nil {
		return err
	}
	if deviceXml, ok := summary.DeviceXml[c.PvId]; ok {
		klog.InfoS("found device xml", "pv-id", c.PvId, "vm-name", c.VmName, "xml", deviceXml)

		// Remove from VM
		xmlFilePath, err := writeTempFile(c.PvId, deviceXml)
		if err != nil {
			return err
		}

		stdout, stderr, err := processOutput(
			exec.CommandContext(
				timeout, "virsh",
				fmt.Sprintf("--connect=%s", c.Cfg.QemuUrl),
				"detach-device",
				c.VmName,
				xmlFilePath,
			),
		)
		if err != nil {
			klog.InfoS("command output", "stdout", stdout, "stderr", stderr, "err", err)
			return err
		}

		// Remove from lock
		return c.PvLock.Truncate(0)
	} else {
		return fmt.Errorf("couldn't find %s in devices for %s", c.PvId, c.VmName)
	}

	return err
}

func (c *LockingVmContext) CreateVolume(volumeSize datasize.ByteSize) (string, error) {
	pvUuid, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	c.PvId = fmt.Sprintf("%s%s", c.Cfg.VolumePrefix, pvUuid.String())

	klog.InfoS("creating logical volume", "pv-id", c.PvId, "size", volumeSize)
	// Create LV
	stdout, stderr, err := processOutput(
		exec.CommandContext(
			c.Ctx,
			"lvcreate",
			"-V",
			volumeSize.String(),
			c.Cfg.LvmVolumeGroup,
			"-n",
			c.PvId,
		),
	)

	if err != nil {
		klog.InfoS("command output", "stdout", stdout, "stderr", stderr, "err", err)
		return "", err
	}

	// TODO handle orphaned LVs created without having UUID returned successfully
	return c.PvId, nil
}

func (c *LockingVmContext) DeleteVolume() error {
	klog.InfoS("deleting logical volume", "pv-id", c.PvId)
	stdout, stderr, err := processOutput(
		exec.CommandContext(
			c.Ctx,
			"lvremove",
			"--yes",
			strings.Join([]string{
				strings.Split(c.Cfg.LvmVolumeGroup, "/")[0],
				c.PvId,
			}, "/"),
		),
	)

	if err != nil {
		klog.InfoS("command output", "stdout", stdout, "stderr", stderr, "err", err)
	}

	return err
}
