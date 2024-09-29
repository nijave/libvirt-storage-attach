package lvm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/c2h5oh/datasize"
	"github.com/google/uuid"
	"k8s.io/klog/v2"
	"libvirt-storage-attach/internal"
	"os"
	"os/exec"
	"strings"
)

type LogicalVolume struct {
	LogicalVolume string `json:"lv_name"`
	VolumeGroup   string `json:"vg_name"`
}

type LvReport struct {
	Report []LogicalVolume `json:"lv"`
}

type LvmReportList struct {
	Reports []LvReport `json:"report"`
}

type VolumeManager struct {
	LockingVmContext *internal.LockingVmContext
	VolumeGroup      string
}

func getVolumeVolumeGroupMapping() map[string]string {
	mapping := make(map[string]string)

	stdout, err := exec.Command(
		"lvs",
		"-o", "vg_name,lv_name",
		"--reportformat", "json",
	).Output()

	if err != nil {
		klog.ErrorS(err, "failed to get logical volume info")
		return mapping
	}

	var report LvmReportList
	err = json.Unmarshal(stdout, &report)

	if err != nil {
		klog.ErrorS(err, "failed to unmarshal logical volume info")
		return mapping
	}

	// There should only be a single report
	for _, report := range report.Reports {
		for _, lv := range report.Report {
			mapping[lv.LogicalVolume] = lv.VolumeGroup
		}
	}

	return mapping
}

func (vManager *VolumeManager) volumeGroupAndLv() (string, string) {
	c := vManager.LockingVmContext
	var volumeGroup string
	volumeGroupMapping := getVolumeVolumeGroupMapping()

	if _, ok := volumeGroupMapping[c.PvId]; !ok {
		// The default volume group can reference a thin pool
		volumeGroup = strings.Split(c.Cfg.LvmVolumeGroup, "/")[0]
	} else {
		volumeGroup = volumeGroupMapping[c.PvId]
	}

	return volumeGroup, c.PvId
}

func (vManager *VolumeManager) lvWithVolumeGroup() string {
	vg, lv := vManager.volumeGroupAndLv()
	return strings.Join([]string{vg, lv}, "/")
}

func (vManager *VolumeManager) Attach() error {
	c := vManager.LockingVmContext
	summary, err := internal.DetectBlockDevices(c.Cfg, c.VmName)
	if err != nil {
		return err
	}
	if _, attached := summary.AttachedDevices[c.PvId]; !attached {
		vg, lv := vManager.volumeGroupAndLv()
		klog.InfoS("attaching device", "pv-id", c.PvId, "vm-name", c.VmName, "target", summary.NextTarget)

		// attach code (after writing state update to lock)
		devicePath := fmt.Sprintf("/dev/%s/%s", vg, lv)
		deviceXmlTmpl := `
<disk type='block' device='disk'>
  <driver name='qemu' type='raw' cache='writeback' discard='unmap'/>
  <source dev='%s'/>
  <target dev="%s" bus="scsi" />
  <serial>%s</serial>
</disk>
`
		deviceSerial := strings.Replace(strings.TrimPrefix(c.PvId, c.Cfg.VolumePrefix), "-", "", -1)
		deviceXml := fmt.Sprintf(deviceXmlTmpl, devicePath, summary.NextTarget, deviceSerial)
		xmlFilePath, err := internal.WriteTempFile(c.PvId, deviceXml)
		if err != nil {
			return err
		}

		timeout, _ := context.WithTimeout(c.Ctx, c.Cfg.AttachTimeout)
		stdout, stderr, err := internal.ProcessOutput(exec.CommandContext(
			timeout,
			"virsh",
			fmt.Sprintf("--connect=%s", c.Cfg.QemuUrl),
			"attach-device",
			"--current",
			c.VmName,
			xmlFilePath,
		))

		if err != nil {
			klog.ErrorS(err, "failed to attach-device", "vm-name", c.VmName, "stdout", stdout, "stderr", stderr)
			return err
		}

		os.Remove(xmlFilePath)

		err = internal.PersistDomainConfig(c.Cfg, c.VmName)
		if err != nil {
			klog.ErrorS(err, "couldn't persist domain config", "vm-name", c.VmName)
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

func (vManager *VolumeManager) Detach() error {
	c := vManager.LockingVmContext
	var err error

	timeout, _ := context.WithTimeout(c.Ctx, c.Cfg.DetachTimeout)
	summary, err := internal.DetectBlockDevices(c.Cfg, c.VmName)

	if err != nil && strings.HasPrefix(err.Error(), "Domain not found: ") {
		return nil
	}

	if err != nil {
		return err
	}

	if deviceXml, ok := summary.DeviceXml[c.PvId]; ok {
		klog.InfoS("found device xml", "pv-id", c.PvId, "vm-name", c.VmName, "xml", deviceXml)

		// Remove from VM
		xmlFilePath, err := internal.WriteTempFile(c.PvId, deviceXml)
		if err != nil {
			return err
		}

		stdout, stderr, err := internal.ProcessOutput(
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

		err = internal.PersistDomainConfig(c.Cfg, c.VmName)
		if err != nil {
			klog.ErrorS(err, "couldn't persist domain config", "vm-name", c.VmName)
		}

		// Remove from lock
		return c.PvLock.Truncate(0)
	} else {
		klog.Warningf("couldn't find %s in devices for %s", c.PvId, c.VmName)
		return nil
	}
}

func (vManager *VolumeManager) ListVolumes() ([]*internal.VolumeInfo, error) {
	c := vManager.LockingVmContext
	var volumeInfo []*internal.VolumeInfo

	vg, _ := vManager.volumeGroupAndLv()
	stdout, stderr, err := internal.ProcessOutput(exec.CommandContext(
		c.Ctx,
		"lvs",
		"--noheadings",
		"--units",
		"B",
		"-o",
		"name,size",
		"--separator",
		"\t",
		"--select",
		fmt.Sprintf(
			"name=~%s[^.]+ && vg_name=%s",
			c.Cfg.VolumePrefix,
			vg,
		),
	))

	if err != nil || strings.HasPrefix(stderr, "WARNING: ") {
		klog.InfoS("command output", "command", "lvs", "stdout", stdout, "stderr", stderr, "err", err)
		if err == nil && strings.Contains(stderr, "Permission denied") {
			err = errors.New("permission denied")
		}
		return volumeInfo, err
	}

	attachedPvs := internal.ListAllAttachedPvs(c.Cfg)

	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		line = strings.TrimSpace(line)
		parts := strings.Split(line, "\t")
		klog.InfoS("parsing lvs line", "line", parts)
		dataSize, _ := datasize.Parse([]byte(parts[1]))
		pvId := parts[0]
		if strings.HasPrefix(pvId, c.Cfg.VolumePrefix) && len(pvId) == len(c.Cfg.VolumePrefix)+36 {
			owners := &[]string{}
			klog.InfoS("checking for pv owners", "pv", pvId)
			if domain, ok := attachedPvs[pvId]; ok {
				owners = &domain
			}
			volumeInfo = append(volumeInfo, &internal.VolumeInfo{
				Id:       parts[0],
				Capacity: dataSize.Bytes(),
				Owners:   *owners,
			})
		}

	}

	// TODO For some reason all the lock files are empty
	//for _, volume := range volumeInfo {
	//	pvLock, err := os.OpenFile(
	//		filepath.Join(c.Cfg.LockPath, volume.Id),
	//		0,
	//		0600,
	//	)
	//
	//	if err != nil {
	//		klog.Warningf("%s %s", volume.Id, err.Error())
	//	} else {
	//		owner := bufio.NewScanner(pvLock).Text()
	//		if len(owner) > 0 {
	//			volume.Owners = []string{owner}
	//			continue
	//		}
	//	}
	//
	//	volume.Owners = []string{}
	//}

	klog.InfoS("volume list", "volumes", volumeInfo)

	return volumeInfo, nil
}

func (vManager *VolumeManager) CreateVolume(volumeSize datasize.ByteSize) (string, error) {
	c := vManager.LockingVmContext
	pvUuid, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	c.PvId = fmt.Sprintf("%s%s", c.Cfg.VolumePrefix, pvUuid.String())

	sizeFlag := "-L"
	if strings.Contains(vManager.VolumeGroup, "/") {
		// Probably creating a thin provisioned volume i.e. vg-name/thin-pool-name
		sizeFlag = "-V"
	}

	klog.InfoS("creating logical volume", "pv-id", c.PvId, "size", volumeSize, "volume-group", vManager.VolumeGroup)
	// Create LV
	stdout, stderr, err := internal.ProcessOutput(
		exec.CommandContext(
			c.Ctx,
			"lvcreate",
			sizeFlag,
			volumeSize.String(),
			vManager.VolumeGroup,
			"-n",
			c.PvId,
		),
	)

	if err != nil {
		klog.InfoS("command output", "command", "lvcreate", "stdout", stdout, "stderr", stderr, "err", err)
		return "", err
	}

	// TODO handle orphaned LVs created without having UUID returned successfully
	return c.PvId, nil
}

func (vManager *VolumeManager) DeleteVolume() error {
	c := vManager.LockingVmContext
	klog.InfoS("deleting logical volume", "pv-id", c.PvId)
	stdout, stderr, err := internal.ProcessOutput(
		exec.CommandContext(
			c.Ctx,
			"lvremove",
			"--yes",
			vManager.lvWithVolumeGroup(),
		),
	)

	if err != nil {
		klog.InfoS("command output", "command", "lvremove", "stdout", stdout, "stderr", stderr, "err", err)
	}

	return err
}
