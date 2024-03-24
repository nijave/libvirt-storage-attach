package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/antchfx/xmlquery"
	"github.com/c2h5oh/datasize"
	"github.com/digitalocean/go-libvirt"
	"github.com/google/uuid"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
	"io"
	"k8s.io/klog/v2"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const DefaultConfigPath = "/etc/libvirt-storage-attach.yaml"

type config struct {
	LockPath       string        `yaml:"lock_path"`
	LvmVolumeGroup string        `yaml:"volume_group"`
	QemuUrl        string        `yaml:"qemu_url"`
	AttachTimeout  time.Duration `yaml:"attach_timeout"`
	DetachTimeout  time.Duration `yaml:"detach_timeout"`
	VolumePrefix   string        `yaml:"volume_prefix"`
}

type lockedVmContext struct {
	Ctx    context.Context
	Cfg    config
	VmName string
	PvId   string
	PvLock *os.File
	VmLock *os.File
}

func loadConfig(conf *config) {
	configPath := os.Getenv("CONFIG_PATH")
	if len(configPath) == 0 {
		configPath = DefaultConfigPath
	}

	y, err := os.ReadFile(configPath)
	if err != nil {
		panic(err)
	}

	if err := yaml.Unmarshal(y, conf); err != nil {
		panic(err)
	}

	klog.Infof("loaded conf %v", conf)

	if err := os.MkdirAll(conf.LockPath, 0755); err != nil {
		panic(err)
	}
}

func main() {
	klog.InitFlags(nil)

	cfg := config{
		LockPath:       "",
		LvmVolumeGroup: "",
		QemuUrl:        "qemu:///system",
		AttachTimeout:  2500 * time.Millisecond,
		DetachTimeout:  2500 * time.Millisecond,
		VolumePrefix:   "pv-",
	}
	loadConfig(&cfg)

	var operation string
	flag.StringVar(&operation, "operation", "", "attach, detach, or create")

	var pvId string
	flag.StringVar(&pvId, "pv-id", "", "persistent volume id")

	var vmName string
	flag.StringVar(&vmName, "vm-name", "", "virtual machine name")

	var volumeSizeText string
	var volumeSize datasize.ByteSize
	flag.StringVar(&volumeSizeText, "size", "", "volume size")

	flag.Parse()

	if ok := map[string]bool{"attach": true, "detach": true, "create": true}[operation]; !ok {
		klog.Fatal(errors.New("operation should be one of (attach, detach, create)"))
	}

	if operation == "attach" || operation == "detach" {
		pvIdFormatError := errors.New("pv-id should be in format pv-<uuid>")
		if len(pvId) != 39 {
			panic(pvIdFormatError)
		}
		if _, err := uuid.Parse(pvId[3:]); err != nil {
			panic(pvIdFormatError)
		}

		if len(vmName) < 1 {
			klog.Fatal(errors.New("vm-name must be set"))
		}
	}

	if operation == "create" {
		if len(volumeSizeText) < 1 {
			klog.Fatal(errors.New("size must be set for create operation"))
		}

		if err := volumeSize.UnmarshalText([]byte(volumeSizeText)); err != nil {
			klog.Fatal(err)
		}

		if volumeSize < datasize.GB {
			klog.Fatal(errors.New("size must be at least 1GB"))
		}
	}

	klog.InfoS("running", "operation", operation, "pv-id", pvId, "vm-name", vmName, "volumeSize", volumeSize)

	var err error
	switch operation {
	case "attach":
		c := lockedVmContext{
			Ctx:    context.Background(),
			Cfg:    cfg,
			VmName: vmName,
			PvId:   pvId,
		}
		err = c.withLock(c.attach)

	case "detach":
		c := lockedVmContext{
			Ctx:    context.Background(),
			Cfg:    cfg,
			VmName: vmName,
			PvId:   pvId,
		}
		err = c.withLock(c.detach)

	case "create":
		c := lockedVmContext{
			Ctx:  context.Background(),
			Cfg:  cfg,
			PvId: "",
		}
		pvId, err = c.createVolume(volumeSize)
		if len(pvId) > 0 {
			fmt.Println(pvId)
		}
	}

	if err != nil {
		klog.Fatal(err)
	}
}

type deviceSummary struct {
	UsedTargets     map[string]bool
	AttachedDevices map[string]bool
	DeviceXml       map[string]string
	NextTarget      string
}

func detectBlockDevices(cfg config, domainName string) (deviceSummary, error) {
	summary := deviceSummary{
		UsedTargets:     map[string]bool{},
		AttachedDevices: map[string]bool{},
		DeviceXml:       map[string]string{},
		NextTarget:      "",
	}

	uri, _ := url.Parse(cfg.QemuUrl)
	virtConn, err := libvirt.ConnectToURI(uri)
	defer virtConn.Disconnect()
	if err != nil {
		return summary, err
	}

	domain, err := virtConn.DomainLookupByName(domainName)
	if err != nil {
		return summary, err
	}
	domainXml, err := virtConn.DomainGetXMLDesc(domain, 0)
	if err != nil {
		return summary, err
	}
	doc, err := xmlquery.Parse(strings.NewReader(domainXml))
	if err != nil {
		return summary, err
	}

	// If a VM has a file-backed disk, disk.type = file
	//result := xmlquery.Find(doc, "//domain/devices/disk[@type='block']")
	result := xmlquery.Find(doc, "//domain/devices/disk[@device='disk']")
	for _, dev := range result {
		device := xmlquery.FindOne(dev, "/source").SelectAttr("dev")
		target := xmlquery.FindOne(dev, "/target").SelectAttr("dev")
		klog.InfoS("found device", "vm-name", domainName, "target", target, "device", device)

		if strings.HasPrefix(target, "sd") || strings.HasPrefix(target, "vd") {
			summary.UsedTargets[target[2:]] = true
		}

		deviceParts := strings.Split(device, "/")
		deviceId := deviceParts[len(deviceParts)-1]
		if strings.HasPrefix(deviceId, cfg.VolumePrefix) {
			summary.AttachedDevices[deviceId] = true
			summary.DeviceXml[deviceId] = dev.OutputXML(true)
		}
	}

outer:
	for i := 96; i <= 122; i++ {
		iChr := string(i)
		if iChr == "`" {
			iChr = ""
		}
		for j := 97; j <= 122; j++ {
			summary.NextTarget = iChr + string(j)
			if _, ok := summary.UsedTargets[summary.NextTarget]; !ok {
				break outer
			}
		}
	}
	summary.NextTarget = "vd" + summary.NextTarget

	return summary, nil
}

func (c *lockedVmContext) withLock(fn func() error) error {
	vmLock, _ := os.Create(filepath.Join(c.Cfg.LockPath, c.VmName))
	if err := unix.Flock(int(vmLock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		log.Fatal(err)
	}
	c.VmLock = vmLock

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

	if err := unix.Flock(int(vmLock.Fd()), unix.LOCK_UN); err != nil {
		return err
	}
	c.VmLock = nil

	if err := unix.Flock(int(vmLock.Fd()), unix.LOCK_UN); err != nil {
		return err
	}
	c.PvLock = nil

	return innerError
}

func processOutput(cmd *exec.Cmd) (string, string, error) {
	klog.InfoS("processing output", "command", cmd.Args)
	bytesOut, err := cmd.Output()
	stdout := strings.TrimRight(string(bytesOut), "\n\t\r ")

	stderr := ""
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		stderr = strings.TrimRight(string(exitError.Stderr), "\n\t\r ")
	}

	return stdout, stderr, err
}

func writeTempFile(suffix, contents string) (string, error) {
	file, err := os.CreateTemp(os.TempDir(), fmt.Sprintf("%s-", suffix))
	if err != nil {
		return "", err
	}
	file.WriteString(contents)
	file.Close()
	return file.Name(), nil
}

func (c *lockedVmContext) attach() error {
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

func (c *lockedVmContext) detach() error {
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

func (c *lockedVmContext) createVolume(volumeSize datasize.ByteSize) (string, error) {
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
