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
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const DefaultConfigPath = "/etc/libvirt-storage-attach.yaml"
const AttachTimeout = 2500
const DetachTimeout = 2500
const QemuUrl = string(libvirt.QEMUSystem)

type config struct {
	LockPath       string `yaml:"lock_path"`
	LvmVolumeGroup string `yaml:"volume_group"`
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

	log.Printf("loaded conf %v\n", conf)

	if err := os.MkdirAll(conf.LockPath, 0755); err != nil {
		panic(err)
	}
}

func main() {
	var cfg config
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
		panic(errors.New("operation should be one of (attach, detach, create)"))
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
			panic(errors.New("vm-name must be set"))
		}
	}

	if operation == "create" {
		if len(volumeSizeText) < 1 {
			panic(errors.New("size must be set for create operation"))
		}

		if err := volumeSize.UnmarshalText([]byte(volumeSizeText)); err != nil {
			panic(err)
		}

		if volumeSize < datasize.GB {
			panic(errors.New("size must be at least 1GB"))
		}
	}

	log.Printf("operation: %s, pv-id: %s\n", operation, pvId)

	var err error
	switch operation {
	case "attach":
		c := lockedVmContext{
			Ctx:    context.TODO(),
			Cfg:    cfg,
			VmName: vmName,
			PvId:   pvId,
			PvLock: nil,
			VmLock: nil,
		}
		err = c.withLock(c.attach)

	case "detach":
		c := lockedVmContext{
			Ctx:    context.TODO(),
			Cfg:    cfg,
			VmName: vmName,
			PvId:   pvId,
			PvLock: nil,
			VmLock: nil,
		}
		err = c.withLock(c.detach)

	case "create":
		c := lockedVmContext{
			Ctx:  context.TODO(),
			Cfg:  cfg,
			PvId: "",
		}
		pvId, err = c.createVolume(volumeSize)
		fmt.Println(pvId)
	}

	if err != nil {
		panic(err)
	}
}

type deviceSummary struct {
	UsedTargets     map[string]bool
	AttachedDevices map[string]bool
	DeviceXml       map[string]string
	NextTarget      string
}

func detectBlockDevices(domainName string) deviceSummary {
	summary := deviceSummary{
		UsedTargets:     map[string]bool{},
		AttachedDevices: map[string]bool{},
		DeviceXml:       map[string]string{},
		NextTarget:      "",
	}

	uri, _ := url.Parse(QemuUrl)
	virtConn, err := libvirt.ConnectToURI(uri)
	defer virtConn.Disconnect()
	if err != nil {
		log.Fatal(err)
	}

	domain, err := virtConn.DomainLookupByName(domainName)
	if err != nil {
		log.Fatal(err)
	}
	domainXml, err := virtConn.DomainGetXMLDesc(domain, 0)
	if err != nil {
		log.Fatal(err)
	}
	//log.Println(domainXml)
	doc, err := xmlquery.Parse(strings.NewReader(domainXml))
	//testXmlFile, _ := os.Open("/home/nick/domain.xml")
	//defer testXmlFile.Close()
	//doc, err := xmlquery.Parse(testXmlFile)
	if err != nil {
		log.Println(err)
	}
	// If a VM has a file-backed disk, disk.type = file
	//result := xmlquery.Find(doc, "//domain/devices/disk[@type='block']")
	result := xmlquery.Find(doc, "//domain/devices/disk[@device='disk']")
	for _, dev := range result {
		device := xmlquery.FindOne(dev, "/source").SelectAttr("dev")
		target := xmlquery.FindOne(dev, "/target").SelectAttr("dev")
		log.Printf("found device: %s %s\n", target, device)

		if strings.HasPrefix(target, "sd") || strings.HasPrefix(target, "vd") {
			summary.UsedTargets[target[2:]] = true
		}

		deviceParts := strings.Split(device, "/")
		deviceId := deviceParts[len(deviceParts)-1]
		if strings.HasPrefix(deviceId, "pv-") {
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

	return summary
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
	log.Printf("%s owned by %s\n", c.PvId, pvOwner.Bytes())

	if pvOwner.Len() > 0 && pvOwner.String() != c.VmName {
		log.Fatalf("pv %s is in use can't be modified by '%s' since it's locked by '%s'", c.PvId, c.VmName, pvOwner.String())
	}

	if pvOwner.Len() == 0 {
		_, err := c.PvLock.WriteString(c.VmName)
		if err != nil {
			log.Fatal(err)
		}
	}

	innerError := fn()

	if err := unix.Flock(int(vmLock.Fd()), unix.LOCK_UN); err != nil {
		log.Fatal(err)
	}
	c.VmLock = nil

	if err := unix.Flock(int(vmLock.Fd()), unix.LOCK_UN); err != nil {
		log.Fatal(err)
	}
	c.PvLock = nil

	return innerError
}

func processOutput(cmd *exec.Cmd) (string, string, error) {
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
	summary := detectBlockDevices(c.VmName)
	if _, attached := summary.AttachedDevices[c.PvId]; !attached {
		log.Printf("attaching %s to %s as %s\n", c.PvId, c.VmName, summary.NextTarget)

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
			log.Fatal(err)
		}

		timeout, _ := context.WithTimeout(c.Ctx, AttachTimeout*time.Millisecond)
		log.Printf("virsh attach-device %s %s", c.VmName, xmlFilePath)
		stdout, stderr, err := processOutput(exec.CommandContext(
			timeout,
			"virsh",
			fmt.Sprintf("--connect=%s", QemuUrl),
			"attach-device",
			c.VmName,
			xmlFilePath,
		))

		os.Remove(xmlFilePath)

		if err != nil {
			log.Printf("lvcreate out: %s\n", stdout)
			log.Printf("lvcreate err: %s\n", stderr)
			return err
		}

		// TODO maybe exit code = 0 is sufficient
		if stdout != "Device attached successfully" {
			return fmt.Errorf("unexpected output '%s' from virsh", stdout)
		}
	} else {
		log.Printf("%s already attached to %s\n", c.PvId, c.VmName)
	}

	return nil
}

func (c *lockedVmContext) detach() error {
	var err error

	timeout, _ := context.WithTimeout(c.Ctx, DetachTimeout*time.Millisecond)
	summary := detectBlockDevices(c.VmName)
	if deviceXml, ok := summary.DeviceXml[c.PvId]; ok {
		log.Printf("Found XML for attached device: %s\n", deviceXml)

		// Remove from VM
		xmlFilePath, err := writeTempFile(c.PvId, deviceXml)
		if err != nil {
			log.Fatal(err)
		}

		stdout, stderr, err := processOutput(
			exec.CommandContext(
				timeout, "virsh",
				fmt.Sprintf("--connect=%s", QemuUrl),
				"detach-device",
				c.VmName,
				xmlFilePath,
			),
		)
		if err != nil {
			log.Printf("lvcreate out: %s\n", stdout)
			log.Printf("lvcreate err: %s\n", stderr)
			return err
		}

		// Remove from lock
		return c.PvLock.Truncate(0)
	} else {
		log.Fatal(fmt.Errorf("couldn't find %s in devices for %s", c.PvId, c.VmName))
	}

	return err
}

func (c *lockedVmContext) createVolume(volumeSize datasize.ByteSize) (string, error) {
	pvUuid, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	c.PvId = fmt.Sprintf("pv-%s", pvUuid.String())

	log.Printf("creating pv of size %s with id %s", volumeSize.String(), c.PvId)
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
		log.Printf("lvcreate out: %s\n", stdout)
		log.Printf("lvcreate err: %s\n", stderr)
		return "", err
	}

	// TODO handle orphaned LVs created without having UUID returned successfully
	return c.PvId, nil
}
