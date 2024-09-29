package internal

import (
	"github.com/antchfx/xmlquery"
	"github.com/digitalocean/go-libvirt"
	"k8s.io/klog/v2"
	"net/url"
	"strings"
)

type deviceSummary struct {
	UsedTargets     map[string]bool
	AttachedDevices map[string]bool
	DeviceXml       map[string]string
	NextTarget      string
}

func DetectBlockDevices(cfg *Config, domainName string) (deviceSummary, error) {
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
	result := xmlquery.Find(doc, "//domain/devices/disk")
	for _, dev := range result {
		device := xmlquery.FindOne(dev, "/source").SelectAttr("dev")
		target := xmlquery.FindOne(dev, "/target").SelectAttr("dev")
		// klog.InfoS("found device", "vm-name", domainName, "target", target, "device", device)

		if strings.HasPrefix(target, "sd") || strings.HasPrefix(target, "vd") {
			summary.UsedTargets[target[2:]] = true
		}

		deviceParts := strings.Split(device, "/")
		deviceId := deviceParts[len(deviceParts)-1]
		// It starts with the prefix and is length prefix + uuid length
		if strings.HasPrefix(deviceId, cfg.VolumePrefix) && len(deviceId) == len(cfg.VolumePrefix)+36 {
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

func listDomains(cfg *Config) ([]string, error) {
	var domains []string

	uri, _ := url.Parse(cfg.QemuUrl)
	virtConn, err := libvirt.ConnectToURI(uri)
	defer virtConn.Disconnect()
	if err != nil {
		return domains, err
	}

	domainList, _, err := virtConn.ConnectListAllDomains(1, 0)

	if err != nil {
		return domains, err
	}

	domains = make([]string, len(domainList))
	for i, domain := range domainList {
		domains[i] = domain.Name
	}

	return domains, nil
}

func ListAllAttachedPvs(cfg *Config) map[string][]string {
	attachedPvs := make(map[string][]string)

	domainList, err := listDomains(cfg)
	if err != nil {
		klog.Error(err.Error())
		return attachedPvs
	}

	for _, domain := range domainList {
		attachedDevices, err := DetectBlockDevices(cfg, domain)
		if err != nil {
			klog.Error("listAllAttachedPvs for %s error:", domain, err)
			continue
		}
		for device := range attachedDevices.AttachedDevices {
			attachedPvs[device] = append(attachedPvs[device], domain)
		}
	}

	klog.InfoS("attached pvs", "pvs", attachedPvs)

	return attachedPvs
}

func PersistDomainConfig(cfg *Config, domainName string) error {
	klog.InfoS("persisting domain config", "vm-name", domainName)

	uri, _ := url.Parse(cfg.QemuUrl)
	virtConn, err := libvirt.ConnectToURI(uri)
	defer virtConn.Disconnect()
	if err != nil {
		return err
	}

	domain, err := virtConn.DomainLookupByName(domainName)
	if err != nil {
		return err
	}

	xml, err := virtConn.DomainGetXMLDesc(domain, libvirt.DomainXMLSecure)
	if err != nil {
		return err
	}

	_, err = virtConn.DomainDefineXMLFlags(xml, libvirt.DomainDefineValidate)
	if err != nil {
		return err
	}

	return nil
}
