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

func detectBlockDevices(cfg *Config, domainName string) (deviceSummary, error) {
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
