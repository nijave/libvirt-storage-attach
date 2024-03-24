package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/c2h5oh/datasize"
	"github.com/google/uuid"
	"k8s.io/klog/v2"
	"libvirt-storage-attach/internal"
)

func main() {
	var err error
	klog.InitFlags(nil)

	var cfg *internal.Config
	cfg, err = internal.LoadConfig()
	if err != nil {
		klog.Fatal(err)
	}

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

	switch operation {
	case "attach":
		c := internal.LockedVmContext{
			Ctx:    context.Background(),
			Cfg:    cfg,
			VmName: vmName,
			PvId:   pvId,
		}
		err = c.WithLock(c.Attach)

	case "detach":
		c := internal.LockedVmContext{
			Ctx:    context.Background(),
			Cfg:    cfg,
			VmName: vmName,
			PvId:   pvId,
		}
		err = c.WithLock(c.Detach)

	case "create":
		c := internal.LockedVmContext{
			Ctx:  context.Background(),
			Cfg:  cfg,
			PvId: "",
		}
		pvId, err = c.CreateVolume(volumeSize)
		if len(pvId) > 0 {
			fmt.Println(pvId)
		}
	}

	if err != nil {
		klog.Fatal(err)
	}
}
