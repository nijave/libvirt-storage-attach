package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/c2h5oh/datasize"
	"github.com/google/uuid"
	"k8s.io/klog/v2"
	"libvirt-storage-attach/internal"
	"os"
)

func main() {
	var err error
	klog.InitFlags(nil)
	klog.SetOutput(os.Stderr)

	var cfg *internal.Config
	cfg, err = internal.LoadConfig()
	if err != nil {
		klog.Error(err)
		os.Exit(255)
	}

	var operation string
	flag.StringVar(&operation, "operation", "", "attach, detach, create, delete")

	var volumeGroup string
	flag.StringVar(&volumeGroup, "volume-group", cfg.LvmVolumeGroup, "volume group name")

	var pvId string
	flag.StringVar(&pvId, "pv-id", "", "persistent volume id")

	var vmName string
	flag.StringVar(&vmName, "vm-name", "", "virtual machine name")

	var volumeSizeText string
	var volumeSize datasize.ByteSize
	flag.StringVar(&volumeSizeText, "size", "", "volume size")

	flag.Parse()

	if ok := map[string]bool{"attach": true, "detach": true, "list": true, "create": true, "delete": true}[operation]; !ok {
		klog.Error(errors.New("operation should be one of (attach, detach, create)"))
		os.Exit(255)
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
			klog.Error(errors.New("vm-name must be set"))
			os.Exit(255)
		}
	}

	if operation == "create" {
		if len(volumeSizeText) < 1 {
			klog.Error(errors.New("size must be set for create operation"))
			os.Exit(255)
		}

		if err := volumeSize.UnmarshalText([]byte(volumeSizeText)); err != nil {
			klog.Error(err)
			os.Exit(255)
		}

		if volumeSize < datasize.GB {
			klog.Error(errors.New("size must be at least 1GB"))
			os.Exit(255)
		}
	}

	if operation == "delete" {
		pvIdFormatError := errors.New("pv-id should be in format pv-<uuid>")
		if len(pvId) != 39 {
			panic(pvIdFormatError)
		}
		if _, err := uuid.Parse(pvId[3:]); err != nil {
			panic(pvIdFormatError)
		}
	}

	klog.InfoS("running", "operation", operation, "pv-id", pvId, "vm-name", vmName, "volumeSize", volumeSize)

	switch operation {
	case "attach":
		c := internal.LockingVmContext{
			Ctx:    context.Background(),
			Cfg:    cfg,
			VmName: vmName,
			PvId:   pvId,
		}
		err = c.WithLock(true, c.Attach)

	case "detach":
		c := internal.LockingVmContext{
			Ctx:    context.Background(),
			Cfg:    cfg,
			VmName: vmName,
			PvId:   pvId,
		}
		err = c.WithLock(true, c.Detach)

	case "list":
		c := internal.LockingVmContext{
			Ctx: context.Background(),
			Cfg: cfg,
		}
		var out []*internal.VolumeInfo
		out, err = c.ListVolumes()
		if err == nil {
			var outJson []byte
			outJson, err = json.Marshal(out)
			fmt.Println(string(outJson))
		}

	case "create":
		c := internal.LockingVmContext{
			Ctx:  context.Background(),
			Cfg:  cfg,
			PvId: "",
		}
		pvId, err = c.CreateVolume(volumeSize, volumeGroup)
		if len(pvId) > 0 {
			fmt.Println(pvId)
		}

	case "delete":
		c := internal.LockingVmContext{
			Ctx:    context.Background(),
			Cfg:    cfg,
			PvId:   pvId,
			VmName: "",
		}
		err = c.WithLock(false, c.DeleteVolume)
	}

	if err != nil {
		klog.Error(err)
		os.Exit(1)
	}
}
