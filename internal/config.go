package internal

import (
	"gopkg.in/yaml.v3"
	"k8s.io/klog/v2"
	"os"
	"time"
)

const DefaultConfigPath = "/etc/libvirt-storage-attach.yaml"

type Config struct {
	LockPath       string        `yaml:"lock_path"`
	LvmVolumeGroup string        `yaml:"volume_group"`
	QemuUrl        string        `yaml:"qemu_url"`
	AttachTimeout  time.Duration `yaml:"attach_timeout"`
	DetachTimeout  time.Duration `yaml:"detach_timeout"`
	VolumePrefix   string        `yaml:"volume_prefix"`
}

func LoadConfig() (*Config, error) {
	configPath := os.Getenv("CONFIG_PATH")
	if len(configPath) == 0 {
		configPath = DefaultConfigPath
	}

	y, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	conf := &Config{
		LockPath:       "",
		LvmVolumeGroup: "",
		QemuUrl:        "qemu:///system",
		AttachTimeout:  2500 * time.Millisecond,
		DetachTimeout:  2500 * time.Millisecond,
		VolumePrefix:   "pv-",
	}

	if err := yaml.Unmarshal(y, conf); err != nil {
		return nil, err
	}

	klog.Infof("loaded conf %v", conf)

	if err := os.MkdirAll(conf.LockPath, 0755); err != nil {
		return nil, err
	}

	return conf, nil
}
