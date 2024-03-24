# libvirt-storage-attach 
Attach and detach logical volume from libvirt domains. Acquires locks around operations and handles generating XML.

Volume attachments create a lockfile with the designated owner as the contents (although transient failures may prevent the actual attachment step--rerun to resume after interruption).

## Setup
```shell
cat <<'EOF' > /etc/libvirt-storage-attach.yaml
lock_path: /var/lib/libvirt-storage-attach
volume_group: fedora_localhost-live/pool00
EOF
```

## Usage
### Create
`pvId=$(libvirt-storage-attach -operation=create 4GB | tail -n 1)`

### Attach
`libvirt-storage-attach -operation=attach -vm-name=my-vm -pv-id=$pvId`

### Detach
`libvirt-storage-attach -operation=detach -vm-name=my-vm -pv-id=$pvId`