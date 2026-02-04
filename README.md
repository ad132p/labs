# labs

I am a dba, therefore most of the time I need to test software on virtual machines
This is how I decided to do it:

## Steps for Fedora 43
```
sudo dnf install @virtualization -y
```


## libvirt

```
sudo virsh pool-define-as --name default --type dir --target $HOME/terraform/volumes
sudo usermod -aG libvirt $USER
```


## terraform

Download VM images and create VMs.

```
ssh-keygen -b 2048 -t rsa -f ./ssh_keys/opentofu -q -N ""
# Choose your OS:
curl --output-dir "sources" -L -o rocky9.qcow2 https://download.rockylinux.org/pub/rocky/9/images/x86_64/Rocky-9-GenericCloud-Base.latest.x86_64.qcow2
curl --output-dir "sources" -L -o debian12.qcow2 https://cloud.debian.org/images/cloud/bookworm/latest/debian-12-generic-amd64.qcow2
tofu init
tofu apply
```

## s3 (garage)

```
mkdir garage/meta
mkdir garage/data

garage/create_config_file.sh 

podman run   -d   --name garaged   -p 3900:3900 -p 3901:3901 -p 3902:3902 -p 3903:3903  -v ./garage.toml:/etc/garage.toml:Z   -v ./garage/meta:/var/lib/garage/meta:Z   -v ./garage/data:/var/lib/garage/data:Z   dxflrs/garage:v2.2.0
```
## k8s

Please check ansible directory for deploying a local k8s cluster.


## airflow
```
export AIRFLOW_CONN_SSH_DEFAULT='ssh://username@:22?{"key_file": "/home/airflow/.ssh/id_rsa"}'
```
