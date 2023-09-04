# This code is compatible with Terraform 4.25.0 and versions that are backwards compatible to 4.25.0.
# For information about validating this Terraform code, see https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/google-cloud-platform-build#format-and-validate-the-configuration

resource "google_compute_instance" "zk-bench-1" {
  boot_disk {
    auto_delete = true
    device_name = "zk-bench-1"

    initialize_params {
      image = "projects/fk-virt-infra/global/images/fk-bullseye-golden-image-v20230509"
      size  = 10
      type  = "pd-balanced"
    }

    mode = "READ_WRITE"
  }

  can_ip_forward      = false
  deletion_protection = false
  enable_display      = false

  labels = {
    goog-ec-src = "vm_add-tf"
  }

  machine_type = "e2-standard-4"

  metadata = {
    enable-oslogin  = "true"
    startup-script  = "#!/bin/bash\n\napt-get -q -o Dir::Etc::sourcelist=\"sources.list.d/google-cloud.list\" -o Dir::Etc::sourceparts=\"-\" -o APT::Get::List-Cleanup=\"0\" -o Acquire::http::Timeout=\"10\" -o Acquire::ftp::Timeout=\"10\" update\n\napt-get install -y jq\n\nget_meta(){\n  http_code=$(curl -w \"%%{http_code}\" -o /dev/null -s $1 -H \"Metadata-Flavor: Google\")\n  #echo the response code is $http_code\n  if [[ $http_code != 200 ]]\n  then\n    CONTENT=\"\"\n    ERROR=\"true\"\n  else\n    CONTENT=\"$(curl $1 -H \"Metadata-Flavor: Google\")\"\n    ERROR=\"false\"\n  fi\n}\n\nmkdir -p /etc/default/megh/\n\nFILE=\"/etc/default/megh/instance_metadata.json\"\n\nget_meta \"http://metadata.google.internal/computeMetadata/v1/instance/id\"\nID=$CONTENT\n\nget_meta \"http://metadata.google.internal/computeMetadata/v1/project/attributes/app\" #custom metadata\nif [ \"$ERROR\" == \"true\" ]; then\n  exit 1\nfi\nAPP=$CONTENT\n\nget_meta \"http://metadata.google.internal/computeMetadata/v1/project/project-id\"\nPROJECT_ID=$CONTENT\n\nget_meta \"http://metadata.google.internal/computeMetadata/v1/instance/attributes/created-by\"\nINSTANCE_GROUP=$(basename $CONTENT)\n\nget_meta \"http://metadata.google.internal/computeMetadata/v1/instance/machine-type\"\nINSTANCE_TYPE=$(basename $CONTENT)\n\nget_meta \"http://metadata.google.internal/computeMetadata/v1/instance/hostname\"\nHOSTNAME=$CONTENT\n\nget_meta \"http://metadata.google.internal/computeMetadata/v1/instance/disks/?recursive=true&alt=json\"\ntemp=($CONTENT)\nDISKS=\"[]\"\ntemp_len=$(echo $temp | jq length)\nfor i in $(seq 0 $(($temp_len-1)))\ndo\n  DISK_TYPE=$(echo $temp | jq -c \".[$i].type\");\n  item=$(jq -nc --arg disk_type $DISK_TYPE \"{disk_type: $DISK_TYPE}\")\n  DISKS=$(echo $DISKS | jq -c \".[$i] |= .+ $item\")\ndone\n\nget_meta \"http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/?recursive=true&alt=json\"\ntemp=($CONTENT)\nNET_INT=\"[]\"\ntemp_len=$(echo $temp | jq length)\nfor i in $(seq 0 $(($temp_len-1)))\ndo\n  IPV4=$(echo $temp | jq -c \".[$i].ip\");\n  HWADDR=$(echo $temp | jq -c \".[$i].mac\");\n  item=$(jq -nc --arg ipv4 $IPV4 --arg hw_address $HWADDR \"{ipv4: $IPV4, hw_address: $HWADDR}\")\n  NET_INT=$(echo $NET_INT | jq -c \".[$i] |= .+ $item\")\ndone\n\nMACHINE_TYPE=\"vm\"\n\nget_meta \"http://metadata.google.internal/computeMetadata/v1/instance/zone\" #GCP zone\nFAULT_ZONE=$(basename $CONTENT)\n\nZONE=$(echo $FAULT_ZONE | sed 's|\\(.*\\)-.*|\\1|')\n\nget_meta \"http://metadata.google.internal/computeMetadata/v1/instance/attributes/vpc-subnet-name\" #custom metadata\nVPC_SUBNET_NAME=$CONTENT\n\nget_meta \"http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/network\"\nVPC_NAME=$(basename $CONTENT)\n\nDETAIL=$(jq -nc \\\n --argjson disks $DISKS --argjson network_interfaces $NET_INT \"{disks: $DISKS, network_interfaces: $NET_INT}\")\n\ncat > $FILE <<EOF\n{\n  \"id\": \"$ID\",\n  \"app\": \"$APP\",\n  \"project-id\": \"$PROJECT_ID\",\n  \"instance_group\": \"$INSTANCE_GROUP\",\n  \"instance_type\": \"$INSTANCE_TYPE\",\n  \"hostname\": \"$HOSTNAME\",\n  \"detail\": $DETAIL,\n  \"machine_type\": \"$MACHINE_TYPE\",\n  \"zone\": \"$ZONE\",\n  \"fault_zone\": \"$FAULT_ZONE\",\n  \"vpc_subnet_name\": \"$VPC_SUBNET_NAME\",\n  \"vpc_name\": \"$VPC_NAME\"\n}\nEOF\n\n# Installing bastion agent\ncd /etc/apt/sources.list.d/\nFILE=\"bastion.list\"\nif [ -f \"$FILE\" ]; then\n  if ! grep -Fq \"deb [trusted=yes] http://10.19.1.12/iaas iaas flipkart\" \"$FILE\" ; then\n  \techo 'deb [trusted=yes] http://10.19.1.12/iaas iaas flipkart' >> $FILE\n  fi\nelse\n  echo 'deb [trusted=yes] http://10.19.1.12/iaas iaas flipkart' > $FILE\nfi\n\napt-get -q -o Dir::Etc::sourcelist=\"sources.list.d/$FILE\" -o Dir::Etc::sourceparts=\"-\" -o APT::Get::List-Cleanup=\"0\" -o Acquire::http::Timeout=\"10\" -o Acquire::ftp::Timeout=\"10\" update\n\napt-get -y install fk-bastion-agent\n\n# Installing User setup script\nget_meta \"http://metadata.google.internal/computeMetadata/v1/instance/attributes/user-script\"\nif [ $ERROR != \"true\" ]; then\n  eval echo \"$CONTENT\"\nfi\n"
    vpc-subnet-name = "fk-preprod-sub-ass1-primary-rb43"
  }

  name = "zk-bench-1"

  network_interface {
    subnetwork = "projects/fk-network-svcs-ab12/regions/asia-south1/subnetworks/fk-preprod-sub-ass1-primary-rb43"
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    preemptible         = false
    provisioning_model  = "STANDARD"
  }

  service_account {
    email  = "fk-varadhi-oss-d-sa@fk-varadhi-oss-d.iam.gserviceaccount.com"
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_secure_boot          = true
    enable_vtpm                 = true
  }

  zone = "asia-south1-c"
}
