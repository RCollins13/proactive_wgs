version 1.0

# Copyright (c) 2023 Ryan L. Collins and the Van Allen/Gusev/Haigis Laboratories
# Distributed under terms of the GPL-2.0 License (see LICENSE)
# Contact: Ryan L. Collins <Ryan_Collins@dfci.harvard.edu>

# Subset one or more VCFs to singletons or doubletons in samples from a predefined list

workflow SubsetRareVariants {
	input {
    Array[File] vcfs
    Array[File] vcf_idxs
    File sample_subset
    File? regions_bed
    String prefix
    String gatksv_samtools_cloud_docker
  }

  scatter ( vcf_info in zip(vcfs, vcf_idxs) ) {
    call Subset {
      input:
        vcf = vcf_info.left,
        vcf_idx = vcf_info.right,
        sample_list = sample_subset,
        regions_bed = regions_bed,
        prefix = basename(vcf_info.left, "vcf.gz") + prefix,
        gatksv_samtools_cloud_docker = gatksv_samtools_cloud_docker
    }
  }

  output {
    Array[File] vcf_subets = Subset.vcf_subset
    Array[File] vcf_subset_idxs = Subset.vcf_subset_idx
  }
}


task Subset {
  input {
    String vcf # Intentionally declared as string to use remote htslib streaming
    File vcf_idx
    File sample_list
    File? regions_bed
    String prefix
    String gatksv_samtools_cloud_docker
    Int disk_gb = 100
    Float mem_gb = 7.75
  }
  parameter_meta {
    vcf: {
      localization_optional: true
    }
  }

  String outfile = "${prefix}.vcf.gz"
  
  command <<<

    set -eu -o pipefail

    export GCS_OAUTH_TOKEN=`gcloud auth application-default print-access-token`

    bcftools view \
      --samples-file ~{sample_list} \
      --force-samples \
      --regions-file ~{regions_bed} \
      ~{vcf} \
    | bcftools view \
      -O z \
      -o ~{outfile} \
      --min-ac 1 \
      --max-ac 2 \
      --drop-genotypes

    tabix -p vcf -f ~{outfile}
  
  >>>

  output {
    File vcf_subset = "${outfile}"
    File vcf_subset_idx = "${outfile}.tbi"
  }

  runtime {
    cpu: 1
    memory: mem_gb + " GiB"
    disks: "local-disk " + disk_gb + " HDD"
    bootDiskSizeGb: 10
    docker: gatksv_samtools_cloud_docker
    preemptible: 1
    maxRetries: 1
  }
}
