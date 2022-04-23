
provider "aws" {
  region     = "us-east-1"
  access_key = "XXXX"
  secret_key = "XXXXX"
}


#===========================================
variable "job-language" {
  default = "python"
}

variable "bucket-name" {
  default = "XXXXX"
}
variable "glue-arn" {
  default = "XXXX"

}
variable "job-name" {
  default = "compress-small-files-large-files"
}
variable "file-name" {
  default = "XXXXX"
}
#===========================================

resource "aws_s3_bucket_object" "upload-glue-script" {
  bucket = "${var.bucket-name}"
  key = "Scripts/${var.file-name}"
  source = "${var.file-name}"
}


resource "aws_glue_job" "glue-job" {
  name = "${var.job-name}"
  role_arn = "${var.glue-arn}"
  description = "This is script to create large files from small files "
  max_retries = "1"
  timeout = 2880
  command {
    script_location = "s3://${var.bucket-name}/Scripts/${var.file-name}"
    python_version = "3"
  }
  execution_property {
    max_concurrent_runs = 2
  }
  glue_version = "3.0"
}