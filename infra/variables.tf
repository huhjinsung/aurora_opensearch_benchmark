variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "ap-northeast-2"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "tmoney-poc"
}

# 기존 VPC 재사용
variable "vpc_id" {
  description = "Existing VPC ID to use"
  type        = string
}

variable "private_subnet_ids" {
  description = "Existing private subnet IDs (2 AZs)"
  type        = list(string)
}

# Aurora MySQL
variable "aurora_instance_class" {
  description = "Aurora MySQL instance class"
  type        = string
  default     = "db.r6g.large"
}

variable "aurora_master_username" {
  description = "Aurora MySQL master username"
  type        = string
  default     = "admin"
}

variable "aurora_master_password" {
  description = "Aurora MySQL master password"
  type        = string
  sensitive   = true
}

# OpenSearch
variable "opensearch_instance_type" {
  description = "OpenSearch instance type"
  type        = string
  default     = "r6g.large.search"
}

variable "opensearch_instance_count" {
  description = "Number of OpenSearch data nodes"
  type        = number
  default     = 2
}

variable "opensearch_master_user" {
  description = "OpenSearch master username"
  type        = string
  default     = "admin"
}

variable "opensearch_master_password" {
  description = "OpenSearch master password"
  type        = string
  sensitive   = true
}

# EC2
variable "bastion_instance_type" {
  description = "Bastion host instance type"
  type        = string
  default     = "t3.medium"
}

variable "injector_instance_type" {
  description = "Data injector EC2 instance type"
  type        = string
  default     = "c5.xlarge"
}
