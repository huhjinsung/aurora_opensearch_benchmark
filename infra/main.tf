###############################################################################
# Data Sources - 기존 AWS-VPC 재사용
###############################################################################
data "aws_vpc" "existing" {
  id = var.vpc_id
}

data "aws_subnet" "private" {
  count = length(var.private_subnet_ids)
  id    = var.private_subnet_ids[count.index]
}

data "aws_ami" "amazon_linux_2" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

locals {
  tags = {
    Project   = var.project_name
    ManagedBy = "terraform"
  }
}

###############################################################################
# VPC Endpoints for SSM (Private Subnet에서 SSM 접속용)
###############################################################################
resource "aws_security_group" "vpc_endpoints" {
  name_prefix = "${var.project_name}-vpce-"
  description = "VPC Endpoints - HTTPS from VPC"
  vpc_id      = data.aws_vpc.existing.id

  ingress {
    description = "HTTPS from VPC"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.existing.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${var.project_name}-sg-vpce" })
}

resource "aws_vpc_endpoint" "ssm" {
  vpc_id              = data.aws_vpc.existing.id
  service_name        = "com.amazonaws.${var.aws_region}.ssm"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = merge(local.tags, { Name = "${var.project_name}-vpce-ssm" })
}

resource "aws_vpc_endpoint" "ssmmessages" {
  vpc_id              = data.aws_vpc.existing.id
  service_name        = "com.amazonaws.${var.aws_region}.ssmmessages"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = merge(local.tags, { Name = "${var.project_name}-vpce-ssmmessages" })
}

resource "aws_vpc_endpoint" "ec2messages" {
  vpc_id              = data.aws_vpc.existing.id
  service_name        = "com.amazonaws.${var.aws_region}.ec2messages"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = merge(local.tags, { Name = "${var.project_name}-vpce-ec2messages" })
}

###############################################################################
# IAM Role for SSM (Bastion + Injector 공용)
###############################################################################
resource "aws_iam_role" "ssm_role" {
  name = "${var.project_name}-ssm-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "ssm_managed_policy" {
  role       = aws_iam_role.ssm_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "s3_read_policy" {
  role       = aws_iam_role.ssm_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_iam_instance_profile" "ssm_profile" {
  name = "${var.project_name}-ssm-profile"
  role = aws_iam_role.ssm_role.name

  tags = local.tags
}

###############################################################################
# Security Groups
###############################################################################

# Bastion Host (Private Subnet, SSM 접속 - SSH 인바운드 불필요)
resource "aws_security_group" "bastion" {
  name_prefix = "${var.project_name}-bastion-"
  description = "Bastion host - SSM access only"
  vpc_id      = data.aws_vpc.existing.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${var.project_name}-sg-bastion" })
}

# Data Injector EC2 (Private Subnet, SSM 접속)
resource "aws_security_group" "injector" {
  name_prefix = "${var.project_name}-injector-"
  description = "Data injector - SSM access only"
  vpc_id      = data.aws_vpc.existing.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${var.project_name}-sg-injector" })
}

# Aurora MySQL
resource "aws_security_group" "aurora" {
  name_prefix = "${var.project_name}-aurora-"
  description = "Aurora MySQL - access from bastion and injector"
  vpc_id      = data.aws_vpc.existing.id

  ingress {
    description     = "MySQL from bastion"
    from_port       = 3306
    to_port         = 3306
    protocol        = "tcp"
    security_groups = [aws_security_group.bastion.id]
  }

  ingress {
    description     = "MySQL from injector"
    from_port       = 3306
    to_port         = 3306
    protocol        = "tcp"
    security_groups = [aws_security_group.injector.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${var.project_name}-sg-aurora" })
}

# OpenSearch
resource "aws_security_group" "opensearch" {
  name_prefix = "${var.project_name}-opensearch-"
  description = "OpenSearch - access from bastion and injector"
  vpc_id      = data.aws_vpc.existing.id

  ingress {
    description     = "HTTPS from bastion"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.bastion.id]
  }

  ingress {
    description     = "HTTPS from injector"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.injector.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${var.project_name}-sg-opensearch" })
}

###############################################################################
# EC2 - Bastion Host (Private Subnet + SSM)
###############################################################################
resource "aws_instance" "bastion" {
  ami                    = data.aws_ami.amazon_linux_2.id
  instance_type          = var.bastion_instance_type
  subnet_id              = var.private_subnet_ids[0]
  vpc_security_group_ids = [aws_security_group.bastion.id]
  iam_instance_profile   = aws_iam_instance_profile.ssm_profile.name

  user_data = <<-EOF
    #!/bin/bash
    yum update -y
    yum install -y python3 python3-pip mysql
    pip3 install pymysql opensearch-py boto3
  EOF

  tags = merge(local.tags, { Name = "${var.project_name}-bastion" })
}

###############################################################################
# EC2 - Data Injector (Private Subnet + SSM)
###############################################################################
resource "aws_instance" "injector" {
  ami                    = data.aws_ami.amazon_linux_2.id
  instance_type          = var.injector_instance_type
  subnet_id              = var.private_subnet_ids[0]
  vpc_security_group_ids = [aws_security_group.injector.id]
  iam_instance_profile   = aws_iam_instance_profile.ssm_profile.name

  root_block_device {
    volume_size = 50
    volume_type = "gp3"
  }

  user_data = <<-EOF
    #!/bin/bash
    yum update -y
    yum install -y python3 python3-pip mysql
    pip3 install pymysql opensearch-py faker boto3
  EOF

  tags = merge(local.tags, { Name = "${var.project_name}-data-injector" })
}

###############################################################################
# Aurora MySQL
###############################################################################
resource "aws_db_subnet_group" "aurora" {
  name       = "${var.project_name}-aurora-subnet"
  subnet_ids = var.private_subnet_ids

  tags = merge(local.tags, { Name = "${var.project_name}-aurora-subnet" })
}

resource "aws_rds_cluster" "aurora" {
  cluster_identifier     = "${var.project_name}-aurora"
  engine                 = "aurora-mysql"
  engine_version         = "8.0.mysql_aurora.3.08.0"
  database_name          = "poc_tmoney"
  master_username        = var.aurora_master_username
  master_password        = var.aurora_master_password
  db_subnet_group_name   = aws_db_subnet_group.aurora.name
  vpc_security_group_ids = [aws_security_group.aurora.id]
  skip_final_snapshot    = true

  tags = merge(local.tags, { Name = "${var.project_name}-aurora" })
}

resource "aws_rds_cluster_instance" "aurora_writer" {
  identifier         = "${var.project_name}-aurora-writer"
  cluster_identifier = aws_rds_cluster.aurora.id
  instance_class     = var.aurora_instance_class
  engine             = aws_rds_cluster.aurora.engine
  engine_version     = aws_rds_cluster.aurora.engine_version

  tags = merge(local.tags, { Name = "${var.project_name}-aurora-writer" })
}

###############################################################################
# OpenSearch
###############################################################################
resource "aws_opensearch_domain" "main" {
  domain_name    = "${var.project_name}-os"
  engine_version = "OpenSearch_2.13"

  cluster_config {
    instance_type          = var.opensearch_instance_type
    instance_count         = var.opensearch_instance_count
    zone_awareness_enabled = true

    zone_awareness_config {
      availability_zone_count = 2
    }
  }

  ebs_options {
    ebs_enabled = true
    volume_size = 100
    volume_type = "gp3"
  }

  vpc_options {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.opensearch.id]
  }

  advanced_security_options {
    enabled                        = true
    internal_user_database_enabled = true

    master_user_options {
      master_user_name     = var.opensearch_master_user
      master_user_password = var.opensearch_master_password
    }
  }

  node_to_node_encryption {
    enabled = true
  }

  encrypt_at_rest {
    enabled = true
  }

  domain_endpoint_options {
    enforce_https       = true
    tls_security_policy = "Policy-Min-TLS-1-2-2019-07"
  }

  tags = merge(local.tags, { Name = "${var.project_name}-opensearch" })
}

data "aws_iam_policy_document" "opensearch_access" {
  statement {
    effect    = "Allow"
    actions   = ["es:*"]
    resources = ["${aws_opensearch_domain.main.arn}/*"]

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }
  }
}

resource "aws_opensearch_domain_policy" "main" {
  domain_name     = aws_opensearch_domain.main.domain_name
  access_policies = data.aws_iam_policy_document.opensearch_access.json
}
