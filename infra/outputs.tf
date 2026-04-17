output "vpc_id" {
  description = "VPC ID"
  value       = data.aws_vpc.existing.id
}

output "bastion_instance_id" {
  description = "Bastion host instance ID (use with: aws ssm start-session --target <id>)"
  value       = aws_instance.bastion.id
}

output "injector_instance_id" {
  description = "Data injector instance ID (use with: aws ssm start-session --target <id>)"
  value       = aws_instance.injector.id
}

output "aurora_endpoint" {
  description = "Aurora MySQL writer endpoint"
  value       = aws_rds_cluster.aurora.endpoint
}

output "opensearch_endpoint" {
  description = "OpenSearch domain endpoint"
  value       = aws_opensearch_domain.main.endpoint
}

output "opensearch_dashboard_endpoint" {
  description = "OpenSearch Dashboards endpoint"
  value       = aws_opensearch_domain.main.dashboard_endpoint
}

output "ssm_connect_bastion" {
  description = "Command to connect to Bastion via SSM"
  value       = "aws ssm start-session --target ${aws_instance.bastion.id} --profile kakaopay"
}

output "ssm_connect_injector" {
  description = "Command to connect to Data Injector via SSM"
  value       = "aws ssm start-session --target ${aws_instance.injector.id} --profile kakaopay"
}

output "ssm_port_forward_dashboards" {
  description = "Command to port-forward OpenSearch Dashboards via Bastion"
  value       = "aws ssm start-session --target ${aws_instance.bastion.id} --profile kakaopay --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters '{\"host\":[\"${aws_opensearch_domain.main.endpoint}\"],\"portNumber\":[\"443\"],\"localPortNumber\":[\"8443\"]}'"
}
