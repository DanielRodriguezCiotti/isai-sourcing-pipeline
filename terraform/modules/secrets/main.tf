resource "aws_secretsmanager_secret" "main" {
  name        = "${var.name_prefix}-secrets"
  description = "Secrets for ${var.name_prefix}"

  tags = var.tags
}

resource "aws_secretsmanager_secret_version" "main" {
  secret_id     = aws_secretsmanager_secret.main.id
  secret_string = jsonencode(var.secrets)
}
