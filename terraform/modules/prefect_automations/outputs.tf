# Prefect Automations Module Outputs


output "automation_names" {
  description = "Names of all automations created by this module"
  value = [prefect_automation.crash_zombie_flows.name]
}
