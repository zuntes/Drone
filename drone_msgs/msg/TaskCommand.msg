# Inbound task from cloud operator
# MQTT topic: drone/{tenant_id}/{drone_id}/task_command  (QoS 1)
# ROS2 topic: /mqtt_bridge/out/task_command

string task_id             # globally unique
string tenant_id           # validated by mqtt_bridge against local config
string drone_id            # validated by mqtt_bridge against local config
string timestamp           # date + time 
string protocol_version    # "1.0"

CommandItem[] payloads     # ordered list, execute by sequence number
