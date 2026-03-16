# Outbound task progress from mission_executor → mqtt_bridge → cloud
# ROS2 topic: /mqtt_bridge/in/task_status  (published by mission_executor)
# MQTT topic: drone/{tenant_id}/{drone_id}/task_status  (QoS 1)

# task_status values
string TASK_RECEIVED="RECEIVED"
string TASK_EXECUTING="EXECUTING"
string TASK_PAUSED="PAUSED"
string TASK_COMPLETED="COMPLETED"
string TASK_FAILED="FAILED"
string TASK_ABORTED="ABORTED"

# command_status values
string CMD_PENDING="PENDING"
string CMD_IN_PROGRESS="IN_PROGRESS"
string CMD_COMPLETED="COMPLETED"
string CMD_FAILED="FAILED"

string task_id
string task_status
uint32 current_sequence
string current_command_id
string command_status
uint32 waypoints_total
uint32 waypoints_done
string abort_reason        # empty string if no abort reason
