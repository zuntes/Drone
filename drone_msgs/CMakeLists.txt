cmake_minimum_required(VERSION 3.8)
project(drone_msgs)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)
find_package(rosidl_default_generators REQUIRED)
find_package(builtin_interfaces REQUIRED)

set(msg_files
  # Telemetry sub-types
  "msg/FlightMode.msg"
  "msg/Position.msg"
  "msg/Altitude.msg"
  "msg/GpsHealth.msg"
  "msg/Orientation.msg"
  "msg/Velocity.msg"
  "msg/BatteryState.msg"
  # Alarm sub-types
  "msg/FailsafeStatus.msg"
  "msg/BatteryAlarm.msg"
  "msg/GpsAlarm.msg"
  "msg/EkfStatus.msg"
  "msg/LandState.msg"
  # Top-level telemetry
  "msg/TelemetryData.msg"
  "msg/BridgeStatus.msg"
  "msg/AlarmData.msg"
  # Task command / status
  "msg/CommandPayload.msg"
  "msg/CommandItem.msg"
  "msg/TaskCommand.msg"
  "msg/TaskStatus.msg"
)

rosidl_generate_interfaces(${PROJECT_NAME}
  ${msg_files}
  DEPENDENCIES builtin_interfaces
)

ament_export_dependencies(rosidl_default_runtime)
ament_package()
