# xArm Prefect Flows Examples

This directory contains examples demonstrating how to use Prefect workflow orchestration with the xArm Python SDK.

## Overview

The xArm Python SDK now includes Prefect task decorators for commonly used operations. These decorators enable:

- **Workflow orchestration**: Chain multiple robot operations together with proper dependency management
- **Logging**: Automatic logging of all operations with timestamps and status
- **Monitoring**: Integration with Prefect's monitoring and observability features
- **Error handling**: Built-in error handling and retry capabilities

## Installation

Install the xArm SDK with Prefect support:

```bash
pip install xarm-python-sdk[prefect]
```

Or install Prefect separately:

```bash
pip install xarm-python-sdk
pip install prefect>=2.0.0
```

## Available Prefect Tasks

The `xarm.prefect_flows` module provides the following task decorators:

### Position Control
- `get_position_task`: Get current cartesian position
- `set_position_task`: Set cartesian position

### Joint Control
- `get_servo_angle_task`: Get current joint angles
- `set_servo_angle_task`: Set joint angles

### Motion
- `move_gohome_task`: Move to home/zero position

### State Management
- `get_state_task`: Get robot state
- `set_state_task`: Set robot state
- `set_mode_task`: Set robot mode
- `motion_enable_task`: Enable/disable motion

### Safety
- `set_collision_sensitivity_task`: Set collision sensitivity

### Error Handling
- `clean_error_task`: Clear error state
- `clean_warn_task`: Clear warning state
- `get_err_warn_code_task`: Get error/warning codes

## Examples

### Simple Movement Flow

Demonstrates basic position control with Prefect:

```bash
python simple_movement_flow.py 192.168.1.113
```

This flow:
1. Connects to the robot
2. Initializes robot state
3. Gets current position and angles
4. Moves to home position
5. Moves to a target position
6. Returns to home

### Joint Movement Flow

Demonstrates joint angle control:

```bash
python simple_movement_flow.py 192.168.1.113 joint
```

This flow:
1. Connects to the robot
2. Moves individual joints
3. Moves all joints to target positions
4. Returns to home

## Usage Pattern

Here's a basic pattern for using Prefect tasks with xArm:

```python
from prefect import flow
from xarm.wrapper import XArmAPI
from xarm.prefect_flows import (
    get_position_task,
    set_position_task,
    move_gohome_task,
)

@flow
def my_robot_flow(ip: str):
    # Initialize robot
    arm = XArmAPI(ip, do_not_open=True)
    arm.connect()
    
    try:
        # Enable motion
        arm.motion_enable(enable=True)
        arm.set_mode(0)
        arm.set_state(0)
        
        # Use Prefect tasks for operations
        code, position = get_position_task(arm)
        set_position_task(arm, x=300, y=0, z=200, wait=True)
        move_gohome_task(arm, wait=True)
        
    finally:
        arm.disconnect()

if __name__ == "__main__":
    my_robot_flow("192.168.1.113")
```

## Prefect Features

### Logging

All tasks automatically log their operations:

```python
@flow
def my_flow(ip):
    logger = get_run_logger()
    logger.info("Starting robot operations...")
    
    arm = XArmAPI(ip)
    # Tasks will automatically log their status
    move_gohome_task(arm, wait=True)  # Logs: "Moving to home position"
```

### Deployments

You can deploy flows to run on schedules or triggers:

```python
# Deploy a flow
simple_movement_flow.deploy(
    name="robot-maintenance",
    work_pool_name="default-process-pool",
    schedules=[{"cron": "0 2 * * *"}]  # Run at 2 AM daily
)
```

### Monitoring

View flow runs in the Prefect UI:

```bash
prefect server start
# Navigate to http://localhost:4200
```

## Safety Considerations

⚠️ **Important**: Always follow xArm safety guidelines:

- Keep clear of the robot arm during operation
- Perform safety assessments before movements
- Use appropriate collision sensitivity settings
- Test movements in a safe environment first
- Have emergency stop procedures in place

## Reference

- [xArm Python SDK Documentation](https://github.com/xArm-Developer/xArm-Python-SDK)
- [Prefect Documentation](https://docs.prefect.io/)
- [Progressive Automations Prefect Example](https://github.com/AccelerationConsortium/progressive-automations-python/blob/main/src/progressive_automations_python/prefect_flows.py)
