#!/usr/bin/env python3
"""
Custom Position Workflow for xArm Robot

This script allows you to move the robot to any position by passing in
5 parameters: x, y, z, roll, pitch, yaw through the Prefect UI.

Features:
- Move to any custom position via Prefect UI parameters
- Safety checks and error handling
- Data tracking and metrics
- Gripper control options
"""

import os
import sys
import time
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Any, List, Optional

# Add the xArm SDK to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from xarm.wrapper import XArmAPI


@dataclass
class MovementResult:
    """Data structure for movement operation results"""
    success: bool
    target_position: List[float]
    execution_time: float
    timestamp: datetime
    error_code: Optional[int] = None
    error_message: Optional[str] = None


@dataclass
class WorkflowMetrics:
    """Cacheable data structure for workflow results"""
    target_position: List[float]
    movement_successful: bool
    total_execution_time: float
    gripper_operations: int
    errors_encountered: int
    start_time: datetime
    end_time: datetime


@task(cache_policy=NO_CACHE)
def initialize_robot(ip: str) -> XArmAPI:
    """Initialize robot connection with safety checks"""
    logger = get_run_logger()
    logger.info(f"Connecting to xArm robot at {ip}")
    
    arm = XArmAPI(ip, baud_checkset=False)
    
    # Initialize robot state
    arm.clean_warn()
    arm.clean_error()
    arm.motion_enable(True)
    arm.set_mode(0)
    arm.set_state(0)
    time.sleep(1)
    
    logger.info("Robot initialized successfully")
    return arm


@task(cache_policy=NO_CACHE)
def get_robot_status(arm: XArmAPI) -> Dict[str, Any]:
    """Get current robot status for safety checks"""
    return {
        'connected': arm.connected,
        'state': arm.state,
        'error_code': arm.error_code,
        'position': arm.position,
        'angles': arm.angles
    }


@task
def check_robot_safety(status: Dict[str, Any]) -> bool:
    """Check if robot is in safe state for operations"""
    logger = get_run_logger()
    
    if not status['connected']:
        logger.error("Robot not connected")
        return False
        
    if status['error_code'] != 0:
        logger.error(f"Robot error code: {status['error_code']}")
        return False
        
    if status['state'] >= 4:
        logger.error(f"Robot in error state: {status['state']}")
        return False
    
    logger.info("Robot safety check passed")
    return True


@task(cache_policy=NO_CACHE)
def validate_position(x: float, y: float, z: float, roll: float, pitch: float, yaw: float) -> bool:
    """Validate that the target position is within safe limits"""
    logger = get_run_logger()
    
    # Safety limits for xArm5 (adjust as needed for your setup)
    x_limits = (-700, 700)  # mm
    y_limits = (-700, 700)  # mm
    z_limits = (100, 700)   # mm - CRITICAL: Don't go below 100mm
    roll_limits = (-180, 180)  # degrees
    pitch_limits = (-90, 90)   # degrees
    yaw_limits = (-180, 180)   # degrees
    
    if not (x_limits[0] <= x <= x_limits[1]):
        logger.error(f"X coordinate {x} outside safe range {x_limits}")
        return False
        
    if not (y_limits[0] <= y <= y_limits[1]):
        logger.error(f"Y coordinate {y} outside safe range {y_limits}")
        return False
        
    if not (z_limits[0] <= z <= z_limits[1]):
        logger.error(f"Z coordinate {z} outside safe range {z_limits}")
        return False
        
    if not (roll_limits[0] <= roll <= roll_limits[1]):
        logger.error(f"Roll {roll} outside safe range {roll_limits}")
        return False
        
    if not (pitch_limits[0] <= pitch <= pitch_limits[1]):
        logger.error(f"Pitch {pitch} outside safe range {pitch_limits}")
        return False
        
    if not (yaw_limits[0] <= yaw <= yaw_limits[1]):
        logger.error(f"Yaw {yaw} outside safe range {yaw_limits}")
        return False
    
    logger.info(f"Position validation passed: [{x}, {y}, {z}, {roll}, {pitch}, {yaw}]")
    return True


@task(cache_policy=NO_CACHE)
def move_to_position(arm: XArmAPI, x: float, y: float, z: float, roll: float, pitch: float, yaw: float, speed: int = 100) -> MovementResult:
    """Execute movement to specified position"""
    logger = get_run_logger()
    target_position = [x, y, z, roll, pitch, yaw]
    
    logger.info(f"Moving to position: [{x}, {y}, {z}, {roll}, {pitch}, {yaw}]")
    
    start_time = time.time()
    code = arm.set_position(x, y, z, roll, pitch, yaw, speed=speed, wait=True)
    execution_time = time.time() - start_time
    
    result = MovementResult(
        success=code == 0,
        target_position=target_position,
        execution_time=execution_time,
        timestamp=datetime.now(),
        error_code=code if code != 0 else None,
        error_message=f'Movement failed, code={code}' if code != 0 else None
    )
    
    if result.success:
        logger.info(f"Position reached successfully in {execution_time:.2f}s")
    else:
        logger.error(f"Movement failed: {result.error_message}")
    
    return result


@task(cache_policy=NO_CACHE)
def control_gripper(arm: XArmAPI, position: int, speed: int = 1000) -> Dict[str, Any]:
    """Control gripper position"""
    logger = get_run_logger()
    action = "Opening" if position > 250 else "Closing"
    logger.info(f"{action} gripper to position {position}")
    
    start_time = time.time()
    code = arm.set_gripper_position(position, wait=True, speed=speed, auto_enable=True)
    execution_time = time.time() - start_time
    
    result = {
        'success': code == 0,
        'action': action,
        'position': position,
        'execution_time': execution_time,
        'error_code': code if code != 0 else None
    }
    
    if result['success']:
        logger.info(f"Gripper {action.lower()} completed in {execution_time:.2f}s")
    else:
        logger.error(f'Gripper operation failed, code={code}')
    
    return result


@task(cache_policy=NO_CACHE)
def cleanup_robot(arm: XArmAPI):
    """Clean up robot connections"""
    logger = get_run_logger()
    logger.info("Cleaning up robot connections")
    
    try:
        arm.disconnect()
        logger.info("Robot cleanup completed")
    except Exception as e:
        logger.warning(f"Cleanup warning: {e}")


@task
def aggregate_metrics(movement_result: MovementResult, gripper_results: List[Dict], start_time: datetime) -> WorkflowMetrics:
    """Aggregate workflow metrics into cacheable summary"""
    
    end_time = datetime.now()
    total_execution_time = movement_result.execution_time + sum(gr.get('execution_time', 0) for gr in gripper_results)
    gripper_operations = len(gripper_results)
    errors_encountered = (0 if movement_result.success else 1) + len([gr for gr in gripper_results if not gr.get('success', False)])
    
    return WorkflowMetrics(
        target_position=movement_result.target_position,
        movement_successful=movement_result.success,
        total_execution_time=total_execution_time,
        gripper_operations=gripper_operations,
        errors_encountered=errors_encountered,
        start_time=start_time,
        end_time=end_time
    )


@flow(name="xArm Custom Position Movement")
def custom_position_workflow(
    robot_ip: str = '192.168.1.231',
    # Position parameters (x, y, z in mm, roll/pitch/yaw in degrees)
    x: float = -60.0,
    y: float = 420.0, 
    z: float = 320.0,  # Safe default height
    roll: float = 179.6,
    pitch: float = 0.1,
    yaw: float = 89.6,
    # Movement parameters
    speed: int = 100,
    # Gripper control
    control_gripper_enabled: bool = False,
    gripper_position: int = 300  # 300 = open, 0 = closed
) -> WorkflowMetrics:
    """
    Move xArm robot to custom position specified via Prefect UI
    
    Args:
        robot_ip: IP address of the xArm robot
        x: X coordinate in mm
        y: Y coordinate in mm  
        z: Z coordinate in mm (CAUTION: minimum 100mm for safety)
        roll: Roll angle in degrees
        pitch: Pitch angle in degrees
        yaw: Yaw angle in degrees
        speed: Movement speed (1-200)
        control_gripper_enabled: Whether to control gripper
        gripper_position: Gripper position (0=closed, 850=open)
    
    Returns:
        WorkflowMetrics: Comprehensive workflow execution data
    """
    logger = get_run_logger()
    workflow_start = datetime.now()
    logger.info(f"Starting custom position workflow")
    logger.info(f"Target position: [{x}, {y}, {z}, {roll}, {pitch}, {yaw}]")
    
    # Initialize robot
    arm = initialize_robot(robot_ip)
    gripper_results = []
    
    try:
        # Check initial robot status
        initial_status = get_robot_status(arm)
        if not check_robot_safety(initial_status):
            logger.error("Robot not ready for operations")
            # Return empty metrics on safety failure
            return WorkflowMetrics(
                target_position=[x, y, z, roll, pitch, yaw],
                movement_successful=False,
                total_execution_time=0.0,
                gripper_operations=0,
                errors_encountered=1,
                start_time=workflow_start,
                end_time=datetime.now()
            )
        
        # Validate target position
        if not validate_position(x, y, z, roll, pitch, yaw):
            logger.error("Target position validation failed")
            return WorkflowMetrics(
                target_position=[x, y, z, roll, pitch, yaw],
                movement_successful=False,
                total_execution_time=0.0,
                gripper_operations=0,
                errors_encountered=1,
                start_time=workflow_start,
                end_time=datetime.now()
            )
        
        # Control gripper if requested (before movement)
        if control_gripper_enabled:
            gripper_result = control_gripper(arm, gripper_position)
            gripper_results.append(gripper_result)
            if not gripper_result['success']:
                logger.warning("Gripper operation failed, continuing with movement")
        
        # Execute movement to target position
        movement_result = move_to_position(arm, x, y, z, roll, pitch, yaw, speed)
        
        if movement_result.success:
            logger.info("Custom position movement completed successfully")
        else:
            logger.error("Custom position movement failed")
        
    except Exception as e:
        logger.error(f"Workflow exception: {e}")
        # Create failed movement result
        movement_result = MovementResult(
            success=False,
            target_position=[x, y, z, roll, pitch, yaw],
            execution_time=0.0,
            timestamp=datetime.now(),
            error_message=str(e)
        )
        
    finally:
        # Always clean up
        cleanup_robot(arm)
    
    # Return comprehensive metrics
    return aggregate_metrics(movement_result, gripper_results, workflow_start)


if __name__ == '__main__':
    # Run the workflow directly for testing
    metrics = custom_position_workflow(
        x=-60.0, y=420.0, z=320.0,
        roll=179.6, pitch=0.1, yaw=89.6,
        control_gripper_enabled=True,
        gripper_position=300
    )
    
    print(f"\n🤖 Custom Position Movement Results:")
    print(f"📍 Target Position: {metrics.target_position}")
    print(f"✅ Movement Successful: {metrics.movement_successful}")
    print(f"⏱️  Total Execution Time: {metrics.total_execution_time:.2f}s")
    print(f"🤏 Gripper Operations: {metrics.gripper_operations}")
    print(f"⚠️  Errors Encountered: {metrics.errors_encountered}")
    print(f"🕐 Duration: {(metrics.end_time - metrics.start_time).total_seconds():.2f}s")
    
    if metrics.movement_successful:
        print("✅ Custom position movement completed successfully!")
    else:
        print("❌ Custom position movement failed!")