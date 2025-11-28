#!/usr/bin/env python3
# Software License Agreement (BSD License)
#
# Copyright (c) 2022, UFACTORY, Inc.
# All rights reserved.
#
# Author: Vinman <vinman.wen@ufactory.cc> <vinman.cub@gmail.com>
# Modified for Prefect workflow orchestration

"""
Prefect-enabled pick and place workflow for xArm5 robot
Based on the working simple_pick_and_place.py script
"""

import sys
import math
import time
import queue
import datetime
import random
import traceback
import threading
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NONE as NO_CACHE
from typing import Optional, Tuple, List, Dict, Any
from dataclasses import dataclass
from datetime import datetime
from xarm import version
from xarm.wrapper import XArmAPI


@dataclass
class RobotStatus:
    """Cacheable robot status information"""
    state: int
    error_code: int
    connected: bool
    timestamp: datetime
    position: Optional[List[float]] = None
    joint_angles: Optional[List[float]] = None
    
    @property
    def is_safe(self) -> bool:
        return self.connected and self.error_code == 0 and self.state < 4


@dataclass
class MovementResult:
    """Cacheable movement operation result"""
    success: bool
    start_position: List[float]
    target_position: List[float]
    execution_time: float
    timestamp: datetime
    error_code: Optional[int] = None
    error_message: Optional[str] = None


@dataclass
class WorkflowMetrics:
    """Cacheable workflow execution metrics"""
    cycles_completed: int
    total_movements: int
    total_execution_time: float
    pick_operations: int
    place_operations: int
    errors_encountered: int
    start_time: datetime
    end_time: datetime


# Robot connection management (non-cacheable)
@task(cache_policy=NO_CACHE)
def initialize_robot(robot_ip: str = '192.168.1.231') -> XArmAPI:
    """Initialize and connect to the xArm robot"""
    logger = get_run_logger()
    logger.info(f"Initializing robot connection to {robot_ip}")
    logger.info(f'xArm-Python-SDK Version: {version.__version__}')
    
    arm = XArmAPI(robot_ip, baud_checkset=False)
    
    # Set up error and state callbacks
    def error_warn_changed_callback(data):
        if data and data['error_code'] != 0:
            logger.warning(f'ErrorCode: {data["error_code"]}, WarnCode: {data["warn_code"]}')
    
    def state_changed_callback(data):
        if data and data['state'] == 4:
            logger.warning('State changed to 4 (Error)')
    
    arm.register_error_warn_changed_callback(error_warn_changed_callback)
    arm.register_state_changed_callback(state_changed_callback)
    
    # Initial setup
    arm.clean_warn()
    arm.clean_error()
    arm.motion_enable(True)
    arm.set_mode(0)
    arm.set_state(0)
    
    # Wait for robot to be ready
    time.sleep(1)
    
    logger.info("Robot initialized successfully")
    return arm


# Robot status monitoring (cacheable data)
@task(cache_policy=NO_CACHE)
def get_robot_status(arm: XArmAPI) -> RobotStatus:
    """Get current robot status as cacheable data"""
    logger = get_run_logger()
    
    if arm.connected and arm.error_code == 0:
        if arm.state == 5:
            cnt = 0
            while arm.state == 5 and cnt < 5:
                cnt += 1
                time.sleep(0.1)
    
    # Get current position and joint angles for data tracking
    try:
        position = arm.get_position()[1][:6] if arm.connected else None
        joint_angles = arm.get_servo_angle()[1] if arm.connected else None
    except:
        position = None
        joint_angles = None
    
    status = RobotStatus(
        state=arm.state,
        error_code=arm.error_code,
        connected=arm.connected,
        timestamp=datetime.now(),
        position=position,
        joint_angles=joint_angles
    )
    
    logger.info(f"Robot status - State: {status.state}, Error: {status.error_code}, Safe: {status.is_safe}")
    return status


@task(cache_policy=NO_CACHE)
def check_robot_safety(status: RobotStatus) -> bool:
    """Validate robot safety based on status"""
    logger = get_run_logger()
    
    if not status.is_safe:
        logger.error(f"Robot safety check failed - State: {status.state}, Error: {status.error_code}")
    
    return status.is_safe


@task(cache_policy=NO_CACHE)
def execute_movement(arm: XArmAPI, target_position: List[float], tcp_speed: int = 20, tcp_acc: int = 100) -> MovementResult:
    """Execute robot movement and return cacheable result data"""
    logger = get_run_logger()
    logger.info(f"Moving to position: {target_position}")
    
    # Get starting position
    try:
        start_pos = arm.get_position()[1][:6] if arm.connected else [0,0,0,0,0,0]
    except:
        start_pos = [0,0,0,0,0,0]
    
    start_time = time.time()
    code = arm.set_position(*target_position, speed=tcp_speed, mvacc=tcp_acc, radius=-1.0, wait=True)
    execution_time = time.time() - start_time
    
    result = MovementResult(
        success=code == 0,
        start_position=start_pos,
        target_position=target_position,
        execution_time=execution_time,
        timestamp=datetime.now(),
        error_code=code if code != 0 else None,
        error_message=f'set_position failed, code={code}' if code != 0 else None
    )
    
    if result.success:
        logger.info(f"Position reached successfully in {execution_time:.2f}s")
    else:
        logger.error(f"Movement failed: {result.error_message}")
    
    return result


@task(cache_policy=NO_CACHE)
def control_gripper(arm: XArmAPI, position: int, speed: int = 1000) -> Dict[str, Any]:
    """Control gripper and return operation data"""
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
        'timestamp': datetime.now(),
        'error_code': code if code != 0 else None
    }
    
    if result['success']:
        logger.info(f"Gripper operation completed successfully in {execution_time:.2f}s")
    else:
        logger.error(f'set_gripper_position failed, code={code}')
    
    return result


@task(cache_policy=NO_CACHE)
def execute_pick_sequence(arm: XArmAPI) -> Dict[str, Any]:
    """Execute the pick portion of pick-and-place and return metrics"""
    logger = get_run_logger()
    logger.info("Starting pick sequence")
    
    pick_metrics = {
        'sequence_type': 'pick',
        'start_time': datetime.now(),
        'movements': [],
        'gripper_operations': [],
        'success': False,
        'total_time': 0.0
    }
    
    try:
        # Move to pickup position (above)
        pickup_above = [-60.0, 420.0, 320.0, 179.6, 0.1, 89.6]
        move_result = execute_movement(arm, pickup_above)
        pick_metrics['movements'].append(move_result)
        if not move_result.success:
            return pick_metrics
        
        # Move down to pickup position
        pickup_position = [-60.0, 420.0, 290.0, 179.6, 0.1, 89.6]
        move_result = execute_movement(arm, pickup_position)
        pick_metrics['movements'].append(move_result)
        if not move_result.success:
            return pick_metrics
        
        # Close gripper to pick up object
        gripper_result = control_gripper(arm, 240)
        pick_metrics['gripper_operations'].append(gripper_result)
        if not gripper_result['success']:
            return pick_metrics
        
        # Move back up
        move_result = execute_movement(arm, pickup_above)
        pick_metrics['movements'].append(move_result)
        if not move_result.success:
            return pick_metrics
        
        pick_metrics['success'] = True
        pick_metrics['total_time'] = (datetime.now() - pick_metrics['start_time']).total_seconds()
        logger.info(f"Pick sequence completed successfully in {pick_metrics['total_time']:.2f}s")
        
    except Exception as e:
        logger.error(f"Pick sequence failed: {e}")
        pick_metrics['error'] = str(e)
    
    return pick_metrics


@task(cache_policy=NO_CACHE)
def execute_place_sequence(arm: XArmAPI) -> Dict[str, Any]:
    """Execute the place portion of pick-and-place and return metrics"""
    logger = get_run_logger()
    logger.info("Starting place sequence")
    
    place_metrics = {
        'sequence_type': 'place',
        'start_time': datetime.now(),
        'movements': [],
        'gripper_operations': [],
        'success': False,
        'total_time': 0.0
    }
    
    try:
        # Move to drop position (above)
        drop_above = [120.0, 420.0, 320.0, 179.6, 0.1, 89.6]
        move_result = execute_movement(arm, drop_above)
        place_metrics['movements'].append(move_result)
        if not move_result.success:
            return place_metrics
        
        # Move down to drop position
        drop_position = [120.0, 420.0, 290.0, 179.6, 0.1, 89.6]
        move_result = execute_movement(arm, drop_position)
        place_metrics['movements'].append(move_result)
        if not move_result.success:
            return place_metrics
        
        # Open gripper to release object
        gripper_result = control_gripper(arm, 300)
        place_metrics['gripper_operations'].append(gripper_result)
        if not gripper_result['success']:
            return place_metrics
        
        # Move back up
        move_result = execute_movement(arm, drop_above)
        place_metrics['movements'].append(move_result)
        if not move_result.success:
            return place_metrics
        
        place_metrics['success'] = True
        place_metrics['total_time'] = (datetime.now() - place_metrics['start_time']).total_seconds()
        logger.info(f"Place sequence completed successfully in {place_metrics['total_time']:.2f}s")
        
    except Exception as e:
        logger.error(f"Place sequence failed: {e}")
        place_metrics['error'] = str(e)
    
    return place_metrics


@task(cache_policy=NO_CACHE)
def cleanup_robot(arm: XArmAPI):
    """Clean up robot connections and callbacks"""
    logger = get_run_logger()
    logger.info("Cleaning up robot connections")
    
    try:
        arm.release_error_warn_changed_callback()
        arm.release_state_changed_callback()
        arm.disconnect()
        logger.info("Robot cleanup completed")
    except Exception as e:
        logger.warning(f"Cleanup warning: {e}")


@task
def aggregate_workflow_metrics(pick_results: List[Dict], place_results: List[Dict], start_time: datetime) -> WorkflowMetrics:
    """Aggregate all workflow metrics into a cacheable summary"""
    
    total_movements = sum(len(result.get('movements', [])) for result in pick_results + place_results)
    total_exec_time = sum(result.get('total_time', 0) for result in pick_results + place_results)
    pick_ops = len([r for r in pick_results if r.get('success', False)])
    place_ops = len([r for r in place_results if r.get('success', False)])
    errors = len([r for r in pick_results + place_results if not r.get('success', False)])
    
    metrics = WorkflowMetrics(
        cycles_completed=min(len(pick_results), len(place_results)),
        total_movements=total_movements,
        total_execution_time=total_exec_time,
        pick_operations=pick_ops,
        place_operations=place_ops,
        errors_encountered=errors,
        start_time=start_time,
        end_time=datetime.now()
    )
    
    return metrics


@flow(name="xArm Pick and Place Workflow")
def pick_and_place_workflow(robot_ip: str = '192.168.1.231', cycles: int = 1) -> WorkflowMetrics:
    """
    Main Prefect flow for xArm pick and place operations
    
    Args:
        robot_ip: IP address of the xArm robot
        cycles: Number of pick-place cycles to execute
    
    Returns:
        WorkflowMetrics: Comprehensive workflow execution data
    """
    logger = get_run_logger()
    workflow_start = datetime.now()
    logger.info(f"Starting pick and place workflow with {cycles} cycle(s)")
    
    # Initialize robot
    arm = initialize_robot(robot_ip)
    pick_results = []
    place_results = []
    
    try:
        # Check initial robot status
        initial_status = get_robot_status(arm)
        if not check_robot_safety(initial_status):
            logger.error("Robot not ready for operations")
            return aggregate_workflow_metrics([], [], workflow_start)
        
        # Execute pick and place cycles
        for cycle in range(cycles):
            logger.info(f"Executing cycle {cycle + 1} of {cycles}")
            
            # Check status before each cycle
            status = get_robot_status(arm)
            if not check_robot_safety(status):
                logger.error(f"Robot status check failed on cycle {cycle + 1}")
                break
            
            # Execute pick sequence with data collection
            pick_result = execute_pick_sequence(arm)
            pick_results.append(pick_result)
            if not pick_result['success']:
                logger.error(f"Pick sequence failed on cycle {cycle + 1}")
                break
            
            # Execute place sequence with data collection
            place_result = execute_place_sequence(arm)
            place_results.append(place_result)
            if not place_result['success']:
                logger.error(f"Place sequence failed on cycle {cycle + 1}")
                break
            
            logger.info(f"Cycle {cycle + 1} completed successfully")
            
            # Small delay between cycles
            if cycle < cycles - 1:
                time.sleep(0.1)
        
        logger.info("All cycles completed successfully")
        
    except Exception as e:
        logger.error(f"Workflow exception: {e}")
        
    finally:
        # Always clean up
        cleanup_robot(arm)
    
    # Return comprehensive metrics
    return aggregate_workflow_metrics(pick_results, place_results, workflow_start)


if __name__ == '__main__':
    # Run the workflow directly for testing
    metrics = pick_and_place_workflow(cycles=1)
    
    print(f"\n🤖 Pick and Place Workflow Results:")
    print(f"✅ Cycles Completed: {metrics.cycles_completed}")
    print(f"📊 Total Movements: {metrics.total_movements}")
    print(f"⏱️  Total Execution Time: {metrics.total_execution_time:.2f}s")
    print(f"🎯 Pick Operations: {metrics.pick_operations}")
    print(f"📍 Place Operations: {metrics.place_operations}")
    print(f"⚠️  Errors Encountered: {metrics.errors_encountered}")
    print(f"🕐 Duration: {(metrics.end_time - metrics.start_time).total_seconds():.2f}s")
    
    if metrics.cycles_completed > 0:
        print("✅ Pick and place workflow completed successfully!")
    else:
        print("❌ Pick and place workflow failed!")