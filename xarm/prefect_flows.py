#!/usr/bin/env python3
# Software License Agreement (BSD License)
#
# Copyright (c) 2025, UFACTORY, Inc.
# All rights reserved.
#
# Author: Vinman <vinman.wen@ufactory.cc> <vinman.cub@gmail.com>

"""
Prefect flow decorators for xArm robotic arm control.

Provides workflow orchestration using Prefect for common xArm operations.
This module wraps frequently used xArm API methods as Prefect tasks to enable
workflow orchestration, logging, and monitoring.

Usage:
    from xarm.prefect_flows import (
        get_position_task,
        set_position_task,
        move_gohome_task
    )
    from xarm.wrapper import XArmAPI
    from prefect import flow

    @flow
    def my_robot_workflow():
        arm = XArmAPI('192.168.1.113')
        arm.connect()
        arm.motion_enable(enable=True)
        arm.set_mode(0)
        arm.set_state(0)
        
        # Use Prefect tasks for operations
        code, position = get_position_task(arm)
        move_gohome_task(arm, wait=True)
        
        arm.disconnect()
"""

from prefect import task
from prefect.logging import get_run_logger


# =============================================================================
# POSITION CONTROL TASKS
# =============================================================================

@task
def get_position_task(arm, is_radian=None):
    """
    Prefect task: Get the current cartesian position of the robot arm.
    
    Args:
        arm: XArmAPI instance
        is_radian: Return roll/pitch/yaw in radians (default: arm.default_is_radian)
    
    Returns:
        tuple: (code, [x, y, z, roll, pitch, yaw])
    """
    logger = get_run_logger()
    logger.info("Getting current cartesian position")
    code, position = arm.get_position(is_radian=is_radian)
    if code == 0:
        logger.info(f"Position retrieved: {position}")
    else:
        logger.error(f"Failed to get position, code: {code}")
    return code, position


@task
def set_position_task(arm, x=None, y=None, z=None, roll=None, pitch=None, yaw=None,
                     radius=None, speed=None, mvacc=None, mvtime=None, relative=False,
                     is_radian=None, wait=False, timeout=None, **kwargs):
    """
    Prefect task: Set the cartesian position of the robot arm.
    
    Args:
        arm: XArmAPI instance
        x, y, z: Cartesian coordinates (mm)
        roll, pitch, yaw: Orientation angles (degrees or radians)
        radius: Move radius for arc motion (None for linear)
        speed: Move speed (mm/s)
        mvacc: Move acceleration (mm/s^2)
        mvtime: Reserved
        relative: Relative move or absolute
        is_radian: Input angles in radians (default: arm.default_is_radian)
        wait: Wait for completion
        timeout: Maximum waiting time (seconds)
    
    Returns:
        int: Status code
    """
    logger = get_run_logger()
    logger.info(f"Setting position: x={x}, y={y}, z={z}, roll={roll}, pitch={pitch}, yaw={yaw}")
    code = arm.set_position(x=x, y=y, z=z, roll=roll, pitch=pitch, yaw=yaw,
                           radius=radius, speed=speed, mvacc=mvacc, mvtime=mvtime,
                           relative=relative, is_radian=is_radian, wait=wait,
                           timeout=timeout, **kwargs)
    if code == 0:
        logger.info("Position set successfully")
    else:
        logger.error(f"Failed to set position, code: {code}")
    return code


# =============================================================================
# JOINT CONTROL TASKS
# =============================================================================

@task
def get_servo_angle_task(arm, servo_id=None, is_radian=None, is_real=False):
    """
    Prefect task: Get the current joint angles of the robot arm.
    
    Args:
        arm: XArmAPI instance
        servo_id: Specific joint ID (1-7) or None for all joints
        is_radian: Return angles in radians (default: arm.default_is_radian)
        is_real: Get real-time angles from servos
    
    Returns:
        tuple: (code, angles) or (code, angle) if servo_id specified
    """
    logger = get_run_logger()
    logger.info(f"Getting joint angles (servo_id={servo_id})")
    code, angles = arm.get_servo_angle(servo_id=servo_id, is_radian=is_radian, is_real=is_real)
    if code == 0:
        logger.info(f"Joint angles retrieved: {angles}")
    else:
        logger.error(f"Failed to get joint angles, code: {code}")
    return code, angles


@task
def set_servo_angle_task(arm, servo_id=None, angle=None, speed=None, mvacc=None,
                        mvtime=None, relative=False, is_radian=None, wait=False,
                        timeout=None, radius=None, **kwargs):
    """
    Prefect task: Set joint angles of the robot arm.
    
    Args:
        arm: XArmAPI instance
        servo_id: Joint ID (1-7) or None for all joints
        angle: Target angle(s) - single value or list for all joints
        speed: Move speed (°/s or rad/s)
        mvacc: Move acceleration (°/s^2 or rad/s^2)
        mvtime: Reserved
        relative: Relative move or absolute
        is_radian: Input angles in radians (default: arm.default_is_radian)
        wait: Wait for completion
        timeout: Maximum waiting time (seconds)
        radius: Move radius for arc joint motion
    
    Returns:
        int: Status code
    """
    logger = get_run_logger()
    logger.info(f"Setting joint angles: servo_id={servo_id}, angle={angle}")
    code = arm.set_servo_angle(servo_id=servo_id, angle=angle, speed=speed,
                              mvacc=mvacc, mvtime=mvtime, relative=relative,
                              is_radian=is_radian, wait=wait, timeout=timeout,
                              radius=radius, **kwargs)
    if code == 0:
        logger.info("Joint angles set successfully")
    else:
        logger.error(f"Failed to set joint angles, code: {code}")
    return code


# =============================================================================
# HOMING TASK
# =============================================================================

@task
def move_gohome_task(arm, speed=None, mvacc=None, mvtime=None, is_radian=None,
                    wait=False, timeout=None, **kwargs):
    """
    Prefect task: Move robot arm to home position (zero position).
    
    Args:
        arm: XArmAPI instance
        speed: Move speed (°/s or rad/s, default 50 °/s)
        mvacc: Move acceleration (°/s^2 or rad/s^2, default 5000 °/s^2)
        mvtime: Reserved
        is_radian: Speed/acceleration in radians (default: arm.default_is_radian)
        wait: Wait for completion
        timeout: Maximum waiting time (seconds)
    
    Returns:
        int: Status code
    """
    logger = get_run_logger()
    logger.info("Moving to home position")
    code = arm.move_gohome(speed=speed, mvacc=mvacc, mvtime=mvtime,
                          is_radian=is_radian, wait=wait, timeout=timeout, **kwargs)
    if code == 0:
        logger.info("Successfully moved to home position")
    else:
        logger.error(f"Failed to move to home, code: {code}")
    return code


# =============================================================================
# STATE MANAGEMENT TASKS
# =============================================================================

@task
def get_state_task(arm):
    """
    Prefect task: Get the current state of the robot arm.
    
    Args:
        arm: XArmAPI instance
    
    Returns:
        tuple: (code, state)
            state: 1=in motion, 2=sleeping, 3=suspended, 4=stopping
    """
    logger = get_run_logger()
    logger.info("Getting robot state")
    code, state = arm.get_state()
    if code == 0:
        state_names = {1: "in motion", 2: "sleeping", 3: "suspended", 4: "stopping"}
        state_name = state_names.get(state, f"unknown({state})")
        logger.info(f"Robot state: {state_name}")
    else:
        logger.error(f"Failed to get state, code: {code}")
    return code, state


@task
def set_state_task(arm, state=0):
    """
    Prefect task: Set the state of the robot arm.
    
    Args:
        arm: XArmAPI instance
        state: Target state (0=motion, 3=pause, 4=stop, 6=deceleration stop)
    
    Returns:
        int: Status code
    """
    logger = get_run_logger()
    state_names = {0: "motion", 3: "pause", 4: "stop", 6: "deceleration stop"}
    state_name = state_names.get(state, f"state {state}")
    logger.info(f"Setting robot state to: {state_name}")
    code = arm.set_state(state=state)
    if code == 0:
        logger.info(f"State set to {state_name} successfully")
    else:
        logger.error(f"Failed to set state, code: {code}")
    return code


@task
def set_mode_task(arm, mode=0, detection_param=0):
    """
    Prefect task: Set the mode of the robot arm.
    
    Args:
        arm: XArmAPI instance
        mode: Target mode (0=position control, 1=servo motion, etc.)
        detection_param: Detection parameter
    
    Returns:
        int: Status code
    """
    logger = get_run_logger()
    mode_names = {0: "position control", 1: "servo motion"}
    mode_name = mode_names.get(mode, f"mode {mode}")
    logger.info(f"Setting robot mode to: {mode_name}")
    code = arm.set_mode(mode=mode, detection_param=detection_param)
    if code == 0:
        logger.info(f"Mode set to {mode_name} successfully")
    else:
        logger.error(f"Failed to set mode, code: {code}")
    return code


# =============================================================================
# COLLISION SENSITIVITY TASK
# =============================================================================

@task
def set_collision_sensitivity_task(arm, value, wait=True):
    """
    Prefect task: Set the collision sensitivity of the robot arm.
    
    Args:
        arm: XArmAPI instance
        value: Sensitivity value (0-5, higher is more sensitive)
        wait: Reserved
    
    Returns:
        int: Status code
    """
    logger = get_run_logger()
    logger.info(f"Setting collision sensitivity to: {value}")
    code = arm.set_collision_sensitivity(value, wait=wait)
    if code == 0:
        logger.info("Collision sensitivity set successfully")
    else:
        logger.error(f"Failed to set collision sensitivity, code: {code}")
    return code


# =============================================================================
# MOTION ENABLE/DISABLE TASK
# =============================================================================

@task
def motion_enable_task(arm, enable=True, servo_id=None):
    """
    Prefect task: Enable or disable motion for the robot arm.
    
    Args:
        arm: XArmAPI instance
        enable: True to enable, False to disable
        servo_id: Specific joint ID or None for all joints
    
    Returns:
        int: Status code
    """
    logger = get_run_logger()
    action = "Enabling" if enable else "Disabling"
    logger.info(f"{action} motion (servo_id={servo_id})")
    code = arm.motion_enable(enable=enable, servo_id=servo_id)
    if code == 0:
        logger.info(f"Motion {'enabled' if enable else 'disabled'} successfully")
    else:
        logger.error(f"Failed to {'enable' if enable else 'disable'} motion, code: {code}")
    return code


# =============================================================================
# ERROR/WARNING MANAGEMENT TASKS
# =============================================================================

@task
def clean_error_task(arm):
    """
    Prefect task: Clear error state of the robot arm.
    
    Args:
        arm: XArmAPI instance
    
    Returns:
        int: Status code
    """
    logger = get_run_logger()
    logger.info("Clearing error state")
    code = arm.clean_error()
    if code == 0:
        logger.info("Error cleared successfully")
    else:
        logger.error(f"Failed to clear error, code: {code}")
    return code


@task
def clean_warn_task(arm):
    """
    Prefect task: Clear warning state of the robot arm.
    
    Args:
        arm: XArmAPI instance
    
    Returns:
        int: Status code
    """
    logger = get_run_logger()
    logger.info("Clearing warning state")
    code = arm.clean_warn()
    if code == 0:
        logger.info("Warning cleared successfully")
    else:
        logger.error(f"Failed to clear warning, code: {code}")
    return code


@task
def get_err_warn_code_task(arm):
    """
    Prefect task: Get current error and warning codes.
    
    Args:
        arm: XArmAPI instance
    
    Returns:
        tuple: (code, [error_code, warn_code])
    """
    logger = get_run_logger()
    logger.info("Getting error and warning codes")
    code, err_warn = arm.get_err_warn_code()
    if code == 0:
        logger.info(f"Error code: {err_warn[0]}, Warning code: {err_warn[1]}")
    else:
        logger.error(f"Failed to get error/warning codes, code: {code}")
    return code, err_warn
