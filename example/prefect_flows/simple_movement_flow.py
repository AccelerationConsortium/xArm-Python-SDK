#!/usr/bin/env python3
# Software License Agreement (BSD License)
#
# Copyright (c) 2025, UFACTORY, Inc.
# All rights reserved.
#
# Author: Vinman <vinman.wen@ufactory.cc> <vinman.cub@gmail.com>

"""
Example: Simple movement flow using Prefect decorators

This example demonstrates how to use Prefect task decorators with xArm SDK
for workflow orchestration, logging, and monitoring.

Requirements:
    pip install xarm-python-sdk[prefect]

Usage:
    python simple_movement_flow.py 192.168.1.113
"""

import os
import sys
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from prefect import flow
from prefect.logging import get_run_logger

from xarm.wrapper import XArmAPI
from xarm.prefect_flows import (
    get_position_task,
    set_position_task,
    get_servo_angle_task,
    set_servo_angle_task,
    move_gohome_task,
    get_state_task,
    set_state_task,
    set_mode_task,
    motion_enable_task,
    set_collision_sensitivity_task,
)


@flow(name="simple-movement-flow")
def simple_movement_flow(ip: str):
    """
    Simple movement flow that demonstrates basic xArm operations with Prefect.
    
    Args:
        ip: IP address of the xArm robot
    """
    logger = get_run_logger()
    logger.info(f"=== SIMPLE MOVEMENT FLOW ===")
    logger.info(f"Connecting to xArm at {ip}")
    
    # Initialize robot connection
    arm = XArmAPI(ip, do_not_open=True)
    arm.connect()
    
    try:
        # Initialize robot state using Prefect tasks
        logger.info("Initializing robot state...")
        motion_enable_task(arm, enable=True)
        set_mode_task(arm, mode=0)  # Position control mode
        set_state_task(arm, state=0)  # Motion state
        
        # Set collision sensitivity
        set_collision_sensitivity_task(arm, value=3)
        
        # Get current state and position
        code, state = get_state_task(arm)
        code, position = get_position_task(arm)
        code, angles = get_servo_angle_task(arm)
        
        logger.info(f"Initial position: {position}")
        logger.info(f"Initial joint angles: {angles}")
        
        # Move to home position
        logger.info("Moving to home position...")
        move_gohome_task(arm, wait=True)
        
        # Wait a bit
        time.sleep(1)
        
        # Get position after homing
        code, home_position = get_position_task(arm)
        logger.info(f"Home position: {home_position}")
        
        # Move to a specific position
        logger.info("Moving to target position...")
        set_position_task(arm, x=300, y=0, z=200, roll=180, pitch=0, yaw=0, 
                         speed=100, wait=True)
        
        # Wait a bit
        time.sleep(1)
        
        # Get final position
        code, final_position = get_position_task(arm)
        logger.info(f"Final position: {final_position}")
        
        # Return to home
        logger.info("Returning to home position...")
        move_gohome_task(arm, wait=True)
        
        logger.info("✅ Flow completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Flow failed: {e}")
        raise
    finally:
        # Disconnect
        arm.disconnect()
        logger.info("Disconnected from robot")


@flow(name="joint-movement-flow")
def joint_movement_flow(ip: str):
    """
    Joint movement flow that demonstrates joint angle control with Prefect.
    
    Args:
        ip: IP address of the xArm robot
    """
    logger = get_run_logger()
    logger.info(f"=== JOINT MOVEMENT FLOW ===")
    logger.info(f"Connecting to xArm at {ip}")
    
    # Initialize robot connection
    arm = XArmAPI(ip, do_not_open=True)
    arm.connect()
    
    try:
        # Initialize robot state
        logger.info("Initializing robot state...")
        motion_enable_task(arm, enable=True)
        set_mode_task(arm, mode=0)
        set_state_task(arm, state=0)
        
        # Get current joint angles
        code, initial_angles = get_servo_angle_task(arm)
        logger.info(f"Initial joint angles: {initial_angles}")
        
        # Move to home position
        logger.info("Moving to home position...")
        move_gohome_task(arm, wait=True)
        time.sleep(1)
        
        # Move specific joints
        logger.info("Moving joint 1 to 30 degrees...")
        set_servo_angle_task(arm, servo_id=1, angle=30, speed=50, wait=True)
        time.sleep(1)
        
        # Get updated angles
        code, updated_angles = get_servo_angle_task(arm)
        logger.info(f"Updated joint angles: {updated_angles}")
        
        # Move all joints to specific positions
        logger.info("Moving all joints to target positions...")
        target_angles = [0, -30, 0, 30, 0, 0, 0]
        set_servo_angle_task(arm, angle=target_angles, speed=50, wait=True)
        time.sleep(1)
        
        # Get final angles
        code, final_angles = get_servo_angle_task(arm)
        logger.info(f"Final joint angles: {final_angles}")
        
        # Return to home
        logger.info("Returning to home position...")
        move_gohome_task(arm, wait=True)
        
        logger.info("✅ Flow completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Flow failed: {e}")
        raise
    finally:
        # Disconnect
        arm.disconnect()
        logger.info("Disconnected from robot")


if __name__ == "__main__":
    # Get IP address from command line or config
    if len(sys.argv) >= 2:
        ip = sys.argv[1]
    else:
        try:
            from configparser import ConfigParser
            parser = ConfigParser()
            parser.read('../robot.conf')
            ip = parser.get('xArm', 'ip')
        except:
            ip = input('Please input the xArm ip address: ')
            if not ip:
                print('Input error, exit')
                sys.exit(1)
    
    # Select which flow to run
    if len(sys.argv) >= 3 and sys.argv[2] == 'joint':
        joint_movement_flow(ip)
    else:
        simple_movement_flow(ip)
