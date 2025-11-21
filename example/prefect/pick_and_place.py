#!/usr/bin/env python3
# Software License Agreement (BSD License)
#
# Copyright (c) 2025, UFACTORY, Inc.
# All rights reserved.
#
# Author: Vinman <vinman.wen@ufactory.cc> <vinman.cub@gmail.com>

"""
Example: Simple pick-and-place flow using Prefect decorators

This example demonstrates workflow orchestration with Prefect by decorating
xArm SDK methods directly. The methods in XArmAPI are decorated with @task
when Prefect is installed, enabling automatic logging and monitoring.

Requirements:
    pip install prefect

Usage:
    python pick_and_place.py 192.168.1.113
"""

import os
import sys
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from prefect import flow
from xarm.wrapper import XArmAPI


@flow(name="pick-and-place")
def pick_and_place_flow(ip: str):
    """
    Simple pick-and-place flow with hardcoded small movements.
    
    Args:
        ip: IP address of the xArm robot
    """
    # Initialize robot
    arm = XArmAPI(ip, do_not_open=True)
    arm.connect()
    
    try:
        # Initialize robot state
        arm.motion_enable(enable=True)
        arm.set_mode(0)
        arm.set_state(0)
        time.sleep(0.5)
        
        # Move to home position
        arm.move_gohome(wait=True)
        time.sleep(0.5)
        
        # Enable and open gripper
        arm.set_gripper_enable(True)
        time.sleep(0.5)
        arm.set_gripper_position(850, wait=True, speed=5000)  # Open
        time.sleep(0.5)
        
        # Move to pick position (small relative movement from home)
        code, home_pos = arm.get_position()
        pick_pos = [home_pos[0] + 50, home_pos[1], home_pos[2] - 50,  # 5cm forward, 5cm down
                    home_pos[3], home_pos[4], home_pos[5]]
        arm.set_position(*pick_pos, wait=True, speed=100)
        time.sleep(0.5)
        
        # Close gripper to grasp
        arm.set_gripper_position(400, wait=True, speed=5000)  # Close
        time.sleep(0.5)
        
        # Lift up
        lift_pos = [pick_pos[0], pick_pos[1], pick_pos[2] + 100,  # 10cm up
                    pick_pos[3], pick_pos[4], pick_pos[5]]
        arm.set_position(*lift_pos, wait=True, speed=100)
        time.sleep(0.5)
        
        # Move to place position (small horizontal movement)
        place_pos = [lift_pos[0], lift_pos[1] + 100, lift_pos[2],  # 10cm to the side
                     lift_pos[3], lift_pos[4], lift_pos[5]]
        arm.set_position(*place_pos, wait=True, speed=100)
        time.sleep(0.5)
        
        # Lower to place
        lower_pos = [place_pos[0], place_pos[1], place_pos[2] - 50,  # 5cm down
                     place_pos[3], place_pos[4], place_pos[5]]
        arm.set_position(*lower_pos, wait=True, speed=100)
        time.sleep(0.5)
        
        # Release gripper
        arm.set_gripper_position(850, wait=True, speed=5000)  # Open
        time.sleep(0.5)
        
        # Return to home
        arm.move_gohome(wait=True)
        
        print("✅ Pick-and-place completed successfully!")
        
    except Exception as e:
        print(f"❌ Flow failed: {e}")
        raise
    finally:
        arm.disconnect()


if __name__ == "__main__":
    # Get IP address from command line
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
    
    pick_and_place_flow(ip)
