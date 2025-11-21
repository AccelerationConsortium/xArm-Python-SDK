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
    # Use default locations (relative to home position)
    python pick_and_place.py 192.168.1.113
    
    # Or specify custom pick and place locations in code
    # See the example in the main block below
"""

import os
import sys
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from prefect import flow
from xarm.wrapper import XArmAPI


@flow(name="pick-and-place")
def pick_and_place_flow(ip: str, object_name: str = "object", 
                        pick_location: list = None, place_location: list = None):
    """
    Simple pick-and-place flow with parameterized pick/place locations and hardcoded movements.
    
    Args:
        ip: IP address of the xArm robot
        object_name: Name/description of the object being picked (for logging)
        pick_location: [x, y, z, roll, pitch, yaw] coordinates for pick location (mm, degrees)
                      If None, uses home position with small offset
        place_location: [x, y, z, roll, pitch, yaw] coordinates for place location (mm, degrees)
                       If None, uses pick location with 100mm horizontal offset
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
        
        # Get current position for reference if locations not provided
        code, home_pos = arm.get_position()
        
        # Set default pick location if not provided (5cm forward, 5cm down from home)
        if pick_location is None:
            pick_location = [home_pos[0] + 50, home_pos[1], home_pos[2] - 50,
                           home_pos[3], home_pos[4], home_pos[5]]
        
        # Set default place location if not provided (10cm to the side from pick)
        if place_location is None:
            place_location = [pick_location[0], pick_location[1] + 100, pick_location[2],
                            pick_location[3], pick_location[4], pick_location[5]]
        
        print(f"🤖 Starting pick-and-place for: {object_name}")
        print(f"   Pick from: {pick_location[:3]}")
        print(f"   Place at: {place_location[:3]}")
        
        # Enable and open gripper
        arm.set_gripper_enable(True)
        time.sleep(0.5)
        arm.set_gripper_position(850, wait=True, speed=5000)  # Open
        time.sleep(0.5)
        
        # Move to pick position (hardcoded movement: approach from above)
        approach_pos = [pick_location[0], pick_location[1], pick_location[2] + 50,  # 5cm above
                       pick_location[3], pick_location[4], pick_location[5]]
        arm.set_position(*approach_pos, wait=True, speed=100)
        time.sleep(0.5)
        
        # Lower to pick position (hardcoded 5cm down)
        arm.set_position(*pick_location, wait=True, speed=100)
        time.sleep(0.5)
        
        # Close gripper to grasp (hardcoded gripper position)
        print(f"✋ Grasping {object_name}")
        arm.set_gripper_position(400, wait=True, speed=5000)  # Close
        time.sleep(0.5)
        
        # Lift up (hardcoded 10cm lift)
        lift_pos = [pick_location[0], pick_location[1], pick_location[2] + 100,
                   pick_location[3], pick_location[4], pick_location[5]]
        arm.set_position(*lift_pos, wait=True, speed=100)
        time.sleep(0.5)
        
        # Move to place location (keeping same height)
        place_approach = [place_location[0], place_location[1], lift_pos[2],
                         place_location[3], place_location[4], place_location[5]]
        arm.set_position(*place_approach, wait=True, speed=100)
        time.sleep(0.5)
        
        # Lower to place position (hardcoded descent)
        arm.set_position(*place_location, wait=True, speed=100)
        time.sleep(0.5)
        
        # Release gripper (hardcoded open position)
        print(f"📦 Placing {object_name}")
        arm.set_gripper_position(850, wait=True, speed=5000)  # Open
        time.sleep(0.5)
        
        # Lift up slightly before returning home (hardcoded 5cm lift)
        retract_pos = [place_location[0], place_location[1], place_location[2] + 50,
                      place_location[3], place_location[4], place_location[5]]
        arm.set_position(*retract_pos, wait=True, speed=100)
        time.sleep(0.5)
        
        # Return to home
        arm.move_gohome(wait=True)
        
        print(f"✅ Pick-and-place of {object_name} completed successfully!")
        
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
    
    # Example 1: Use default locations (relative to home position)
    pick_and_place_flow(ip, object_name="small box")
    
    # Example 2: Specify custom pick and place locations
    # Uncomment to use custom locations (coordinates in mm and degrees)
    # pick_and_place_flow(
    #     ip=ip,
    #     object_name="component A",
    #     pick_location=[300, 0, 200, 180, 0, 0],  # [x, y, z, roll, pitch, yaw]
    #     place_location=[300, 150, 200, 180, 0, 0]
    # )
