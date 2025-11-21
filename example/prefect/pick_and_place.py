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


# Hardcoded coordinate mappings for lab equipment locations
# All coordinates in mm and degrees: [x, y, z, roll, pitch, yaw]
LOCATION_MAPPINGS = {
    "vial_racks": {
        "dx7b": {
            "base_position": [300, 0, 150, 180, 0, 0],
            "row_offset": 25,  # mm between rows
            "column_offset": 25,  # mm between columns
            "rows": ["a", "b", "c", "d"],
            "columns": ["1", "2", "3", "4"]
        },
        "dx8c": {
            "base_position": [300, 150, 150, 180, 0, 0],
            "row_offset": 25,
            "column_offset": 25,
            "rows": ["a", "b", "c", "d"],
            "columns": ["1", "2", "3", "4"]
        }
    },
    "storage": {
        "shelf_a": {
            "position": [400, 100, 200, 180, 0, 0]
        },
        "shelf_b": {
            "position": [400, 200, 200, 180, 0, 0]
        }
    }
}


def resolve_location(location_descriptor: dict) -> list:
    """
    Resolve an abstracted location descriptor to actual coordinates.
    
    Args:
        location_descriptor: Dict describing the location, e.g.
                           {"vial_rack_id": "dx7b", "row": "a", "column": "1"}
                           or {"storage_id": "shelf_a"}
    
    Returns:
        List of coordinates [x, y, z, roll, pitch, yaw]
    """
    # Handle vial rack locations
    if "vial_rack_id" in location_descriptor:
        rack_id = location_descriptor["vial_rack_id"]
        row = location_descriptor.get("row", "a")
        column = location_descriptor.get("column", "1")
        
        if rack_id not in LOCATION_MAPPINGS["vial_racks"]:
            raise ValueError(f"Unknown vial rack ID: {rack_id}")
        
        rack = LOCATION_MAPPINGS["vial_racks"][rack_id]
        base_pos = rack["base_position"].copy()
        
        # Calculate offsets
        row_index = rack["rows"].index(row.lower())
        col_index = rack["columns"].index(column)
        
        # Apply offsets to base position
        base_pos[0] += col_index * rack["column_offset"]
        base_pos[1] += row_index * rack["row_offset"]
        
        return base_pos
    
    # Handle storage locations
    elif "storage_id" in location_descriptor:
        storage_id = location_descriptor["storage_id"]
        
        if storage_id not in LOCATION_MAPPINGS["storage"]:
            raise ValueError(f"Unknown storage ID: {storage_id}")
        
        return LOCATION_MAPPINGS["storage"][storage_id]["position"].copy()
    
    else:
        raise ValueError("Location descriptor must contain 'vial_rack_id' or 'storage_id'")


@flow(name="pick-and-place")
def pick_and_place_flow(ip: str, object_name: str = "object", 
                        pick_location: dict = None, place_location: dict = None):
    """
    Simple pick-and-place flow with abstracted location descriptors and hardcoded movements.
    
    Args:
        ip: IP address of the xArm robot
        object_name: Name/description of the object being picked (for logging)
        pick_location: Location descriptor dict, e.g.
                      {"vial_rack_id": "dx7b", "row": "a", "column": "1"}
                      or {"storage_id": "shelf_a"}
                      If None, uses default vial rack location
        place_location: Location descriptor dict (same format as pick_location)
                       If None, uses default storage location
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
        
        # Set default locations if not provided
        if pick_location is None:
            pick_location = {"vial_rack_id": "dx7b", "row": "a", "column": "1"}
        
        if place_location is None:
            place_location = {"storage_id": "shelf_a"}
        
        # Resolve abstract locations to actual coordinates (hardcoded mappings)
        pick_coords = resolve_location(pick_location)
        place_coords = resolve_location(place_location)
        
        print(f"🤖 Starting pick-and-place for: {object_name}")
        print(f"   Pick from: {pick_location} → {pick_coords[:3]}")
        print(f"   Place at: {place_location} → {place_coords[:3]}")
        
        # Enable and open gripper (hardcoded gripper settings)
        arm.set_gripper_enable(True)
        time.sleep(0.5)
        arm.set_gripper_position(850, wait=True, speed=5000)  # Open
        time.sleep(0.5)
        
        # Move to pick position (hardcoded movement: approach from 5cm above)
        approach_pos = [pick_coords[0], pick_coords[1], pick_coords[2] + 50,
                       pick_coords[3], pick_coords[4], pick_coords[5]]
        arm.set_position(*approach_pos, wait=True, speed=100)
        time.sleep(0.5)
        
        # Lower to pick position (hardcoded descent)
        arm.set_position(*pick_coords, wait=True, speed=100)
        time.sleep(0.5)
        
        # Close gripper to grasp (hardcoded gripper position)
        print(f"✋ Grasping {object_name}")
        arm.set_gripper_position(400, wait=True, speed=5000)  # Close
        time.sleep(0.5)
        
        # Lift up (hardcoded 10cm lift)
        lift_pos = [pick_coords[0], pick_coords[1], pick_coords[2] + 100,
                   pick_coords[3], pick_coords[4], pick_coords[5]]
        arm.set_position(*lift_pos, wait=True, speed=100)
        time.sleep(0.5)
        
        # Move to place location (keeping same height, hardcoded movement)
        place_approach = [place_coords[0], place_coords[1], lift_pos[2],
                         place_coords[3], place_coords[4], place_coords[5]]
        arm.set_position(*place_approach, wait=True, speed=100)
        time.sleep(0.5)
        
        # Lower to place position (hardcoded descent)
        arm.set_position(*place_coords, wait=True, speed=100)
        time.sleep(0.5)
        
        # Release gripper (hardcoded open position)
        print(f"📦 Placing {object_name}")
        arm.set_gripper_position(850, wait=True, speed=5000)  # Open
        time.sleep(0.5)
        
        # Lift up slightly before returning home (hardcoded 5cm lift)
        retract_pos = [place_coords[0], place_coords[1], place_coords[2] + 50,
                      place_coords[3], place_coords[4], place_coords[5]]
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
    
    # Example 1: Use default locations
    pick_and_place_flow(ip, object_name="vial")
    
    # Example 2: Pick from one vial rack location and place in another
    # pick_and_place_flow(
    #     ip=ip,
    #     object_name="sample vial",
    #     pick_location={"vial_rack_id": "dx7b", "row": "b", "column": "2"},
    #     place_location={"vial_rack_id": "dx8c", "row": "a", "column": "3"}
    # )
    
    # Example 3: Pick from vial rack and place in storage
    # pick_and_place_flow(
    #     ip=ip,
    #     object_name="experiment sample",
    #     pick_location={"vial_rack_id": "dx7b", "row": "c", "column": "4"},
    #     place_location={"storage_id": "shelf_b"}
    # )
