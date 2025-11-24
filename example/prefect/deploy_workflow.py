#!/usr/bin/env python3
"""
Prefect Deployment Configuration for xArm Pick and Place Workflow

This file defines how to deploy the pick and place workflow to Prefect Cloud
with proper scheduling, parameters, and error handling.
"""

from prefect import serve
from pick_place_workflow import pick_and_place_workflow

if __name__ == "__main__":
    # Define the deployment with scheduling and parameters
    pick_place_deployment = pick_and_place_workflow.to_deployment(
        name="xArm-Pick-Place-Production",
        parameters={
            "robot_ip": "192.168.1.231",
            "cycles": 1
        },
        description="Production pick and place workflow for xArm5 robot",
        tags=["robotics", "xarm", "pick-place"],
        version="1.0.0"
    )
    
    # Serve the deployment
    serve(pick_place_deployment)