#!/usr/bin/env python3
"""
Prefect Deployment Configuration for xArm Custom Position Workflow

This file creates and applies a deployment to Prefect Cloud for moving the robot
to any custom position via UI parameters.
"""

from custom_position_workflow import custom_position_workflow

if __name__ == "__main__":
    print("🚀 Creating xArm Custom Position deployment...")
    
    # Create deployment with UI-friendly parameters
    deployment = custom_position_workflow.to_deployment(
        name="xarm-custom-position",
        work_pool_name="default-process-pool",
        parameters={
            "robot_ip": "192.168.1.231",
            # Position parameters - these will appear as input fields in Prefect UI
            "x": -60.0,      # X coordinate (mm)
            "y": 420.0,     # Y coordinate (mm) 
            "z": 320.0,     # Z coordinate (mm) - SAFE DEFAULT HEIGHT
            "roll": 179.6,  # Roll angle (degrees)
            "pitch": 0.1,   # Pitch angle (degrees)
            "yaw": 89.6,    # Yaw angle (degrees)
            # Movement parameters
            "speed": 100,   # Movement speed (1-200)
            # Gripper control
            "control_gripper_enabled": False,  # Whether to control gripper
            "gripper_position": 300  # Gripper position (0=closed, 850=open)
        },
        description="Move xArm robot to any custom position via Prefect UI parameters",
        tags=["robotics", "xarm", "custom-position", "movement"],
        version="1.0.0"
    )
    
    # Apply the deployment to Prefect Cloud
    try:
        deployment_id = deployment.apply()
        print(f"✅ Deployment created successfully!")
        print(f"📋 Deployment ID: {deployment_id}")
        print(f"🌐 View in Prefect Cloud: https://app.prefect.cloud")
        
        print(f"\n🎯 To move robot to custom positions:")
        print(f"   1. Go to Prefect Cloud UI: https://app.prefect.cloud")
        print(f"   2. Navigate to Deployments → 'xarm-custom-position'")
        print(f"   3. Click 'Run' to see parameter input fields:")
        print(f"      • x, y, z: Position coordinates (mm)")
        print(f"      • roll, pitch, yaw: Orientation angles (degrees)")
        print(f"      • speed: Movement speed (1-200)")
        print(f"      • control_gripper_enabled: Enable/disable gripper control")
        print(f"      • gripper_position: 0=closed, 850=open")
        
        print(f"\n⚠️  Safety Notes:")
        print(f"   • Z coordinate minimum: 100mm (avoid base plate collision)")
        print(f"   • Default safe position: x=-60, y=420, z=320")
        print(f"   • Test movements at low speed first")
        
        print(f"\n📊 To view runs:")
        print(f"   prefect flow-run ls --deployment-name 'xarm-custom-position'")
        
    except Exception as e:
        print(f"❌ Deployment failed: {e}")
        print(f"\n🔍 Common fixes:")
        print(f"   - Check Prefect Cloud authentication")
        print(f"   - Verify workspace permissions")
        print(f"   - Check work pool exists: prefect work-pool ls")