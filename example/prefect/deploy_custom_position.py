#!/usr/bin/env python3
"""
Prefect Deployment Configuration for xArm Custom Position Workflow

This file creates and applies a deployment to Prefect Cloud for moving the robot
to any custom position via UI parameters.
"""

from pathlib import Path


def create_deployments(work_pool_name: str = "default-process-pool"):
    """
    Create Prefect deployment for xArm custom position control.
    
    This should be run once during setup to register the flow with Prefect Cloud.
    
    Args:
        work_pool_name: Name of the work pool to use (default: "default-process-pool")
        
    Usage:
        python deploy_custom_position.py
    """
    from custom_position_workflow import custom_position_workflow
    
    # Get the source directory (where the workflow files are)
    source_dir = Path(__file__).parent
    
    print(f"Creating deployment with work pool: {work_pool_name}")
    print("=== DEPLOYING xArm CUSTOM POSITION FLOW ===")
    
    # Deploy custom position workflow
    custom_position_workflow.from_source(
        source=str(source_dir),
        entrypoint="custom_position_workflow.py:custom_position_workflow",
    ).deploy(
        name="xarm-custom-position-deploy",
        work_pool_name=work_pool_name,
        description="Move xArm robot to any custom position via Prefect UI parameters",
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
        tags=["robotics", "xarm", "custom-position", "movement"],
        version="4.0.0"
    )
    print(f"Deployed 'custom_position_workflow/xarm-custom-position-deploy'")
    
    print(f"\nDeployment created successfully!")
    
    print(f"\nNext steps:")
    print(f"1. Start a worker: prefect worker start --pool {work_pool_name}")
    print(f"2. Trigger a flow from Python:")
    print(f"   from prefect.deployments import run_deployment")
    print(f"   run_deployment('custom_position_workflow/xarm-custom-position-deploy', parameters={{'x': 200, 'y': 300, 'z': 400}}, timeout=0)")
    print(f"3. Or from CLI:")
    print(f"   prefect deployment run 'custom_position_workflow/xarm-custom-position-deploy' --param x=200 --param y=300 --param z=400")
    print(f"4. Or from Prefect Cloud UI:")
    print(f"   Go to https://app.prefect.cloud → Deployments → 'xarm-custom-position-deploy' → Run")
    
    print(f"\n⚠️  Safety Notes:")
    print(f"   • Z coordinate minimum: 100mm (avoid base plate collision)")
    print(f"   • Default safe position: x=-60, y=420, z=320")
    print(f"   • Test movements at low speed first")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        work_pool = sys.argv[1]
    else:
        work_pool = "default-process-pool"
    
    print("🚀 Creating xArm Custom Position deployment...")
    try:
        create_deployments(work_pool)
    except Exception as e:
        print(f"❌ Deployment failed: {e}")
        print(f"\n🔍 Common fixes:")
        print(f"   - Check Prefect Cloud authentication")
        print(f"   - Verify workspace permissions")
        print(f"   - Check work pool exists: prefect work-pool ls")