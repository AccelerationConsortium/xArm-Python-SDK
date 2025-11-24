#!/usr/bin/env python3
"""
Prefect Deployment Configuration for xArm Pick and Place Workflow

This file creates and applies a deployment to Prefect Cloud for the xArm pick and place workflow.
"""

from prefect.runner.storage import GitRepository
from pick_place_workflow import pick_and_place_workflow

if __name__ == "__main__":
    print("🚀 Creating xArm Pick and Place deployment...")
    
    # Create deployment with parameters and local code storage
    deployment = pick_and_place_workflow.to_deployment(
        name="xarm-pick-place-production",
        work_pool_name="default-process-pool",
        parameters={
            "robot_ip": "192.168.1.231",
            "cycles": 1
        },
        description="Production pick and place workflow for xArm5 robot",
        tags=["robotics", "xarm", "pick-place", "production"],
        version="1.0.0"
    )
    
    # Apply the deployment to Prefect Cloud
    try:
        deployment_id = deployment.apply()
        print(f"✅ Deployment created successfully!")
        print(f"📋 Deployment ID: {deployment_id}")
        print(f"🌐 View in Prefect Cloud: https://app.prefect.cloud")
        print(f"🤖 Robot IP: 192.168.1.231")
        print(f"🔄 Default Cycles: 1")
        
        print("\n🎯 To run the deployment:")
        print(f"   prefect deployment run 'xArm Pick and Place Workflow/xarm-pick-place-production'")
        
        print("\n📊 To view runs:")
        print(f"   prefect flow-run ls --deployment-name 'xarm-pick-place-production'")
        
    except Exception as e:
        print(f"❌ Deployment failed: {e}")
        print("\n🔍 Common fixes:")
        print("   - Check Prefect Cloud authentication: prefect auth login")
        print("   - Verify workspace permissions include 'run_flows'")
        print("   - Check work pool exists: prefect work-pool ls")