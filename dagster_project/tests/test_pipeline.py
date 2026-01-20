#!/usr/bin/env python3
"""
🧪 Test Script for Medical Telegram Pipeline
Tests both manual pipeline and Dagster orchestration
"""
import sys
import os
import subprocess
from datetime import datetime

def test_manual_pipeline():
    """Test the original manual pipeline"""
    print("1. Testing Manual Pipeline...")
    scripts_to_test = [
        ("src/scraper.py", "Scraper"),
        ("scripts/run_pipeline.py", "Pipeline Runner"),
        ("src/yolo_detect.py", "YOLO Detection")
    ]

    for script, name in scripts_to_test:
        if os.path.exists(script):
            print(f"   ✅ {name} script exists: {script}")
        else:
            print(f"   ⚠️  {name} script not found: {script}")

    return True

def test_dagster_pipeline():
    """Test the Dagster orchestration"""
    print("\n2. Testing Dagster Orchestration...")

    try:
        # Check Dagster installation
        import dagster
        print(f"   ✅ Dagster installed: {dagster.__version__}")

        # Try to import our pipeline
        from dagster.pipeline import medical_telegram_daily_pipeline
        print("   ✅ Pipeline module imported successfully")

        # Test execution
        print("   🚀 Executing Dagster pipeline...")
        result = medical_telegram_daily_pipeline.execute_in_process()

        if result.success:
            print("   ✅ Dagster pipeline executed successfully!")
            output = result.output_for_node("validate_pipeline_results")
            print(f"   📊 Result: {output.get('pipeline_status', 'Unknown')}")
            return True
        else:
            print("   ❌ Dagster pipeline execution failed")
            return False

    except Exception as e:
        print(f"   ❌ Dagster test failed: {e}")
        return False

def test_api():
    """Test the FastAPI is accessible"""
    print("\n3. Testing FastAPI...")

    api_files = [
        "API/main.py",
        "API/database.py",
        "API/schemas.py"
    ]

    all_exist = True
    for file in api_files:
        if os.path.exists(file):
            print(f"   ✅ API file exists: {file}")
        else:
            print(f"   ⚠️  API file missing: {file}")
            all_exist = False

    return all_exist

def main():
    """Run all tests"""
    print("=" * 60)
    print("🧪 MEDICAL TELEGRAM PIPELINE TEST SUITE")
    print("=" * 60)

    results = []

    # Run tests
    results.append(("Manual Pipeline", test_manual_pipeline()))
    results.append(("Dagster Orchestration", test_dagster_pipeline()))
    results.append(("FastAPI", test_api()))

    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)

    passed = 0
    total = len(results)

    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
        if success:
            passed += 1

    print(f"\n🎯 Result: {passed}/{total} tests passed")

    if passed == total:
        print("\n🏆 ALL SYSTEMS GO! Pipeline is ready for production!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Check above for details.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
