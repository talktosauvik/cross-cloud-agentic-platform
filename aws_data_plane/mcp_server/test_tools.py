"""
Unit test harness for FastMCP tools
"""
import base64
import json
from server import (
    get_customer_snapshot,
    upload_vehicle_title,
    verify_vehicle_title,
    submit_signed_renewal,
)

def run_tests():
    print("\n--- 1. Testing get_customer_snapshot ---")
    snapshot = get_customer_snapshot("OMF-4821")
    print(json.dumps(snapshot, indent=2))
    assert snapshot["customer_id"] == "OMF-4821"
    assert len(snapshot["accounts"]) >= 2
    print("✓ get_customer_snapshot passed.")

    print("\n--- 2. Testing upload_vehicle_title ---")
    mock_img_b64 = base64.b64encode(b"MOCK_TACOMA_TITLE_SCAN").decode("utf-8")
    s3_uri = upload_vehicle_title("ACC-88392", mock_img_b64, "tacoma_title_scan.png")
    print(f"Uploaded URI: {s3_uri}")
    assert s3_uri.startswith("s3://")
    print("✓ upload_vehicle_title passed.")

    print("\n--- 3. Testing verify_vehicle_title (Clean Title + Dormant NMVTIS Anomaly Hook) ---")
    clean_verif = verify_vehicle_title("4T1BF1FK5LU129384", "Dhanshri More", "OH")
    print(json.dumps(clean_verif, indent=2))
    assert clean_verif["qualification_status"] == "APPROVED"
    assert clean_verif["nmvtis_anomaly"] == "DORMANT_2020_IN_ELECTRONIC_LIEN_RECORD"
    print("✓ verify_vehicle_title (clean + NMVTIS detour hook) passed.")

    print("\n--- 4. Testing verify_vehicle_title (Salvage Brand Rejection) ---")
    salv_verif = verify_vehicle_title("4T1BF1FK5LU12SALV", "Dhanshri More", "OH")
    print(json.dumps(salv_verif, indent=2))
    assert salv_verif["qualification_status"] == "REJECTED"
    assert "Salvage" in salv_verif["rejection_reason"]
    print("✓ verify_vehicle_title (salvage rejection) passed.")

    print("\n--- 5. Testing submit_signed_renewal (BOOKED_PENDING_FUNDING_AUDIT) ---")
    booking = submit_signed_renewal(
        account_id="ACC-88392",
        new_principal=11500.00,
        approved_apr=18.5,
        term_months=60,
        monthly_payment=295.00,
        vin="4T1BF1FK5LU129384"
    )
    print(json.dumps(booking, indent=2))
    assert booking["status"] == "BOOKED_PENDING_FUNDING_AUDIT"
    assert booking["hold_reason"] == "HOLD_NMVTIS_ANOMALY"
    assert booking["confirmation_code"] == "OMFRN9238"
    print("✓ submit_signed_renewal passed.")

    print("\n==========================================")
    print("ALL FASTMCP TOOLS VERIFIED SUCCESSFULLY!")
    print("==========================================\n")

if __name__ == "__main__":
    run_tests()
