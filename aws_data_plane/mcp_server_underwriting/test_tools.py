"""
Local verification script for Act 2 Underwriting FastMCP server tools.
"""

import json
from server import get_origination_audit_trail, approve_underwriting_override

def run_tests():
    print("--- 1. Testing get_origination_audit_trail (Beat B1) ---")
    trail = get_origination_audit_trail("OMF-4821")
    print(json.dumps(trail, indent=2))
    assert trail["customer_id"] == "OMF-4821"
    assert trail["confirmation_code"] == "OMFRN9238"
    assert trail["origination_status"] == "BOOKED_PENDING_FUNDING_AUDIT"
    assert trail["hold_reason"] == "HOLD_NMVTIS_ANOMALY"
    assert trail["nmvtis_anomaly"] == "DORMANT_2020_IN_ELECTRONIC_LIEN_RECORD"
    assert trail["ltv_ratio"] == 46.7
    print("✓ get_origination_audit_trail passed.")

    print("\n--- 2. Testing approve_underwriting_override (Beat B2) ---")
    override = approve_underwriting_override(
        customer_id="OMF-4821",
        account_id="ACC-88392",
        policy_citation="OMF-POL-4.2A",
        analyst_id="EMP-OH-904"
    )
    print(json.dumps(override, indent=2))
    assert override["status"] == "FUNDED_DISBURSED"
    assert override["confirmation_code"] == "OMFRN9238-FUNDED"
    assert override["override_policy_code"] == "OMF-POL-4.2A"
    assert override["override_approved_by"] == "EMP-OH-904"
    assert override["lien_status"] == "OMF_PERFECTED"
    assert override["s3_audit_receipt_uri"].startswith("s3://")
    print("✓ approve_underwriting_override passed.")

    print("\n========================================================")
    print("ALL ACT 2 UNDERWRITING FASTMCP TOOLS VERIFIED!")
    print("========================================================\n")

if __name__ == "__main__":
    run_tests()
