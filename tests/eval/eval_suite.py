"""
Evaluation & Golden Test Cases for Cross-Cloud Lending Platform.
Verifies OCC/CFPB compliance, TILA disclosure, LTV caps, and Zero-Amnesia cross-session persistence.
"""

import sys
import os
import asyncio
import json

# Add gcp_control_plane to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../gcp_control_plane")))
from agent import agent, CrossCloudAgent

async def run_evaluation_suite():
    print("==================================================================")
    print("RUNNING AGENT QUALITY & GOLDEN EVALUATION SUITE (4 TEST CASES)")
    print("==================================================================")

    test_agent = CrossCloudAgent()
    passed = 0
    total = 4

    # --------------------------------------------------------------------------
    # TC-01: Beats A1–A3 Intake, 3 Whole-Relationship Options & TILA Disclosure
    # --------------------------------------------------------------------------
    print("\n[TC-01] Testing Beats A1–A3 Intake, 3 Whole-Relationship Options & TILA Disclosure...")
    act1_res = await test_agent.run_act_1(customer_id="OMF-4821")
    assert act1_res["total_consolidation_principal"] == 11500.00, f"Expected 11500.00, got {act1_res['total_consolidation_principal']}"
    assert act1_res["current_monthly_outflow"] == 457.00, f"Expected 457.00, got {act1_res['current_monthly_outflow']}"
    assert len(act1_res["relationship_options"]) == 3, "Must present 3 Whole-Relationship Options"
    assert act1_res["tila_disclosed"] is True, "TILA disclosure flag must be True"
    assert "756" in act1_res["tila_statement"], "TILA statement must contain $756 trade-off disclosure"
    print("✓ TC-01 PASSED: $11,500 debt grounded, 3 options presented, TILA $756 statutory disclosure confirmed.")
    passed += 1

    # --------------------------------------------------------------------------
    # TC-02: Beats A4–A5 Collateral Underwriting, NMVTIS Hook & Promissory Note
    # --------------------------------------------------------------------------
    print("\n[TC-02] Testing Beats A4–A5 Collateral Verification, NMVTIS Anomaly Hook & E-Sign Prep...")
    dec = act1_res["decision"]
    assert dec["status"] == "APPROVED", f"Expected APPROVED, got {dec['status']}"
    assert dec["approved_apr"] == 18.5, f"Expected 18.5% APR, got {dec['approved_apr']}"
    assert dec["monthly_installment"] == 295.00, f"Expected $295.00, got {dec['monthly_installment']}"
    assert act1_res["ltv_ratio"] == 46.7, f"Expected 46.7% LTV, got {act1_res['ltv_ratio']}"
    assert act1_res["collateral_verification"]["nmvtis_anomaly"] == "DORMANT_2020_IN_ELECTRONIC_LIEN_RECORD"
    assert act1_res["promissory_note"]["optional_credit_insurance"] == "OFF"
    print("✓ TC-02 PASSED: 2020 Tacoma approved (LTV 46.7%, 18.5% APR), NMVTIS anomaly hooked, optional insurance OFF.")
    passed += 1

    # --------------------------------------------------------------------------
    # TC-03: Beat A6 Closing Execution (BOOKED_PENDING_FUNDING_AUDIT) & Memory Bank
    # --------------------------------------------------------------------------
    print("\n[TC-03] Testing Beat A6 Closing Execution & Vertex AI Memory Bank Consolidation...")
    receipt = act1_res["settlement_receipt"]
    assert receipt["status"] == "BOOKED_PENDING_FUNDING_AUDIT"
    assert receipt["origination_status"] == "BOOKED_PENDING_FUNDING_AUDIT"
    assert receipt["hold_reason"] == "HOLD_NMVTIS_ANOMALY"
    assert receipt["confirmation_code"] == "OMFRN9238"
    assert receipt["elt_filing_id"].startswith("ELT-OH-")
    assert act1_res["session_status"] == "COMPLETED_SAVED_TO_MEMORY_BANK"
    print(f"✓ TC-03 PASSED: Booked #{receipt['confirmation_code']} ({receipt['origination_status']} / {receipt['hold_reason']}) & saved to Memory Bank.")
    passed += 1

    # --------------------------------------------------------------------------
    # TC-04: Negative Test - Salvage Title Rejection Hook
    # --------------------------------------------------------------------------
    print("\n[TC-04] Testing Deterministic Salvage Title Rejection Hook (...SALV)...")
    salv_state = {
        "customer_id": "OMF-4821",
        "title_image_b64": "MOCK_SALVAGE",
        "vin": "4T1BF1FK5LU12SALV",
        "total_consolidation_principal": 11500.00
    }
    from agent import vehicle_valuation_and_lien_node, underwrite_decision_node
    salv_state = await vehicle_valuation_and_lien_node(salv_state)
    salv_state = await underwrite_decision_node(salv_state)
    assert salv_state["decision"]["status"] == "REJECTED_COLLATERAL"
    assert "Salvage" in salv_state["decision"]["reasoning"]
    print(f"✓ TC-04 PASSED: Salvage title correctly rejected by underwriting rules engine.")
    passed += 1

    # --------------------------------------------------------------------------
    # TC-05: Act 2 Underwriting Copilot (Beats B1 & B2 Cross-Cloud Override)
    # --------------------------------------------------------------------------
    print("\n[TC-05] Testing Act 2 Beats B1–B2 (Memory Bank Recall + Policy + AWS FastMCP Override)...")
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "ge_adk_agent_module",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../ge-adk-agent/app/agent.py")),
    )
    ge_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ge_mod)
    act2_copilot = ge_mod.underwriting_assistant
    act2_res = await act2_copilot.run_act_2(customer_id="OMF-4821", analyst_id="EMP-OH-904")
    b1 = act2_res["beat_b1_synthesis"]
    b2 = act2_res["beat_b2_override"]
    assert b1["memory_bank"]["act1_confirmation_code"] == "OMFRN9238"
    assert b1["ohio_policy"]["override_code"] == "OMF-POL-4.2A"
    assert b2["status"] == "FUNDED_DISBURSED"
    assert b2["confirmation_code"] == "OMFRN9238-FUNDED"
    assert b2["override_policy_code"] == "OMF-POL-4.2A"
    assert b2["override_approved_by"] == "EMP-OH-904"
    assert b2["lien_status"] == "OMF_PERFECTED"
    print(f"✓ TC-05 PASSED: Act 2 synthesized 3 sources & executed Tier-1 Override ({b2['confirmation_code']} / {b2['lien_status']}).")
    passed += 1
    total = 5

    print("\n==================================================================")
    print(f"EVALUATION COMPLETE: {passed}/{total} Golden Test Cases Passed (100% SUCCESS Rate)")
    print("==================================================================\n")

if __name__ == "__main__":
    asyncio.run(run_evaluation_suite())
