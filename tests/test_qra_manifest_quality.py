import pandas as pd

from ati_shadow_policy.webscrape import build_qra_manifest


def test_build_qra_manifest_prefers_quarter_relevant_documents():
    links = pd.DataFrame(
        [
            {
                "source_page": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding",
                "text": "Quarterly Refunding Statement for Q2 2026",
                "href": "https://home.treasury.gov/news/press-releases/jy9999",
            },
            {
                "source_page": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding",
                "text": "Quarterly Refunding Financing Estimates by Calendar Year",
                "href": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/quarterly-refunding-financing-estimates-by-calendar-year",
            },
            {
                "source_page": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding",
                "text": "Treasury Quarterly Refunding",
                "href": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding",
            },
            {
                "source_page": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding",
                "text": "Remarks: A Reset on Liquidity Regulation",
                "href": "https://home.treasury.gov/news/press-releases/sb0412",
            },
            {
                "source_page": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding",
                "text": "Q3 2025 TBAC Charge and Supplemental Materials (PDF)",
                "href": "https://home.treasury.gov/system/files/136/tbac-charge-q3-2025.pdf",
            },
        ]
    )

    manifest = build_qra_manifest(links, min_relevance_score=5)

    assert "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding" not in set(
        manifest["href"]
    )
    assert "https://home.treasury.gov/news/press-releases/sb0412" not in set(manifest["href"])
    assert "https://home.treasury.gov/news/press-releases/jy9999" in set(manifest["href"])
    assert "https://home.treasury.gov/system/files/136/tbac-charge-q3-2025.pdf" in set(manifest["href"])

    assert manifest["quarter_relevant"].all()
    assert (manifest["relevance_score"] >= 5).all()


def test_build_qra_manifest_sets_doc_type_and_quality_tier():
    links = pd.DataFrame(
        [
            {
                "text": "Quarterly Refunding Financing Estimates by Calendar Year",
                "href": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/quarterly-refunding-financing-estimates-by-calendar-year",
            },
            {
                "text": "Q1 2026 TBAC charge (pdf)",
                "href": "https://home.treasury.gov/system/files/136/q1-2026-tbac-charge.pdf",
            },
            {
                "text": "Quarterly Refunding Statement for Q1 2026",
                "href": "https://home.treasury.gov/news/press-releases/jy1200",
            },
        ]
    )

    manifest = build_qra_manifest(links, min_relevance_score=5)
    by_href = manifest.set_index("href")

    assert (
        by_href.loc[
            "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/quarterly-refunding-financing-estimates-by-calendar-year",
            "doc_type",
        ]
        == "financing_estimates"
    )
    assert by_href.loc["https://home.treasury.gov/system/files/136/q1-2026-tbac-charge.pdf", "doc_type"] == "tbac_attachment"
    assert (
        by_href.loc["https://home.treasury.gov/news/press-releases/jy1200", "doc_type"]
        == "quarterly_refunding_press_release"
    )
    assert by_href.loc["https://home.treasury.gov/system/files/136/q1-2026-tbac-charge.pdf", "quality_tier"] == "primary_document"
    assert by_href.loc["https://home.treasury.gov/news/press-releases/jy1200", "quality_tier"] == "official_release_page"


def test_build_qra_manifest_uses_source_context_for_quarter_labeled_links():
    links = pd.DataFrame(
        [
            {
                "source_page": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/quarterly-refunding-financing-estimates-by-calendar-year",
                "text": "1st Quarter",
                "href": "https://home.treasury.gov/news/press-releases/sb0007",
            },
            {
                "source_page": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/quarterly-refunding-financing-estimates-by-calendar-year",
                "text": "Treasury International Capital Data for January",
                "href": "https://home.treasury.gov/news/press-releases/sb0418",
            },
        ]
    )

    manifest = build_qra_manifest(links, min_relevance_score=5)
    by_href = manifest.set_index("href")

    assert "https://home.treasury.gov/news/press-releases/sb0007" in set(manifest["href"])
    assert "https://home.treasury.gov/news/press-releases/sb0418" not in set(manifest["href"])
    assert by_href.loc["https://home.treasury.gov/news/press-releases/sb0007", "doc_type"] == "quarterly_refunding_press_release"


def test_build_qra_manifest_recognizes_known_qra_release_text():
    links = pd.DataFrame(
        [
            {
                "source_page": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding",
                "text": "Quarterly Refunding Statement for 2022 May",
                "href": "https://home.treasury.gov/news/press-releases/jy0908",
            },
            {
                "source_page": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/quarterly-refunding-financing-estimates-by-calendar-year",
                "text": "quarterly-refunding-financing-estimates-by-calendar-year",
                "href": "https://home.treasury.gov/news/press-releases/jy0755",
            },
            {
                "source_page": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding",
                "text": "Some Policy Speech",
                "href": "https://home.treasury.gov/news/press-releases/sb7777",
            },
        ]
    )

    manifest = build_qra_manifest(links, min_relevance_score=5)
    by_href = manifest.set_index("href")

    assert "https://home.treasury.gov/news/press-releases/jy0908" in set(manifest["href"])
    assert "https://home.treasury.gov/news/press-releases/sb7777" not in set(manifest["href"])
    assert by_href.loc["https://home.treasury.gov/news/press-releases/jy0908", "doc_type"] == "quarterly_refunding_press_release"


def test_build_qra_manifest_infers_quarter_from_labels_and_filenames():
    links = pd.DataFrame(
        [
            {
                "source_page": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/quarterly-refunding-financing-estimates-by-calendar-year",
                "text": "2024 1st Quarter",
                "href": "https://home.treasury.gov/news/press-releases/jy2054",
            },
            {
                "source_page": "https://home.treasury.gov/policy-issues/financing-the-government/quarterly-refunding/quarterly-refunding-archives/quarterly-refunding-financing-estimates-by-calendar-year",
                "text": "3rd Quarter",
                "href": "https://home.treasury.gov/system/files/221/fe-1995-q3.pdf",
            },
        ]
    )

    manifest = build_qra_manifest(links, min_relevance_score=5).set_index("href")

    assert manifest.loc["https://home.treasury.gov/news/press-releases/jy2054", "quarter"] == "2024Q1"
    assert manifest.loc["https://home.treasury.gov/system/files/221/fe-1995-q3.pdf", "quarter"] == "1995Q3"
