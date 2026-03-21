import hashlib
from pathlib import Path

import pandas as pd

from ati_shadow_policy import webscrape


def test_filter_links_allowed_extensions_handle_query_strings():
    links = pd.DataFrame(
        [
            {"href": "https://example.com/doc.pdf?download=1", "text": "PDF"},
            {"href": "https://example.com/page.html", "text": "HTML"},
            {"href": "https://example.com/image.png", "text": "PNG"},
        ]
    )

    filtered = webscrape.filter_links(links, allowed_extensions=[".pdf", ".html"])

    assert list(filtered["href"]) == [
        "https://example.com/doc.pdf?download=1",
        "https://example.com/page.html",
    ]


def test_guess_filename_is_deterministic_and_extractor_compatible():
    row = pd.Series(
        {
            "href": "https://example.com/download?id=123",
            "text": "Quarterly Refunding Statement PDF",
        }
    )
    first = webscrape._guess_filename(row)
    second = webscrape._guess_filename(row)

    assert first == second
    assert first.endswith(".pdf")
    assert "_" + hashlib.sha1(row["href"].encode("utf-8")).hexdigest()[:10] in first


def test_download_link_manifest_adds_provenance_and_skips_existing(monkeypatch, tmp_path: Path):
    def fake_download(url: str, path: Path, **kwargs):
        if kwargs.get("skip_existing") and path.exists():
            return {
                "download_status": "skipped_existing",
                "download_attempts": 0,
                "http_status": None,
                "final_url": url,
                "content_type": None,
                "content_length": path.stat().st_size,
                "etag": None,
                "last_modified": None,
                "downloaded_at_utc": None,
                "bytes_written": 0,
                "skipped_existing": True,
                "error_type": None,
                "error_message": None,
            }
        path.write_bytes(b"%PDF-1.4 fake pdf content")
        return {
            "download_status": "ok",
            "download_attempts": 1,
            "http_status": 200,
            "final_url": url,
            "content_type": "application/pdf",
            "content_length": "25",
            "etag": "abc123",
            "last_modified": "Fri, 01 Jan 2021 00:00:00 GMT",
            "downloaded_at_utc": "2026-03-20T00:00:00+00:00",
            "bytes_written": 25,
            "skipped_existing": False,
            "error_type": None,
            "error_message": None,
        }

    monkeypatch.setattr(webscrape, "download_binary_with_metadata", fake_download)
    links = pd.DataFrame(
        [{"href": "https://example.com/qra?file=1", "text": "QRA Press Statement PDF"}]
    )

    first = webscrape.download_link_manifest(links, tmp_path)
    second = webscrape.download_link_manifest(links, tmp_path)

    assert first.loc[0, "download_status"] == "ok"
    assert second.loc[0, "download_status"] == "skipped_existing"
    assert first.loc[0, "local_extension"] == ".pdf"
    assert first.loc[0, "content_type"] == "application/pdf"
    assert first.loc[0, "source_href_sha1"]
    assert first.loc[0, "filename_method"] == "slug_sha1_href_ext"
