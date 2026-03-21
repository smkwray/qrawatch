# Data Sources

This project is designed around **free public data**.

## Source registry

| Source family | What it gives you | Why it matters | Script |
|---|---|---|---|
| Treasury Quarterly Refunding pages and archives | refunding statements, financing estimates, TBAC materials, guidance language | identifies policy timing and quarter-level issuance guidance | `scripts/03_download_qra_materials.py`, `scripts/04_extract_qra_text.py` |
| FiscalData: Treasury Securities Auctions Data (`auctions_query`) | auction-level issue dates, maturities, security types, offering amounts, yields and related fields | builds gross issuance flows and security-bucket classification | `scripts/01_download_fiscaldata.py` |
| FiscalData: MSPD marketable detail (`mspd_table_3_market`) | outstanding marketable Treasury securities by issue / maturity detail | stock bill-share and debt-composition extensions | `scripts/01_download_fiscaldata.py` |
| FiscalData: Buybacks (`buybacks_operations`) | buyback operations and accepted amounts | duration-removal extension and buyback controls | `scripts/01_download_fiscaldata.py` |
| FiscalData: Average Interest Rates (`avg_interest_rates`) | average rates by security category | rollover-risk / debt-service appendix | `scripts/01_download_fiscaldata.py` |
| FiscalData: DTS operating cash balance | Treasury cash balance detail | optional daily TGA / cash-management controls | `scripts/01_download_fiscaldata.py` |
| FRED / H.4.1 core series | reserves, TGA, Fed Treasury holdings, term premium, yields | plumbing channel and price outcomes | `scripts/02_download_fred.py` |
| Treasury Investor Class Auction Allotments | bidder / allotment splits by auction | investor-absorption extension | `scripts/05_download_investor_allotments.py` |
| Treasury International Capital (TIC) | foreign transactions and holdings context | foreign-absorption extension | manual for now; add script later |
| SEC Form N-MFP | money market fund holdings and flows | MMF demand for bills and ON RRP substitution story | `scripts/06_download_sec_nmfp.py` |
| New York Fed Primary Dealer Statistics | dealer positions, transactions, financing, and related activity | dealer-balance-sheet / market-plumbing extension | `scripts/07_download_primary_dealer.py` |

## Core FRED series included in the scaffold

| Series ID | Use |
|---|---|
| `THREEFYTP10` | 10-year Kim-Wright term premium |
| `DGS2` | 2-year Treasury yield |
| `DGS10` | 10-year Treasury yield |
| `DGS30` | 30-year Treasury yield |
| `SP500` | optional risk-asset outcome |
| `VIXCLS` | optional risk-aversion outcome |
| `DFF` | policy-rate control |
| `WRESBAL` | reserve balances, week average |
| `WDTGAL` | Treasury General Account, Wednesday level |
| `WTREGEN` | Treasury General Account, week average |
| `RRPONTSYD` | ON RRP daily usage |
| `WLRRAL` | weekly reverse-repo liabilities |
| `TREAST` | Fed Treasury holdings |
| `WALCL` | Fed total assets |

## Core FiscalData datasets included in the scaffold

| Key | Endpoint |
|---|---|
| `auctions_query` | `v1/accounting/od/auctions_query` |
| `mspd_table_1` | `v1/debt/mspd/mspd_table_1` |
| `mspd_table_3_market` | `v1/debt/mspd/mspd_table_3_market` |
| `buybacks_operations` | `v1/accounting/od/buybacks_operations` |
| `avg_interest_rates` | `v2/accounting/od/avg_interest_rates` |
| `debt_outstanding` | `v2/accounting/od/debt_outstanding` |
| `operating_cash_balance` | `v1/accounting/dts/operating_cash_balance` |
| `mts_table_1` | `v1/accounting/mts/mts_table_1` |

## Notes that matter for implementation

### QRA data
The quarter-level ATI series depends on quarter-specific financing language and tables. That part is often embedded in HTML press releases, PDFs, and TBAC attachments, so the repo separates:

- **document collection**
- **text extraction**
- **manual / semi-manual quarter capture**

before trying to automate every table.

### SEC N-MFP
The SEC page notes a format transition: the datasets were updated to include **N-MFP3** submissions, and the documentation was updated accordingly. Any parser must handle both legacy and newer formats.

### Primary Dealer Statistics
The New York Fed provides export formats intended for automated consumption. Use those exports instead of scraping rendered tables.

### Investor allotments
These are excellent for an extension on who absorbed the issuance, but they should not block the main pricing and plumbing paper.

## Recommended minimal data set for a first paper draft

You can get a publishable first pass from just these:

1. manual quarter ATI seed
2. QRA documents
3. auctions data
4. core FRED series
5. buyback operations

Everything else is an upgrade, not a prerequisite.
