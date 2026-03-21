PYTHON ?= $(HOME)/venvs/qrawatch/bin/python
SHELL := /bin/zsh
RUN = source .env && "$(PYTHON)"

.PHONY: bootstrap seed qra-enrich official-capture official-ati historical-seed shock-template elasticity identification absorption test download-core qra investor primary sec event plumbing duration publish qra-quality backend-validate regenerate site

bootstrap:
	$(RUN) scripts/00_bootstrap.py

seed:
	$(RUN) scripts/08_build_ati_index.py

qra-enrich:
	$(RUN) scripts/20_enrich_official_qra_capture.py

official-capture:
	$(RUN) scripts/13_build_official_qra_capture.py

official-ati:
	$(RUN) scripts/17_build_official_ati.py

historical-seed:
	$(RUN) scripts/22_seed_forward_official_quarters.py --direction backward

shock-template:
	$(RUN) scripts/23_seed_qra_shock_template.py

elasticity:
	$(RUN) scripts/24_build_qra_event_elasticity.py

identification:
	$(RUN) scripts/25_build_qra_identification_tables.py

absorption:
	$(RUN) scripts/26_build_auction_absorption.py

test:
	$(RUN) -B -m pytest -q

download-core:
	$(RUN) scripts/01_download_fiscaldata.py --all
	$(RUN) scripts/02_download_fred.py --preset core

qra:
	$(RUN) scripts/03_download_qra_materials.py --download-files
	$(RUN) scripts/04_extract_qra_text.py

investor:
	$(RUN) scripts/05_download_investor_allotments.py --download-files
	$(RUN) scripts/16_build_investor_allotments_inventory.py

primary:
	$(RUN) scripts/07_download_primary_dealer.py --download-files
	$(RUN) scripts/18_build_primary_dealer_inventory.py

sec:
	$(RUN) scripts/06_download_sec_nmfp.py --download-files
	$(RUN) scripts/19_build_sec_nmfp_inventory.py

event:
	$(RUN) scripts/09_build_qra_event_panel.py
	$(RUN) scripts/10_run_event_study.py

plumbing:
	$(RUN) scripts/11_run_plumbing_regressions.py

duration:
	$(RUN) scripts/12_build_public_duration_supply.py

publish:
	$(RUN) scripts/15_build_publish_artifacts.py

qra-quality:
	$(RUN) scripts/14_qra_quality_report.py

backend-validate:
	$(RUN) scripts/21_validate_backend.py

regenerate: download-core qra qra-enrich official-capture official-ati investor primary sec seed event shock-template elasticity identification absorption plumbing duration publish qra-quality backend-validate

site: publish
	@echo "Copying publish artifacts to site/data/..."
	@mkdir -p site/data
	@cp output/publish/*.json site/data/
	@cp output/publish/*.csv site/data/
	@echo "Site ready. Serve with: cd site && python3 -m http.server 8000"
