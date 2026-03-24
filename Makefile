PYTHON := $(HOME)/venvs/qrawatch/bin/python
SHELL := /bin/zsh
EXPECTED_VENV := $(HOME)/venvs/qrawatch
RUN = source .env && "$(MAKE)" --no-print-directory guard-env >/dev/null && "$(PYTHON)" -B

.PHONY: guard-env bootstrap seed qra-enrich official-capture official-ati historical-seed shock-template elasticity causal-review identification identification-refresh absorption test download-core qra investor primary sec event plumbing duration publish qra-quality backend-validate regenerate pricing-figures site

guard-env:
	@source .env && \
	[[ "$${UV_PROJECT_ENVIRONMENT:-}" == "$(EXPECTED_VENV)" ]] || { echo "Error: UV_PROJECT_ENVIRONMENT must be $(EXPECTED_VENV)"; exit 1; } && \
	[[ -x "$(PYTHON)" ]] || { echo "Error: expected external python at $(PYTHON)"; exit 1; } && \
	forbidden="$$(find . -type d \( -name '.venv' -o -name 'venv' -o -name 'env' -o -name '__pycache__' -o -name '.pytest_cache' -o -name '.ruff_cache' -o -name '.mypy_cache' -o -name '.cache' \) -prune -print)"; \
	if [[ -n "$$forbidden" ]]; then \
		echo "Error: forbidden local env/cache directories found:"; \
		echo "$$forbidden"; \
		exit 1; \
	fi

bootstrap: guard-env
	$(RUN) scripts/00_bootstrap.py

seed: guard-env
	$(RUN) scripts/08_build_ati_index.py

qra-enrich: guard-env
	$(RUN) scripts/20_enrich_official_qra_capture.py

official-capture: guard-env
	$(RUN) scripts/13_build_official_qra_capture.py

official-ati: guard-env
	$(RUN) scripts/17_build_official_ati.py

historical-seed: guard-env
	$(RUN) scripts/22_seed_forward_official_quarters.py --direction backward

shock-template: guard-env
	$(RUN) scripts/23_seed_qra_shock_template.py

elasticity: guard-env
	$(RUN) scripts/24_build_qra_event_elasticity.py

causal-review: guard-env
	$(RUN) scripts/28_seed_qra_causal_review_inputs.py

identification: guard-env
	$(RUN) scripts/25_build_qra_identification_tables.py

identification-refresh: guard-env
	$(RUN) scripts/25_build_qra_identification_tables.py

absorption: guard-env
	$(RUN) scripts/26_build_auction_absorption.py

test: guard-env
	$(RUN) -m pytest -q

download-core: guard-env
	$(RUN) scripts/01_download_fiscaldata.py --all
	$(RUN) scripts/02_download_fred.py --preset core

qra: guard-env
	$(RUN) scripts/03_download_qra_materials.py --download-files
	$(RUN) scripts/04_extract_qra_text.py

investor: guard-env
	$(RUN) scripts/05_download_investor_allotments.py --download-files
	$(RUN) scripts/16_build_investor_allotments_inventory.py

primary: guard-env
	$(RUN) scripts/07_download_primary_dealer.py --download-files
	$(RUN) scripts/18_build_primary_dealer_inventory.py

sec: guard-env
	$(RUN) scripts/06_download_sec_nmfp.py --download-files
	$(RUN) scripts/19_build_sec_nmfp_inventory.py

event: guard-env
	$(RUN) scripts/09_build_qra_event_panel.py
	$(RUN) scripts/10_run_event_study.py

plumbing: guard-env
	$(RUN) scripts/11_run_plumbing_regressions.py

duration: guard-env
	$(RUN) scripts/12_build_public_duration_supply.py

publish: guard-env
	$(RUN) scripts/15_build_publish_artifacts.py

pricing-figures: guard-env
	$(RUN) scripts/31_build_pricing_figures.py

qra-quality: guard-env
	$(RUN) scripts/14_qra_quality_report.py

backend-validate: guard-env
	$(RUN) scripts/21_validate_backend.py

regenerate: download-core qra qra-enrich official-capture official-ati investor primary sec seed event shock-template elasticity identification causal-review identification-refresh absorption plumbing duration publish qra-quality backend-validate

site: guard-env publish
	@echo "Copying publish artifacts to site/data/..."
	@mkdir -p site/data
	@find site/data -maxdepth 1 -type f \( -name '*.json' -o -name '*.csv' \) -delete
	@rsync -a --ignore-existing --include='*.json' --include='*.csv' --exclude='*' output/publish/ site/data/
	@if [[ -d output/figures ]]; then \
		mkdir -p site/figures; \
		find site/figures -maxdepth 1 -type f -delete 2>/dev/null || true; \
		rsync -a --ignore-existing output/figures/ site/figures/ 2>/dev/null || true; \
	fi
	@echo "Site ready. Serve with: cd site && python3 -m http.server 8000"
