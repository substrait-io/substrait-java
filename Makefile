COMPONENTS := core isthmus isthmus-cli
SCAN_COMPONENTS := $(addprefix scan-,$(COMPONENTS))

osv_scanner := ./osv-scanner
kernel_name := $(shell uname -s | tr '[:upper:]' '[:lower:]')

machine_hardware := $(shell uname -m)
ifeq ($(machine_hardware), aarch64)
	machine_hardware := arm64
endif
ifeq ($(machine_hardware), x86_64)
	machine_hardware := amd64
endif

.PHONY: scan
scan: $(SCAN_COMPONENTS)

.PHONY: $(SCAN_COMPONENTS)
$(SCAN_COMPONENTS): scan-%: $(osv_scanner) %/gradle.lockfile
	$(osv_scanner) scan source --lockfile $*/gradle.lockfile --config osv-scanner.toml

$(osv_scanner):
	curl --silent --show-error --location --output $(osv_scanner) https://github.com/google/osv-scanner/releases/latest/download/osv-scanner_$(kernel_name)_$(machine_hardware)
	chmod u+x $(osv_scanner)

.PHONY: install-osv-scanner
install-osv-scanner: uninstall-osv-scanner $(osv_scanner)

.PHONY: uninstall-osv-scanner
uninstall-osv-scanner:
	rm -f $(osv_scanner)

$(addsuffix /gradle.lockfile,$(COMPONENTS)): %/gradle.lockfile:
	./gradlew --quiet ':$*:dependencies' --write-locks --configuration runtimeClasspath

.PHONY: clean
clean:
	find . -maxdepth 2 -type f -name gradle.lockfile -delete -print
