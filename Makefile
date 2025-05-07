#
# SPDX-License-Identifier: Apache-2.0
#

OSV_SCANNER_IMAGE := ghcr.io/google/osv-scanner:v2.0.2

.PHONY: scan
scan:
ifdef component
	./gradlew --quiet ':$(component):dependencies' --write-locks --configuration runtimeClasspath
	docker run --rm --volume './$(component)/gradle.lockfile:/gradle.lockfile' $(OSV_SCANNER_IMAGE) scan --lockfile /gradle.lockfile
else
	$(MAKE) component=core scan
	$(MAKE) component=isthmus scan
	$(MAKE) component=isthmus-cli scan
endif

.PHONY: clean
clean:
	find . -depth 2 -type f -name gradle.lockfile -delete -print
