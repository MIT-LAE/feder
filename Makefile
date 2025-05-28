DOC_MODULES=feder
DOC_LOGO=/pages/iross/feder/lae-logo.png

.PHONY: dist docs doc-server

dist:
	uv build --package feder

docs:
	mkdir -p docs
	cp deploy/lae-logo.png docs
	cd api ; uv run pdoc --logo $(DOC_LOGO) -o ../docs $(DOC_MODULES)

doc-server:
	cd api ; uv run pdoc $(DOC_MODULES)
