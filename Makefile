demo:
	python -m demo

alltest:
	pytest -vvs

clean-tiles:
	for i in {0..10}; \
	do \
	rm -f dataserver/static/tiles/$$i/*; \
	done

fetch:
	python bin/dataserver_fetch.py
