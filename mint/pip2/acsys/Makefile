.PHONY : all
all : DPM.pyc dpm_protocol.pyc

%.pyc : %.py
	python -m py_compile $<

DPM.pyc : DPM.py

dpm_protocol.pyc : dpm_protocol.py

DPM.pyc : dpm_protocol.pyc

clean ::
	rm -f *pyc
