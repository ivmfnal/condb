FILES = ConDB.py dbdig.py trace.py timelib.py

UPSLIB = $(UPSROOT)/lib
CLLIB = $(CLROOT)/lib

build: $(UPSLIB) $(CLLIB) $(SRVROOT) $(GUIROOT)
	cp $(FILES) $(UPSLIB)
	cp $(FILES) $(CLLIB)
	cp $(FILES) $(SRVROOT)
	cd $(UPSLIB);	echo "API_Version = \"$(API_VERSION)\"" > API_Version.py
	cd $(CLLIB);	echo "API_Version = \"$(API_VERSION)\"" > API_Version.py
	cd $(SRVROOT);	echo "API_Version = \"$(API_VERSION)\"" > API_Version.py
    
$(UPSLIB):
	mkdir -p $@

$(CLLIB):
	mkdir -p $@
	
$(SRVROOT):
	mkdir -p $@
	
	
    
