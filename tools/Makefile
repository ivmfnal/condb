UPSBIN = $(UPSROOT)/bin
CLBIN = $(CLROOT)/bin

UIFILES = create_table.py get_csv.py post_csv.py read_data.py write_data.py tag.py patch.py py3.py signature.py
SRVFILES = py3.py signature.py

build: $(UPSBIN) $(CLBIN) $(SRVROOT)
	cp $(UIFILES) $(UPSBIN)
	cp $(UIFILES) $(CLBIN)
	cp $(SRVFILES) $(SRVROOT)
    
$(UPSBIN):
	mkdir -p $@

$(CLBIN):
	mkdir -p $@

$(SRVROOT):
	mkdir -p $@
	
	
