
API_VERSION = 3.2
SERVER_VERSION = 3.2a

VERSION = $(API_VERSION)_$(SERVER_VERSION)

BLDROOT = $(HOME)/build/ConDB
UPSROOT = $(BLDROOT)/ups
CLROOT = $(BLDROOT)/client
SRVROOT = $(BLDROOT)/server

WSGI_PY_ROOT = $(HOME)/build/wsgi_py

TARDIR = /tmp/$(USER)
CLTAR = $(TARDIR)/ConDB_Client_$(API_VERSION).tar
SRVTAR = $(TARDIR)/ConDB_Server_$(VERSION).tar
UPSTAR = $(TARDIR)/condb_ups_$(API_VERSION).tar

all:    
	@echo "make nova        - will make the product with NOvA API"
	@echo "make generic     - will make generic version"

nova:	
	@make -f Makefile-NOvA
    
generic:	clean build $(TARDIR)
	cd $(UPSROOT);	tar cf $(UPSTAR) *
	cd $(CLROOT);	tar cf $(CLTAR) *
	cd $(SRVROOT);	tar cf $(SRVTAR) *
	@echo
	@echo Client tarfile ........... $(CLTAR)
	@echo Server tarfile ........... $(SRVTAR)
	@echo UPS tarfile .............. $(UPSTAR)
	@echo
    
build:  $(SRVROOT) $(CLROOT) $(UPSROOT) $(GUIROOT)
	cd API; make API_VERSION=$(API_VERSION) VERSION=$(VERSION) UPSROOT=$(UPSROOT) CLROOT=$(CLROOT) SRVROOT=$(SRVROOT) build
	cd server; make VERSION=$(VERSION) GUI_VERSION=$(SERVER_VERSION) WSGI_PY_ROOT=$(WSGI_PY_ROOT) CLROOT=$(CLROOT) SRVROOT=$(SRVROOT) GUIROOT=$(GUIROOT) build
	cd tools; make VERSION=$(VERSION) CLROOT=$(CLROOT) UPSROOT=$(UPSROOT) SRVROOT=$(SRVROOT) build
	cd ups; make VERSION=$(VERSION) UPSROOT=$(UPSROOT) build

clean:
	rm -rf $(BLDROOT)
    
$(SRVROOT):
	mkdir -p $@

$(GUIROOT):
	mkdir -p $@

$(UPSROOT):
	mkdir -p $@

$(CLROOT):
	mkdir -p $@
	
$(TARDIR):
	mkdir -p $@
    
    
   
	
